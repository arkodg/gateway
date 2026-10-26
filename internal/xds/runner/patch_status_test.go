// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package runner

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ktypes "k8s.io/apimachinery/pkg/types"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/envoyproxy/gateway/internal/ir"
	"github.com/envoyproxy/gateway/internal/message"
	xtypes "github.com/envoyproxy/gateway/internal/xds/types"
)

func patchStatus(name, gateway string, generation int64) *ir.EnvoyPatchPolicyStatus {
	return &ir.EnvoyPatchPolicyStatus{
		Name: name, Namespace: "default", Generation: generation,
		Status: &gwapiv1.PolicyStatus{Ancestors: []gwapiv1.PolicyAncestorStatus{{
			AncestorRef: gwapiv1.ParentReference{Name: gwapiv1.ObjectName(gateway)},
		}}},
	}
}

func storePatchInput(inputs *message.XdsIR, key string, statuses ...*ir.EnvoyPatchPolicyStatus) {
	value := &message.XdsIRWithContext{XdsIR: &ir.Xds{}}
	for _, status := range statuses {
		value.XdsIR.EnvoyPatchPolicies = append(value.XdsIR.EnvoyPatchPolicies, &ir.EnvoyPatchPolicy{EnvoyPatchPolicyStatus: *status})
	}
	inputs.Store(key, value)
}

func TestPatchStatusOwnership(t *testing.T) {
	resources := new(message.ProviderResources)
	private := patchStatus("private-policy", "private", 1)
	edge := patchStatus("edge-policy", "edge", 1)
	stale := patchStatus("removed-policy", "removed", 1)
	for _, status := range []*ir.EnvoyPatchPolicyStatus{edge, stale} {
		resources.EnvoyPatchPolicyStatuses.Store(ktypes.NamespacedName{Namespace: status.Namespace, Name: status.Name}, status.Status)
	}
	inputs := new(message.XdsIR)
	storePatchInput(inputs, "private", private)
	storePatchInput(inputs, "edge", edge)
	publisher := newPatchStatusPublisher(resources, inputs)
	publisher.publish("private", xtypes.EnvoyPatchPolicyStatuses{private}, time.Unix(1, 0))
	require.Len(t, resources.EnvoyPatchPolicyStatuses.LoadAll(), 3, "pending initial keys must retain their previous statuses")
	publisher.publish("edge", xtypes.EnvoyPatchPolicyStatuses{edge}, time.Unix(1, 0))
	statuses := resources.EnvoyPatchPolicyStatuses.LoadAll()
	require.Len(t, statuses, 2)
	require.NotContains(t, statuses, ktypes.NamespacedName{Namespace: "default", Name: stale.Name})
	privateKey := ktypes.NamespacedName{Namespace: "default", Name: private.Name}
	edgeKey := ktypes.NamespacedName{Namespace: "default", Name: edge.Name}

	// An unresolved target must not overwrite or delete its existing status.
	empty := *private
	empty.Status = &gwapiv1.PolicyStatus{}
	storePatchInput(inputs, "private", &empty)
	publisher.publish("private", xtypes.EnvoyPatchPolicyStatuses{&empty}, time.Unix(2, 0))
	require.Equal(t, private.Status, resources.EnvoyPatchPolicyStatuses.LoadAll()[privateKey])
	inputs.Delete("private")
	publisher.publish("private", nil, time.Unix(3, 0))
	statuses = resources.EnvoyPatchPolicyStatuses.LoadAll()
	require.NotContains(t, statuses, privateKey)
	require.Equal(t, edge.Status, statuses[edgeKey], "one key must not clear another key's status")
	inputs.Delete("edge")
	publisher.publish("edge", nil, time.Unix(3, 0))
	require.Empty(t, resources.EnvoyPatchPolicyStatuses.LoadAll())
}

func TestPatchStatusRetargetAndRecreation(t *testing.T) {
	resources := new(message.ProviderResources)
	old := patchStatus("policy", "private", 100)
	recreated := patchStatus("policy", "edge", 1)
	inputs := new(message.XdsIR)
	storePatchInput(inputs, "private", old)
	storePatchInput(inputs, "edge", recreated)
	publisher := newPatchStatusPublisher(resources, inputs)
	key := ktypes.NamespacedName{Namespace: "default", Name: "policy"}
	// The older key completes last. A recreated object's smaller generation
	// must not allow the previous target's status to replace the newer input.
	publisher.publish("edge", xtypes.EnvoyPatchPolicyStatuses{recreated}, time.Unix(2, 0))
	publisher.publish("private", xtypes.EnvoyPatchPolicyStatuses{old}, time.Unix(1, 0))
	require.Equal(t, recreated.Status, resources.EnvoyPatchPolicyStatuses.LoadAll()[key])
	inputs.Delete("private")
	publisher.publish("private", nil, time.Unix(3, 0))
	require.Equal(t, recreated.Status, resources.EnvoyPatchPolicyStatuses.LoadAll()[key])
	inputs.Delete("edge")
	publisher.publish("edge", nil, time.Unix(4, 0))
	// An old translation may finish after the policy's new target was removed.
	publisher.publish("private", xtypes.EnvoyPatchPolicyStatuses{old}, time.Unix(1, 0))
	require.Empty(t, resources.EnvoyPatchPolicyStatuses.LoadAll(), "late work must not resurrect a deleted or retargeted policy")
}

func TestPatchStatusConcurrentKeys(t *testing.T) {
	resources := new(message.ProviderResources)
	inputs := new(message.XdsIR)
	for _, key := range []string{"private", "edge"} {
		storePatchInput(inputs, key, patchStatus(key, key, 1))
	}
	publisher := newPatchStatusPublisher(resources, inputs)
	var workers sync.WaitGroup
	for _, key := range []string{"private", "edge"} {
		workers.Go(func() {
			for generation := int64(1); generation <= 20; generation++ {
				publisher.publish(key, xtypes.EnvoyPatchPolicyStatuses{patchStatus(key, key, generation)}, time.Unix(generation, 0))
			}
		})
	}
	workers.Wait()
	require.Len(t, resources.EnvoyPatchPolicyStatuses.LoadAll(), 2)
}

func TestPatchStatusRetargetPreservesStatusWhileNewOwnerTranslates(t *testing.T) {
	resources := new(message.ProviderResources)
	inputs := new(message.XdsIR)
	old := patchStatus("policy", "private", 1)
	updated := patchStatus("policy", "edge", 2)
	storePatchInput(inputs, "private", old)
	storePatchInput(inputs, "edge")
	publisher := newPatchStatusPublisher(resources, inputs)
	publisher.publish("private", xtypes.EnvoyPatchPolicyStatuses{old}, time.Unix(1, 0))
	publisher.publish("edge", nil, time.Unix(1, 0))

	storePatchInput(inputs, "private")
	storePatchInput(inputs, "edge", updated)
	publisher.publish("private", nil, time.Unix(2, 0))
	key := ktypes.NamespacedName{Namespace: "default", Name: "policy"}
	current, found := resources.EnvoyPatchPolicyStatuses.Load(key)
	require.True(t, found, "a pending current owner must preserve the last published status")
	require.Equal(t, old.Status, current)

	publisher.publish("edge", xtypes.EnvoyPatchPolicyStatuses{updated}, time.Unix(2, 0))
	current, found = resources.EnvoyPatchPolicyStatuses.Load(key)
	require.True(t, found)
	require.Equal(t, updated.Status, current)
}

func TestPatchStatusRetargetBackDoesNotRestoreOlderStatus(t *testing.T) {
	resources := new(message.ProviderResources)
	inputs := new(message.XdsIR)
	old := patchStatus("policy", "edge", 1)
	updated := patchStatus("policy", "private", 2)
	storePatchInput(inputs, "edge", old)
	storePatchInput(inputs, "private")
	publisher := newPatchStatusPublisher(resources, inputs)
	publisher.publish("edge", xtypes.EnvoyPatchPolicyStatuses{old}, time.Unix(1, 0))
	publisher.publish("private", nil, time.Unix(1, 0))

	storePatchInput(inputs, "edge")
	storePatchInput(inputs, "private", updated)
	publisher.publish("private", xtypes.EnvoyPatchPolicyStatuses{updated}, time.Unix(2, 0))

	// Inputs advance while the previous batch is still completing. The policy
	// moves back to edge before its old removal has finished translating.
	storePatchInput(inputs, "private")
	storePatchInput(inputs, "edge", patchStatus("policy", "edge", 3))
	publisher.publish("other", nil, time.Unix(2, 0))
	key := ktypes.NamespacedName{Namespace: "default", Name: "policy"}
	require.Equal(t, updated.Status, resources.EnvoyPatchPolicyStatuses.LoadAll()[key])
}
