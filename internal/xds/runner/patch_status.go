// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package runner

import (
	"maps"
	"slices"
	"sync"
	"time"

	ktypes "k8s.io/apimachinery/pkg/types"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/envoyproxy/gateway/internal/message"
	xtypes "github.com/envoyproxy/gateway/internal/xds/types"
)

type patchStatusEntry struct {
	status     *gwapiv1.PolicyStatus
	generation int64
	storedAt   time.Time
}

// patchStatusPublisher owns status cleanup by xDS key. A translation must not
// delete the statuses of another gateway that is still translating.
type patchStatusPublisher struct {
	mu             sync.Mutex
	resources      *message.ProviderResources
	inputs         *message.XdsIR
	byKey          map[string]map[ktypes.NamespacedName]patchStatusEntry
	published      map[ktypes.NamespacedName]patchStatusEntry
	initialPending map[string]struct{}
}

func newPatchStatusPublisher(resources *message.ProviderResources, inputs *message.XdsIR) *patchStatusPublisher {
	initial := patchPolicyOwners(inputs)
	p := &patchStatusPublisher{
		resources:      resources,
		inputs:         inputs,
		byKey:          make(map[string]map[ktypes.NamespacedName]patchStatusEntry),
		published:      make(map[ktypes.NamespacedName]patchStatusEntry),
		initialPending: make(map[string]struct{}, len(initial)),
	}
	for key := range initial {
		p.initialPending[key] = struct{}{}
	}
	for name, status := range resources.EnvoyPatchPolicyStatuses.LoadAll() {
		p.published[name] = patchStatusEntry{status: status}
	}
	return p
}

func patchPolicyOwners(inputs *message.XdsIR) map[string]map[ktypes.NamespacedName]struct{} {
	owners := make(map[string]map[ktypes.NamespacedName]struct{})
	// The predicate runs under the map's read lock. Gather only policy names and
	// return false to avoid deep-copying every gateway's complete IR.
	inputs.LoadAllMatching(func(key string, input *message.XdsIRWithContext) bool {
		names := make(map[ktypes.NamespacedName]struct{})
		if input != nil && input.XdsIR != nil {
			for _, policy := range input.XdsIR.EnvoyPatchPolicies {
				names[ktypes.NamespacedName{Namespace: policy.Namespace, Name: policy.Name}] = struct{}{}
			}
		}
		owners[key] = names
		return false
	})
	return owners
}

func (p *patchStatusPublisher) publish(key string, statuses xtypes.EnvoyPatchPolicyStatuses, storedAt time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	owned := make(map[ktypes.NamespacedName]patchStatusEntry, len(statuses))
	for _, status := range statuses {
		entry := patchStatusEntry{generation: status.Generation, storedAt: storedAt}
		if status.Status != nil && len(status.Status.Ancestors) > 0 {
			entry.status = status.Status
		}
		owned[ktypes.NamespacedName{Namespace: status.Namespace, Name: status.Name}] = entry
	}
	if len(owned) == 0 {
		delete(p.byKey, key)
	} else {
		p.byKey[key] = owned
	}
	delete(p.initialPending, key)

	// Inputs can move or delete a policy while an older key is still translating.
	// Only current owners may publish it; otherwise an old completion could
	// resurrect a status after the new owner has already removed the policy.
	currentOwners := patchPolicyOwners(p.inputs)
	currentPolicies := make(map[ktypes.NamespacedName]struct{})
	for _, names := range currentOwners {
		for name := range names {
			currentPolicies[name] = struct{}{}
		}
	}
	selected := make(map[ktypes.NamespacedName]patchStatusEntry)
	for _, owner := range slices.Sorted(maps.Keys(p.byKey)) {
		for name, entry := range p.byKey[owner] {
			if _, current := currentOwners[owner][name]; !current {
				continue
			}
			previous, found := selected[name]
			// Each policy has one target. If it moves between keys, prefer the newer
			// input, regardless of which translation finishes first. StoredAt also
			// handles policy recreation, where metadata.generation can reset.
			if !found || entry.storedAt.After(previous.storedAt) ||
				(entry.storedAt.Equal(previous.storedAt) && entry.generation > previous.generation) {
				selected[name] = entry
			}
		}
	}
	for name, entry := range selected {
		// An empty status still claims the policy, but must not replace its last
		// known status while the target is unresolved.
		if entry.status == nil {
			continue
		}
		previous := p.published[name]
		// Inputs may move back to an earlier owner while a batch is completing.
		// Its retained result must not replace a more recently published status.
		if entry.storedAt.Before(previous.storedAt) ||
			(entry.storedAt.Equal(previous.storedAt) && entry.generation < previous.generation) {
			continue
		}
		if previous.status != entry.status {
			p.resources.EnvoyPatchPolicyStatuses.Store(name, entry.status)
		}
		p.published[name] = entry
	}
	// Keep preexisting status until all initial keys have had a chance to claim it.
	if len(p.initialPending) == 0 {
		for name := range p.published {
			// A policy can move to a key whose translation has not completed yet.
			// Preserve its last status until that owner publishes or removes it.
			if _, present := currentPolicies[name]; !present {
				p.resources.EnvoyPatchPolicyStatuses.Delete(name)
				delete(p.published, name)
			}
		}
	}
}
