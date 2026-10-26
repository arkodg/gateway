// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"io"
	"sync"
	"testing"

	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/stretchr/testify/require"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/envoyproxy/gateway/internal/ir"
	"github.com/envoyproxy/gateway/internal/logging"
	"github.com/envoyproxy/gateway/internal/xds/types"
)

func TestTranslateDoesNotMutateSharedPatchStatus(t *testing.T) {
	input := &ir.Xds{EnvoyPatchPolicies: []*ir.EnvoyPatchPolicy{{
		EnvoyPatchPolicyStatus: ir.EnvoyPatchPolicyStatus{
			Name: "policy", Namespace: "default", Generation: 1,
			Status: &gwapiv1.PolicyStatus{Ancestors: []gwapiv1.PolicyAncestorStatus{{
				AncestorRef: gwapiv1.ParentReference{Name: "gateway"},
			}}},
		},
		JSONPatches: []*ir.JSONPatchConfig{{Type: resourcev3.ClusterType, Name: "missing", Operation: ir.JSONPatchOperation{Op: "invalid"}}},
	}}}
	before := input.DeepCopy()
	var workers sync.WaitGroup
	var results [2]*types.ResourceVersionTable
	for i := range results {
		workers.Go(func() {
			translator := &Translator{Logger: logging.DefaultLogger(io.Discard, egv1a1.LogLevelError)}
			results[i], _ = translator.Translate(t.Context(), input)
		})
	}
	workers.Wait()
	require.Equal(t, before, input, "patch translation must leave published IR immutable")
	for _, result := range results {
		require.NotNil(t, result)
		require.Len(t, result.EnvoyPatchPolicyStatuses, 1)
		require.NotSame(t, input.EnvoyPatchPolicies[0].Status, result.EnvoyPatchPolicyStatuses[0].Status)
		require.NotEmpty(t, result.EnvoyPatchPolicyStatuses[0].Status.Ancestors[0].Conditions)
	}
	require.NotSame(t, results[0].EnvoyPatchPolicyStatuses[0].Status, results[1].EnvoyPatchPolicyStatuses[0].Status)
}
