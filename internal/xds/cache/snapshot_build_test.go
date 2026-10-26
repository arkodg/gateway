// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package cache

import (
	"sync"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	cachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"

	"github.com/envoyproxy/gateway/internal/xds/types"
)

// gatedCluster holds protobuf hashing at a deterministic point without delaying
// resource-name lookup or changing the bytes that are eventually marshalled.
type gatedCluster struct {
	*clusterv3.Cluster
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *gatedCluster) ProtoReflect() protoreflect.Message {
	return gatedClusterMessage{Message: c.Cluster.ProtoReflect(), cluster: c}
}

type gatedClusterMessage struct {
	protoreflect.Message
	cluster *gatedCluster
}

func (m gatedClusterMessage) ProtoMethods() *protoiface.Methods { return nil }

func (m gatedClusterMessage) Range(f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	m.cluster.once.Do(func() {
		close(m.cluster.entered)
		<-m.cluster.release
	})
	m.Message.Range(f)
}

func awaitSnapshot[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot work was blocked by another build")
		var zero T
		return zero
	}
}

func TestSnapshotDoesNotHashSOTWOnlyKey(t *testing.T) {
	cache := newTestSnapshotCache(t)
	cache.streamIDNodeInfo[1] = &corev3.Node{Id: "private-node", Cluster: "private"}
	cache.streamIDNodeInfo[2] = &corev3.Node{Id: "edge-node", Cluster: "edge"}
	cache.streamDuration[1] = time.Now()
	cache.deltaStreamDuration[2] = time.Now()
	cluster := &gatedCluster{Cluster: &clusterv3.Cluster{Name: "private"}, entered: make(chan struct{}), release: make(chan struct{})}
	close(cluster.release)
	require.NoError(t, cache.GenerateNewSnapshot("private", types.XdsResources{resourcev3.ClusterType: []cachetypes.Resource{cluster}}, t.Context()))
	select {
	case <-cluster.entered:
		t.Fatal("another key's delta stream must not cause unnecessary hashing for a SOTW-only key")
	default:
	}
}

func TestSnapshotBuildDoesNotBlockOtherKeys(t *testing.T) {
	for _, action := range []string{"other-key", "newer-same-key", "delete", "delete-and-recreate"} {
		t.Run(action, func(t *testing.T) {
			cache := newTestSnapshotCache(t)
			cache.streamIDNodeInfo[1] = &corev3.Node{Id: "private-node", Cluster: "private"}
			cache.streamIDNodeInfo[2] = &corev3.Node{Id: "edge-node", Cluster: "edge"}
			cache.deltaStreamDuration[1] = time.Now()
			cache.deltaStreamDuration[2] = time.Now()
			old := &gatedCluster{Cluster: &clusterv3.Cluster{Name: "old"}, entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(old.release) })
			defer release()
			oldDone := make(chan error, 1)
			go func() {
				oldDone <- cache.GenerateNewSnapshot("private", types.XdsResources{resourcev3.ClusterType: []cachetypes.Resource{old}}, t.Context())
			}()
			awaitSnapshot(t, old.entered)
			key, node := "private", "private-node"
			if action == "other-key" {
				key, node = "edge", "edge-node"
			}
			latest := make(chan error, 1)
			go func() {
				if action == "delete" || action == "delete-and-recreate" {
					if err := cache.GenerateNewSnapshot(key, nil, t.Context()); err != nil {
						latest <- err
						return
					}
					if action == "delete" {
						latest <- nil
						return
					}
				}
				latest <- cache.GenerateNewSnapshot(key, types.XdsResources{
					resourcev3.ClusterType: []cachetypes.Resource{&clusterv3.Cluster{Name: "new"}},
				}, t.Context())
			}()
			require.NoError(t, awaitSnapshot(t, latest), "a blocked hash must not hold either cache lock")
			release()
			require.NoError(t, awaitSnapshot(t, oldDone))
			snapshot, err := cache.GetSnapshot(node)
			require.NoError(t, err)
			clusters := snapshot.GetResources(resourcev3.ClusterType)
			if action == "delete" {
				require.Empty(t, clusters, "an old build must not resurrect a deleted snapshot")
			} else {
				require.Contains(t, clusters, "new")
				require.NotContains(t, clusters, "old", "an old build must not overwrite a newer snapshot")
				require.NotEmpty(t, snapshot.GetVersionMap(resourcev3.ClusterType))
			}
		})
	}
}
