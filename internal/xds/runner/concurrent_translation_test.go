// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package runner

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/stretchr/testify/require"
	"github.com/telepresenceio/watchable"

	"github.com/envoyproxy/gateway/internal/envoygateway/config"
	"github.com/envoyproxy/gateway/internal/ir"
	"github.com/envoyproxy/gateway/internal/message"
	"github.com/envoyproxy/gateway/internal/xds/cache"
	"github.com/envoyproxy/gateway/internal/xds/types"
)

type gatedSnapshotCache struct {
	cache.SnapshotCacheWithCallbacks
	entered   chan struct{}
	release   chan struct{}
	published chan string
}

func (c *gatedSnapshotCache) GenerateNewSnapshot(key string, resources types.XdsResources, _ context.Context) error {
	listener := resources[resourcev3.ListenerType][0].(*listenerv3.Listener)
	port := listener.GetAddress().GetSocketAddress().GetPortValue()
	if key == "private" && port == 8080 {
		close(c.entered)
		<-c.release
	}
	c.published <- fmt.Sprintf("%s:%d", key, port)
	return nil
}

func receiveTranslation[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("translation did not complete")
		var zero T
		return zero
	}
}

func TestRunnerConcurrentTranslationPreservesBatchOrder(t *testing.T) {
	cfg, err := config.New(io.Discard, io.Discard)
	require.NoError(t, err)
	runner := New(&Config{Server: *cfg, XdsIR: new(message.XdsIR), ProviderResources: new(message.ProviderResources)})
	snapshots := &gatedSnapshotCache{entered: make(chan struct{}), release: make(chan struct{}), published: make(chan string, 3)}
	runner.cache = snapshots
	subscription := make(chan watchable.Snapshot[string, *message.XdsIRWithContext], 2)
	done := make(chan struct{})
	release := sync.OnceFunc(func() { close(snapshots.release) })
	closeInput := sync.OnceFunc(func() { close(subscription) })
	t.Cleanup(func() {
		release()
		closeInput()
		receiveTranslation(t, done)
	})
	go func() {
		runner.translateFromSubscription(subscription)
		close(done)
	}()
	value := func(port uint32) *message.XdsIRWithContext {
		return &message.XdsIRWithContext{XdsIR: &ir.Xds{HTTP: []*ir.HTTPListener{{
			CoreListenerDetails: ir.CoreListenerDetails{Name: "http", Address: "0.0.0.0", Port: port},
			Hostnames:           []string{"example.com"},
		}}}}
	}
	subscription <- watchable.Snapshot[string, *message.XdsIRWithContext]{State: map[string]*message.XdsIRWithContext{
		"private": value(8080), "edge": value(8080),
	}}
	receiveTranslation(t, snapshots.entered)
	require.Equal(t, "edge:8080", receiveTranslation(t, snapshots.published))
	subscription <- watchable.Snapshot[string, *message.XdsIRWithContext]{Updates: []watchable.Update[string, *message.XdsIRWithContext]{
		{Key: "private", Value: value(8081)},
	}}
	closeInput()
	select {
	case published := <-snapshots.published:
		t.Fatalf("next batch published before private finished: %s", published)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	require.Equal(t, "private:8080", receiveTranslation(t, snapshots.published))
	require.Equal(t, "private:8081", receiveTranslation(t, snapshots.published))
}
