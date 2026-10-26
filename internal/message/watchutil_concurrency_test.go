// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package message_test

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/telepresenceio/watchable"

	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/envoyproxy/gateway/internal/logging"
	"github.com/envoyproxy/gateway/internal/message"
)

func batchSubscriber(t *testing.T, handle func(message.Update[string, int], chan error)) (chan watchable.Snapshot[string, int], <-chan struct{}, func()) {
	t.Helper()
	input := make(chan watchable.Snapshot[string, int], 8)
	done := make(chan struct{})
	closeInput := sync.OnceFunc(func() { close(input) })
	go func() {
		defer close(done)
		message.HandleSubscriptionWithConcurrency(logging.DefaultLogger(io.Discard, egv1a1.LogLevelError),
			message.Metadata{Runner: "batch-test"}, input, 2, handle)
	}()
	t.Cleanup(func() {
		closeInput()
		receiveBatch(t, done)
	})
	return input, done, closeInput
}

func receiveBatch[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for batch subscriber")
		var zero T
		return zero
	}
}

func sendBatch(t *testing.T, ch chan<- watchable.Snapshot[string, int], snapshot watchable.Snapshot[string, int]) {
	t.Helper()
	select {
	case ch <- snapshot:
	case <-time.After(5 * time.Second):
		t.Fatal("batch subscriber stopped receiving updates")
	}
}

func TestBatchSubscriptionPublishesIndependentKeysAndWaitsBetweenBatches(t *testing.T) {
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	started := make(chan message.Update[string, int], 8)
	published := make(chan message.Update[string, int], 8)
	input, done, closeInput := batchSubscriber(t, func(update message.Update[string, int], _ chan error) {
		started <- update
		if update.Key == "private" && update.Value == 1 {
			<-gate
		}
		published <- update
	})
	sendBatch(t, input, watchable.Snapshot[string, int]{State: map[string]int{"private": 1, "edge": 10}})
	first, second := receiveBatch(t, started), receiveBatch(t, started)
	require.True(t, first.Initial)
	require.True(t, second.Initial)
	require.ElementsMatch(t, []string{"private", "edge"}, []string{first.Key, second.Key})
	require.Equal(t, "edge", receiveBatch(t, published).Key, "edge must publish while private is blocked")
	sendBatch(t, input, watchable.Snapshot[string, int]{Updates: []watchable.Update[string, int]{
		{Key: "private", Value: 2}, {Key: "private", Value: 3}, {Key: "edge", Value: 11},
	}})
	closeInput()
	select {
	case update := <-started:
		t.Fatalf("next batch started before private finished: %+v", update)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-done:
		t.Fatal("subscriber returned with an in-flight translation")
	default:
	}
	release()
	require.Equal(t, 1, receiveBatch(t, published).Value)
	first, second = receiveBatch(t, started), receiveBatch(t, started)
	require.False(t, first.Initial)
	require.False(t, second.Initial)
	require.ElementsMatch(t, []int{3, 11}, []int{first.Value, second.Value})
	require.ElementsMatch(t, []int{3, 11}, []int{receiveBatch(t, published).Value, receiveBatch(t, published).Value})
	receiveBatch(t, done)
	require.Empty(t, started, "the superseded update must not run")
}

func TestBatchSubscriptionDeleteAndRecreate(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "recreate"}[recreate], func(t *testing.T) {
			gate := make(chan struct{})
			release := sync.OnceFunc(func() { close(gate) })
			defer release()
			started := make(chan message.Update[string, int], 8)
			input, done, closeInput := batchSubscriber(t, func(update message.Update[string, int], _ chan error) {
				started <- update
				if update.Value == 1 {
					<-gate
				}
			})
			sendBatch(t, input, watchable.Snapshot[string, int]{State: map[string]int{"gateway": 1}})
			receiveBatch(t, started)
			updates := []watchable.Update[string, int]{{Key: "gateway", Value: 2}, {Key: "gateway", Delete: true}}
			if recreate {
				updates = append(updates, watchable.Update[string, int]{Key: "gateway", Value: 3})
			}
			sendBatch(t, input, watchable.Snapshot[string, int]{Updates: updates})
			closeInput()
			release()
			last := receiveBatch(t, started)
			require.Equal(t, !recreate, last.Delete)
			if recreate {
				require.Equal(t, 3, last.Value)
			}
			receiveBatch(t, done)
			require.Empty(t, started)
		})
	}
}

func TestBatchSubscriptionBoundsInitialReplay(t *testing.T) {
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	started := make(chan message.Update[string, int], 3)
	input, done, closeInput := batchSubscriber(t, func(update message.Update[string, int], _ chan error) {
		started <- update
		<-gate
	})
	sendBatch(t, input, watchable.Snapshot[string, int]{State: map[string]int{"one": 1, "two": 2, "three": 3}})
	require.True(t, receiveBatch(t, started).Initial)
	require.True(t, receiveBatch(t, started).Initial)
	select {
	case <-started:
		t.Fatal("more than two keys ran concurrently")
	case <-time.After(50 * time.Millisecond):
	}
	closeInput()
	release()
	require.True(t, receiveBatch(t, started).Initial)
	receiveBatch(t, done)
}

func TestBatchSubscriptionRecoversAndDrainsErrors(t *testing.T) {
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	started := make(chan int, 2)
	input, done, closeInput := batchSubscriber(t, func(update message.Update[string, int], errorsOut chan error) {
		started <- update.Value
		if update.Value == 1 {
			<-gate
			panic("translation failed")
		}
		for range 30 {
			errorsOut <- errors.New("translation error")
		}
	})
	sendBatch(t, input, watchable.Snapshot[string, int]{State: map[string]int{"gateway": 1}})
	require.Equal(t, 1, receiveBatch(t, started))
	sendBatch(t, input, watchable.Snapshot[string, int]{Updates: []watchable.Update[string, int]{{Key: "gateway", Value: 2}}})
	closeInput()
	release()
	require.Equal(t, 2, receiveBatch(t, started), "a panic must release the key for its pending update")
	receiveBatch(t, done)
}
