---
title: Concurrent Translation
weight: 90
---

Envoy Gateway processes up to two GatewayClasses concurrently when translating
Gateway API resources, and up to two gateway configurations concurrently when
building xDS resources. This is enabled automatically.

A smaller gateway can publish its configuration while another gateway in the
same batch is still translating. Each stage completes its current batch before
processing the next one, preserving update and deletion order.

The two stages can run at the same time. Concurrency can increase peak controller
memory usage, so monitor memory when sizing the controller for large configurations.
A single gateway's translation is not split between workers.
