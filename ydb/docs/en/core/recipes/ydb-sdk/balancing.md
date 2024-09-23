---
title: "Overview of client load balancing in {{ ydb-short-name }}"
description: "The section contains code recipes with client load balancing settings in different {{ ydb-short-name }} SDKs."
---
# Balancing

{% include [work in progress message](_includes/addition.md) %}

{{ ydb-short-name }} uses client load balancing because it is more efficient when a lot of traffic from multiple client applications comes to a database.
In most cases, it just works in the {{ ydb-short-name }} SDK. However, sometimes specific settings for client load balancing are required, for example, to reduce server hops and request time or to distribute the load across availability zones.

Note that custom balancing is limited when it comes to {{ ydb-short-name }} sessions. Custom balancing in the {{ ydb-short-name }} SDKs is performed only when creating a new {{ ydb-short-name }} session on a specific node. Once the session is created, all queries in this session are passed to the node where the session was created. Queries in the same {{ ydb-short-name }} session are not balanced between different {{ ydb-short-name }} nodes.

This section contains code recipes with client load balancing settings in different {{ ydb-short-name }} SDKs.

Table of contents:
- [Random choice](balancing-random-choice.md)
- [Prefer the nearest data center](balancing-prefer-local.md)
- [Prefer the specific availability zone](balancing-prefer-location.md)
