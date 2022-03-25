# Balancing

{% include [work in progress message](../../_includes/addition.md) %}

{{ ydb-short-name }} uses client load balancing because it is more efficient when a lot of traffic from multiple client applications comes to a database.
In most cases, it just works in the {{ ydb-short-name }} SDK. However, sometimes specific settings for client load balancing are required, for example, to reduce server hops and request time or to distribute the load across availability zones.

This section contains code recipes with client load balancing settings in different {{ ydb-short-name }} SDKs.

