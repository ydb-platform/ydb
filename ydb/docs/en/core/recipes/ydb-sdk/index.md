# {{ ydb-short-name }} SDK code recipes

This section contains code recipes in different programming languages for a variety of tasks that are common when working with the {{ ydb-short-name }} SDK.

Table of contents:

- [Initializing the driver](init.md)
- [Authentication](auth.md)

  - [Using a token](auth-access-token.md)
  - [Anonymous](auth-anonymous.md)
  - [Service account file](auth-service-account.md)
  - [Metadata service](auth-metadata.md)
  - [Using environment variables](auth-env.md)
  - [Username and password based](auth-static.md)

- [Balancing](balancing.md)

  - [Random choice](balancing-random-choice.md)
  - [Prefer the nearest data center](balancing-prefer-local.md)
  - [Prefer the availability zone](balancing-prefer-location.md)

- [Running repeat queries](retry.md)
- [Setting the session pool size](session-pool-limit.md)
- [Inserting data](upsert.md)
- [Bulk upsert of data](bulk-upsert.md)
- [Setting up the transaction execution mode](tx-control.md)
- [Configuring time to live (TTL)](ttl.md)
- Coordination

  - [Distributed lock](distributed-lock.md)
  - [Service discovery](service-discovery.md)
  - [Configuration publication](config-publication.md)
  - [Leader election](leader-election.md)

- [Troubleshooting](debug.md)

  - [Enable logging](debug-logs.md)
  - [Enable metrics in Prometheus](debug-prometheus.md)
  - [Enable tracing in Jaeger](debug-jaeger.md)

See also:

- [{#T}](../../dev/index.md)
- [{#T}](../../dev/example-app/index.md)
- [{#T}](../../reference/ydb-sdk/index.md)
