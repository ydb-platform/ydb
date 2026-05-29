# Code recipes using {{ ydb-short-name }} SDK and frameworks

This section contains code recipes in various programming languages for solving common practical tasks using {{ ydb-short-name }} SDK.

Contents:

- [Driver initialization](init.md)
- [Authentication](auth.md)

  - [Using a token](auth-access-token.md)
  - [Anonymous](auth-anonymous.md)
  - [Service account file](auth-service-account.md)
  - [Metadata service](auth-metadata.md)
  - [Using environment variables](auth-env.md)
  - [Using login and password](auth-static.md)

- [Load balancing](balancing.md)
  - [Uniform random selection](balancing-random-choice.md)
  - [Prefer the nearest data center](balancing-prefer-local.md)
  - [Prefer the availability zone](balancing-prefer-location.md)

- [Retrying requests](retry.md)
- [Setting the session pool size](session-pool-limit.md)
- [Inserting data](upsert.md)
- [Batch data insertion](bulk-upsert.md)
- [Setting the transaction execution mode](tx-control.md)
- [Configuring table row time-to-live (TTL)](ttl.md)
- [Vector search](vector-search.md)
- Coordination

  - [Distributed locking](distributed-lock.md)
  - [Service discovery](service-discovery.md)
  - [Configuration publication](config-publication.md)
  - [Leader election](leader-election.md)

- [Troubleshooting](debug.md)

  - [Enable logging](debug-logs.md)
  - [Connect metrics to Prometheus](debug-prometheus.md)
  - [Tracing with OpenTelemetry](debug-otel.md)

See also:

- [{#T}](../../dev/index.md)
- [{#T}](../../dev/example-app/index.md)
- [{#T}](../../reference/ydb-sdk/index.md)
