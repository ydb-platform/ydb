## gRPC metadata headers

{{ ydb-short-name }} uses the following gRPC metadata headers:

* gRPC headers which a client sends to {{ ydb-short-name }}:
  * `x-ydb-database` - database
  * `x-ydb-auth-ticket` - auth token from a credentials provider
  * `x-ydb-sdk-build-info` - {{ ydb-short-name }} SDK build info
  * `x-ydb-trace-id` - user-defined request ID. If not defined by client {{ ydb-short-name }} SDK generates automatically using [UUID](https://en.wikipedia.org/wiki/UUID) format
  * `x-ydb-application-name` - optional user-defined application name
  * `x-ydb-client-capabilities` - supported client SDK capabilities (`session-balancer` and other)
  * `x-ydb-client-pid` - client application process ID
  * `traceparent` - OpenTelemetry trace ID ([specification](https://w3c.github.io/trace-context/#header-name))

* gRPC headers which {{ ydb-short-name }} sends to client with responses:
  * `x-ydb-server-hints` - notifications from {{ ydb-short-name }} (such as `session-close` and other)
  * `x-ydb-consumed-units` - consumed units on the current request
