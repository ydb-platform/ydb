# Remote embedder UDFs

Implements the [remote embedder RFC](https://wiki.yandex-team.ru/kikimr/developers/rfc/udf-dlja-udalennogo-jembeddera/)
using the [OpenAI-compatible embeddings API](https://developers.openai.com/api/reference/resources/embeddings/methods/create).

In YDB's built-in `Knn` module, `ApplyRemoteEmbedder` is an alias for
`Knn::ApplyRemoteEmbedder`.
Use the qualified name in view definitions and in SQL declarations captured by
views. Generated columns cannot call the UDF.
The first argument selects the scalar or streaming form.

```text
Knn::ApplyRemoteEmbedder(
    Text:String, Model:String, BaseUrl:String, AuthSecretName:String,
    MaxRetries:Uint32? = 5
) -> Struct<Embedding:String, Status:Uint32>

Knn::ApplyRemoteEmbedder(
    InputStream:Stream<Struct<Text:String, PassThrough:T>>,
    Model:String, BaseUrl:String, AuthSecretName:String,
    BatchSize:Uint32? = 32, MaxTokensPerSecond:Uint32? = 0,
    MaxRetries:Uint32? = 5
) -> Stream<Struct<Embedding:String, Status:Uint32, PassThrough:T>>
```

`BaseUrl` is the API base URL, for example `https://api.openai.com/v1`.
The UDF appends `/embeddings`, accepting a trailing slash in the base URL.
Database secrets require HTTPS with server certificate verification.
The scalar request sends `input` as a JSON string. The streaming request sends
each batch as a JSON array.
The token is read from the execution environment's UDF secure parameters using
`AuthSecretName` and sent as `Authorization: Bearer <token>`. In KQP queries,
`AuthSecretName` must be a non-empty String literal naming a database secret;
KQP resolves it using the normal secret permission checks and supplies it to
the UDF.
Missing secrets and invalid configuration fail the query.
An administrator must bind each absolute schema-secret path to an HTTPS origin
in `QueryServiceConfig.RemoteEmbedderSecretOrigins`. For example, bind
`/Root/openai_token` to `https://api.openai.com` for the base URL above. Both
the hostname and effective port must match. KQP checks the live binding and
the node's `QueryServiceConfig.HostnamePatterns` before each POST.

`Embedding` uses the binary float-vector representation produced by
`Knn::ToBinaryStringFloat` (without its type tag), suitable for storage in a
vector column. An unsuccessful request produces an empty string.
`Status` is the HTTP response status, normally `200` on success; `0` means a
transport failure, and `502` also represents an invalid embeddings response
or a response body larger than 16 MiB.
Malformed, incomplete, duplicate-index, or inconsistent-dimension responses
fail the entire batch. Response indices determine the association with input
rows; output order and the exact `PassThrough` type and value are preserved.

`MaxRetries` counts additional attempts after the initial request. Transport
errors, HTTP 408, 429 and 5xx responses are retried with exponential backoff
starting at 100 ms and capped at 10 seconds. Other HTTP failures and malformed
successful responses are returned immediately. The transport accepts identity
encoding and limits each attempt to 60 seconds, including up to 10 seconds
for TCP connection establishment. Each scalar call or stream batch has a
two-minute deadline across attempts, backoff and rate-limit waits. A
rate-limit wait that exceeds the remaining budget fails the query.
Serialized embeddings are limited to 16 MiB per batch.

`BatchSize` must be positive. A batch holds at most 1,024 rows and has a 1 MiB
budget for text and estimated retained values. A row above that budget fails
the query. A pass-through value whose size cannot be estimated is processed on
its own. The stream forwards upstream yields without losing buffered rows and
flushes the final partial batch. An empty stream sends no request.
HTTP calls use a bounded background worker pool. Retry and rate-limit delays
use a separate timer queue. Scalar and streaming UDFs wait for these results
on the calling thread. A full worker or timer queue fails the query.

`MaxTokensPerSecond` applies independently to each stream invocation; omitted,
NULL or zero disables throttling. As no model-specific tokenizer is available,
the limiter conservatively estimates one token per input byte (at least one per
text), then accounts for any larger `usage.prompt_tokens` reported by the
server. A usage adjustment of two minutes or more makes the response invalid.
Requests, including retries, are spaced by that token budget. The first
batch can be sent immediately, including a batch larger than one second's
budget. This is an estimate, not an exact model token count or a cluster-wide
quota.

```sql
SELECT Knn::ApplyRemoteEmbedder(
    'Дом', 'text-embedding-3-large', 'https://api.openai.com/v1', '/Root/openai_token');

$requests = AsList(
    AsStruct('Молоко' AS Text, AsStruct(1 AS Id) AS PassThrough),
    AsStruct('Корова' AS Text, AsStruct(2 AS Id) AS PassThrough)
);

SELECT Embedding, Status, PassThrough
FROM (
    PROCESS AS_TABLE($requests)
    USING Knn::ApplyRemoteEmbedder(
        TableRows(), 'text-embedding-3-large', 'https://api.openai.com/v1',
        '/Root/openai_token', 32u, 10240u, 5u)
);
```
