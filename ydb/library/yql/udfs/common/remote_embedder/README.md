# RemoteEmbedder UDF

Implements the [remote embedder RFC](https://wiki.yandex-team.ru/kikimr/developers/rfc/udf-dlja-udalennogo-jembeddera/)
using the [OpenAI-compatible embeddings API](https://developers.openai.com/api/reference/resources/embeddings/methods/create).

The bare SQL names `ApplyRemoteEmbedder` and `ApplyRemoteEmbedderStream` from
the RFC are aliases for the functions in the `RemoteEmbedder` module.

```text
RemoteEmbedder::ApplyRemoteEmbedder(
    Model:String, BaseUrl:String, AuthSecretName:String, Text:String,
    MaxRetries:Uint32? = 5
) -> Struct<Embedding:String, Status:Uint32>

RemoteEmbedder::ApplyRemoteEmbedderStream(
    Model:String, BaseUrl:String, AuthSecretName:String,
    InputStream:Stream<Struct<Text:String, PassThrough:T>>,
    BatchSize:Uint32? = 32, MaxTokensPerSecond:Uint32? = 0,
    MaxRetries:Uint32? = 5
) -> Stream<Struct<Embedding:String, Status:Uint32, PassThrough:T>>
```

`BaseUrl` is the API base URL, for example `https://api.openai.com/v1`.
The UDF appends `/embeddings`, accepting a trailing slash in the base URL.
HTTP and HTTPS are supported; HTTPS verifies the server certificate.
The token is read from the execution environment's UDF secure parameters using
`AuthSecretName` and sent as `Authorization: Bearer <token>`. In KQP queries,
`AuthSecretName` must be a non-empty String literal naming a database secret;
KQP resolves it using the normal secret permission checks and supplies it to
the UDF. Other UDF hosts must provide this secure parameter themselves.
Missing secrets and invalid configuration fail the query.

`Embedding` uses the binary float-vector representation produced by
`Knn::ToBinaryStringFloat` (without its type tag), suitable for storage in a
vector column. An unsuccessful request produces an empty string.
`Status` is the HTTP response status, normally `200` on success; `0` means a
transport failure, and `502` also represents an invalid embeddings response.
Malformed, incomplete, duplicate-index, or inconsistent-dimension responses
fail the entire batch. Response indices determine the association with input
rows; output order and the exact `PassThrough` type and value are preserved.

`MaxRetries` counts additional attempts after the initial request. Transport
errors, HTTP 408, 429 and 5xx responses are retried with exponential backoff
starting at 100 ms and capped at 10 seconds. Other HTTP failures and malformed
successful responses are returned immediately. Requests have a 10-second
connection timeout and a 60-second socket timeout.

`BatchSize` must be positive. The stream buffers at most one batch and forwards
upstream yields without losing partially buffered rows. It flushes the final
partial batch at end of input; an empty stream sends no request. HTTP calls,
retry backoff and throttling are synchronous.

`MaxTokensPerSecond` applies independently to each stream invocation; omitted,
NULL or zero disables throttling. As no model-specific tokenizer is available,
the limiter conservatively estimates one token per input byte (at least one per
text), then accounts for any larger `usage.prompt_tokens` reported by the
server. Requests, including retries, are spaced by that token budget. The first
batch can be sent immediately, including a batch larger than one second's
budget. This is an estimate, not an exact model token count or a cluster-wide
quota.

```sql
SELECT RemoteEmbedder::ApplyRemoteEmbedder(
    'text-embedding-3-large', 'https://api.openai.com/v1', 'openai_token', 'Дом');

$requests = AsList(
    AsStruct('Молоко' AS Text, AsStruct(1 AS Id) AS PassThrough),
    AsStruct('Корова' AS Text, AsStruct(2 AS Id) AS PassThrough)
);

SELECT Embedding, Status, PassThrough
FROM (
    PROCESS AS_TABLE($requests)
    USING RemoteEmbedder::ApplyRemoteEmbedderStream(
        'text-embedding-3-large', 'https://api.openai.com/v1', 'openai_token',
        TableRows(), 32u, 10240u, 5u)
);
```
