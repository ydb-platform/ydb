# YDB test recipe

The YDB test recipe starts a local YDB cluster for tests built with `ya make` and exports its connection settings. Add it to a test target with:

```yamake
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
```

The recipe always exports `YDB_ENDPOINT`, `YDB_DATABASE`, and `YDB_CONNECTION_STRING`.

## HTTP APIs

Set `YDB_ENABLE_HTTP_PROXY=true` to start the unified HTTP proxy. The recipe then exports its base URL as `YDB_HTTP_PROXY_ENDPOINT`. DataStreams-compatible Kinesis API is available whenever this proxy is enabled. AWS clients should use `ru-central1` as the signing region.

Set `YDB_ENABLE_SQS_TOPIC_API=true` to enable SQS API backed by YDB Topics. This option implies `YDB_ENABLE_HTTP_PROXY=true`, so setting both variables is not required. The legacy YMQ-backed SQS API is not enabled by this option.

Shared topic consumers additionally require the `enable_topic_message_level_parallelism` feature flag. A test using Topic SQS API can configure the recipe as follows:

```yamake
ENV(YDB_ENABLE_SQS_TOPIC_API=true)
ENV(YDB_FEATURE_FLAGS="enable_topic_message_level_parallelism")

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
```

Use the exported proxy endpoint together with the database path when creating an SQS client:

```python
import os

import boto3

database = '/' + os.environ['YDB_DATABASE'].strip('/')
client = boto3.client(
    'sqs',
    aws_access_key_id='unused',
    aws_secret_access_key='unused',
    aws_session_token='root@builtin',
    endpoint_url=os.environ['YDB_HTTP_PROXY_ENDPOINT'] + database,
    region_name='ru-central1',
)
```

When invoking `ydb_recipe` directly, `--enable-http-proxy` and `--enable-sqs-topic-api` are the command-line equivalents of the environment variables.
