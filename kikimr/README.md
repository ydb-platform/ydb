# Yandex Database (YDB aka KiKiMR) and Related Projects

See <https://wiki.yandex-team.ru/kikimr> for details.

> For client SDK, examples, API description go to the `public` directory.

## YDB Directory Structure

`blockstore` -- a contract for Blockstore project (messages and their ids);
`core` -- YDB implementation;
`docs` -- documentation as a code;
`driver` -- YDB binary for Yandex (has some internal deps like Unified Agent, TVM, etc);
`driver_lib` -- a common part for `driver` and `driver_oss`;
`driver_oss` -- YDB binary for open source;
`filestore` -- a contract for Filestore project (messages and their ids);
`kikhouse_new` -- ClickHouse over YDB;
`library` -- standalone libraries used by YDB;
`mvp` -- a proxy to YDB, implements UI calls, etc;
`papers` -- papers about YDB;
`persqueue` -- persistent queue (aka Logbroker) implementation;
`public` -- SDKs, APIs and tools for YDB end users;
`services` -- grpc services for YDB public API;
`sqs` -- Amazon SQS (Simple Queue Service) API implementation on top of YDB. It is provided in Yandex.Cloud as
YMQ (Yandex Message Queue);
`streaming` -- YQL Streams implementation (as a part of Yandex Query service);
`ydbcp` -- YDB Control Plane;
`yf` -- Yandex Functions serverless computing and job execution service;
`yq` -- Yandex Query implementation;


## Rest
ci
deployment
juggler
production
scripts
serverless_proxy
testing
tools

