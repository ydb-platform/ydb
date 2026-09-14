# Data at rest encryption

{{ ydb-short-name }} supports transparent data encryption at the [DS proxy](../../concepts/glossary.md#ds-proxy) level using the [ChaCha8](https://cr.yp.to/chacha/chacha-20080128.pdf) algorithm. {{ ydb-short-name }} includes two implementations of this algorithm, which switch depending on the availability of the AVX-512F instruction set.

By default, data at rest encryption is disabled. For instructions on enabling it, refer to the [{#T}](../../reference/configuration/domains_config.md#domains-blob) section.

For more details on the implementation, refer to [ydb/core/blobstorage/dsproxy/dsproxy_encrypt.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/dsproxy/dsproxy_encrypt.cpp) and [ydb/core/blobstorage/crypto](https://github.com/ydb-platform/ydb/tree/main/ydb/core/blobstorage/crypto).
