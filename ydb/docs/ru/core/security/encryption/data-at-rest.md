# Шифрование данных при хранении

{{ ydb-short-name }} поддерживает прозрачное шифрование данных на уровне [прокси распределённого хранилища](../../concepts/glossary.md#ds-proxy) с использованием алгоритма [ChaCha8](https://cr.yp.to/chacha/chacha-20080128.pdf). {{ ydb-short-name }} включает две реализации этого алгоритма, которые переключаются в зависимости от доступности набора инструкций AVX-512F.

По умолчанию шифрование данных при хранении отключено. Инструкции по его включению можно найти в разделе [{#T}](../../reference/configuration/domains_config.md#domains-blob).

Более подробную информацию о реализации можно найти в [ydb/core/blobstorage/dsproxy/dsproxy_encrypt.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/dsproxy/dsproxy_encrypt.cpp) и [ydb/core/blobstorage/crypto](https://github.com/ydb-platform/ydb/tree/main/ydb/core/blobstorage/crypto).
