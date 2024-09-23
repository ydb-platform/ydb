# Обзор gRPC API

{{ ydb-short-name }} предоставляет gRPC API, с помощью которого вы можете управлять [ресурсами](../../concepts/datamodel/index.md) и данными БД. Для описания методов и структур данных API используется [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/proto3) (proto 3). Подробнее смотрите [.proto-спецификации с комментариями](https://github.com/ydb-platform/ydb-api-protos). Также {{ ydb-short-name }} использует специальные [заголовки метаданных gRPC](grpc-headers.md).

Доступны следующие сервисы:

* [{#T}](health-check-api.md).
