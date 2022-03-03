# Использование Docker-контейнера {{ ydb-short-name }}

Для отладки или тестирования вы можете запустить [Docker](https://docs.docker.com/get-docker/)-контейнер YDB.

В результате выполнения описанных ниже инструкций вы получите локальную базу данных YDB, к которой можно будет обратиться по следующим реквизитам:

{% list tabs %}

- gRPC

  * [эндпоинт](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`;
  * [размещение базы данных](../../../concepts/connect.md#database): `/local`;
  * [аутентификация](../../../concepts/connect.md#auth-modes): Анонимная (без аутентификации).

- gRPCs/TLS

  * [эндпоинт](../../../concepts/connect.md#endpoint): `grpcs://localhost:2135`;
  * [размещение базы данных](../../../concepts/connect.md#database): `/local`;
  * [аутентификация](../../../concepts/connect.md#auth-modes): Анонимная (без аутентификации).

{% endlist %}
