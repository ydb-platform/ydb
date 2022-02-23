# Использование Docker-контейнера YDB

Для отладки или тестирования вы можете запустить [Docker](https://docs.docker.com/get-docker/)-контейнер YDB.

В результате выполнения описанных ниже инструкций вы получите локальную базу данных YDB, к которой можно будет обратиться по следующим реквизитам:

{% list tabs %}

- gRPC

  - [Эндпоинт](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
  - [Размещение базы данных](../../../concepts/connect.md#database): `/local`
  - [Аутентификация](../../../concepts/connect.md#auth-modes): Анонимная (без аутентификации)

- gRPCs/TLS

  - [Эндпоинт](../../../concepts/connect.md#endpoint): `grpcs://localhost:2135`
  - [Размещение базы данных](../../../concepts/connect.md#database): `/local`
  - [Аутентификация](../../../concepts/connect.md#auth-modes): Анонимная (без аутентификации)

{% endlist %}
