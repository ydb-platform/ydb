## Выгрузите Docker-образ {{ ydb-short-name }} {#install}

Выгрузите актуальную публичную версию Docker-образа:

```bash
docker pull cr.yandex/yc/yandex-docker-local-ydb:latest
```

Проверьте, что Docker-образ успешно выгружен:

```bash
docker image list
```

Результат выполнения:

```bash
REPOSITORY                             TAG       IMAGE ID       CREATED        SIZE
cr.yandex/yc/yandex-docker-local-ydb   latest    b73c5c1441af   2 months ago   793MB
```