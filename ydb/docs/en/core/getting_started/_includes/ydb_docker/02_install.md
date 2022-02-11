## Pull a {{ ydb-short-name }} Docker image {#install}

Pull the current public version of the Docker image:

```bash
docker pull cr.yandex/yc/yandex-docker-local-ydb:latest
```

Make sure the Docker image has been pulled:

```bash
docker image list
```

Output:

```bash
REPOSITORY                             TAG       IMAGE ID       CREATED        SIZE
cr.yandex/yc/yandex-docker-local-ydb   latest    b73c5c1441af   2 months ago   793MB
```