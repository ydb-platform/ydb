## Лицензия и используемые компоненты {#license}

В корне Docker-контейнера расположены файл с текстом лицензионного соглашения (`LICENSE`) и список используемых компонентов и их лицензии (`THIRD_PARTY_LICENSES`).

Просмотрите текст лицензионного соглашения:

```bash
docker run --rm -it --entrypoint cat {{ ydb_local_docker_image }} LICENSE
```

Просмотрите все использованные при создании компоненты и их лицензии:

```bash
docker run --rm -it --entrypoint cat {{ ydb_local_docker_image }} THIRD_PARTY_LICENSES
```
