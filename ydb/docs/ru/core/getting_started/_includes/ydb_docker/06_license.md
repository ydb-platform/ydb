## Лицензия и используемые компоненты {#license}

В корне Docker-контейнера расположены файл с текстом лицензионного соглашения (`LICENSE`) и список используемых компонентов и их лицензии (`THIRD_PARTY_LICENSES`).

Просмотрите текст лицензионного соглашения:

```bash
sudo docker run --rm -it --entrypoint cat cr.yandex/yc/yandex-docker-local-ydb LICENSE
```

Просмотрите все использованные при создании компоненты и их лицензии:

```bash
sudo docker run --rm -it --entrypoint cat cr.yandex/yc/yandex-docker-local-ydb THIRD_PARTY_LICENSES
```
