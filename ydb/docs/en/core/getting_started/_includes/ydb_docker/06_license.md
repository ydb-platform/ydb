## License and components {#license}

The Docker container root includes a file with the license agreement (`LICENSE`) and a list of the components used and their licenses (`THIRD_PARTY_LICENSES`).

Read the license agreement:

```bash
sudo docker run --rm -it --entrypoint cat cr.yandex/yc/yandex-docker-local-ydb LICENSE
```

View all the included components and their licenses:

```bash
sudo docker run --rm -it --entrypoint cat cr.yandex/yc/yandex-docker-local-ydb THIRD_PARTY_LICENSES
```
