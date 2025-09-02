# Docker image `{{ ydb_local_docker_image}}` tags naming

For the [{{ ydb_local_docker_image}}](https://hub.docker.com/r/ydbplatform/local-ydb) Docker image, the following naming rules apply for tags:

| Tag Name                 | Description                                                                                                             |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `latest`               | Corresponds to the most recent *stable* version of {{ ydb-short-name }} tested on production clusters. Rebuilt for each new {{ ydb-short-name }} release. |
| `edge`                 | A candidate for the next *stable* version, currently undergoing testing. Includes new features but may not be stable and thus unsuitable for production environments.  |
| `trunk`, `main`, `nightly` | The latest version of {{ ydb-short-name }} from the main development branch. Includes all recent changes and is rebuilt nightly. Similarly to `edge`, it is not suitable for production environments.      |
| `XX.Y`                 | Corresponds to the latest minor version of {{ ydb-short-name }} in a major release `XX.Y`, including all patches.                           |
| `XX.Y.ZZ`              | Corresponds to the {{ ydb-short-name }} release version `XX.Y.ZZ`.                                                                    |
| `XX.Y-slim`, `XX.Y.ZZ-slim` | Compressed binaries of {{ ydb-short-name }} (`ydbd` and `ydb`) with smaller image size but a slower startup. Uses [UPX](https://github.com/upx/upx). |
