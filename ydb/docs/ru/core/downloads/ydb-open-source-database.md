<!-- markdownlint-disable no-emphasis-as-heading -->

# Загрузка {{ ydb-short-name }} Open-Source Database

{{ ydb-short-name }} Open-Source Database (`ydbd`) — исполняемый файл для запуска узла [кластера {{ ydb-short-name }}](../concepts/glossary.md#cluster). Распространяется под [лицензией Apache 2.0](https://github.com/ydb-platform/ydb/blob/main/LICENSE).

См. также [{#T}](./yandex-enterprise-database.md).

## Linux

#|
|| Версия |  Дата выпуска | Скачать | Список изменений ||
|| **v24.4** | > | > | > ||
|| v.24.4.4.12  | 03.06.25 | [Бинарный файл](https://binaries.ydb.tech/release/24.4.4.12/ydbd-24.4.4.12-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-4-4-12) ||
|| v.24.4.4.2   | 15.04.25 | [Бинарный файл](https://binaries.ydb.tech/release/24.4.4.2/ydbd-24.4.4.2-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-4-4-2) ||
|| **v24.3** | > | > | > ||
|| v.24.3.15.5   | 06.02.25 | [Бинарный файл](https://binaries.ydb.tech/release/24.3.15.5/ydbd-24.3.15.5-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-3-15-5) ||
|| v.24.3.11.14  | 09.01.25 | [Бинарный файл](https://binaries.ydb.tech/release/24.3.11.14/ydbd-24.3.11.14-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-3-11-14) ||
|| v.24.3.11.13  | 24.12.24 | [Бинарный файл](https://binaries.ydb.tech/release/24.3.11.13/ydbd-24.3.11.13-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-3-11-13) ||
|| **v24.2** | > | > | > ||
|| v.24.2.7  | 20.08.24 | [Бинарный файл](https://binaries.ydb.tech/release/24.2.7/ydbd-24.2.7-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-2) ||
|| **v24.1** | > | > | > ||
|| v.24.1.18 | 31.07.24 | [Бинарный файл](https://binaries.ydb.tech/release/24.1.18/ydbd-24.1.18-linux-amd64.tar.gz) | [См. список](../changelog-server.md#24-1) ||
|| **v23.4** | > | > | > ||
|| v.23.4.11 | 14.05.24 | [Бинарный файл](https://binaries.ydb.tech/release/23.4.11/ydbd-23.4.11-linux-amd64.tar.gz) | [См. список](../changelog-server.md#23-4) ||
|| **v23.3** | > | > | > ||
|| v.23.3.17 | 14.12.23 | [Бинарный файл](https://binaries.ydb.tech/release/23.3.17/ydbd-23.3.17-linux-amd64.tar.gz) | — ||
|| v.23.3.13 | 12.10.23 | [Бинарный файл](https://binaries.ydb.tech/release/23.3.13/ydbd-23.3.13-linux-amd64.tar.gz) | [См. список](../changelog-server.md#23-3) ||
|| **v23.2** | > | > | > ||
|| v.23.2.12 | 14.08.23 | [Бинарный файл](https://binaries.ydb.tech/release/23.2.12/ydbd-23.2.12-linux-amd64.tar.gz) | [См. список](../changelog-server.md#23-2) ||
|#

## Docker

#|
|| Версия |  Дата выпуска | Скачать | Список изменений ||
|| **v24.4** | > | > | > ||
|| v.24.4.4.12  | 03.06.25 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.4.4.12` | [См. список](../changelog-server.md#24-4-4-12) ||
|| v.24.4.4.2  | 15.04.25 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.4.4.2` | [См. список](../changelog-server.md#24-4-4-2) ||
|| **v24.3** | > | > | > ||
|| v.24.3.15.5  | 06.02.25 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.3.15.5` | [См. список](../changelog-server.md#24-3-15-5) ||
|| v.24.3.11.14  | 09.01.25 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.3.11.14` | [См. список](../changelog-server.md#24-3-11-14) ||
|| v.24.3.11.13  | 24.12.24 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.3.11.13` | [См. список](../changelog-server.md#24-3-11-13) ||
|| **v24.2** | > | > | > ||
|| v.24.2.7  | 20.08.24 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.2.7` | [См. список](../changelog-server.md#24-2) ||
|| **v24.1** | > | > | > ||
|| v.24.1.18 | 31.07.24 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.1.18` | [См. список](../changelog-server.md#24-1) ||
|| **v23.4** | > | > | > ||
|| v.23.4.11 | 14.05.24 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.4.11` | [См. список](../changelog-server.md#23-4) ||
|| **v23.3** | > | > | > ||
|| v.23.3.17 | 14.12.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.3.17` | — ||
|| v.23.3.13 | 12.10.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.3.13` | [См. список](../changelog-server.md#23-3) ||
|| **v23.2** | > | > | > ||
|| v.23.2.12 | 14.08.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.2.12` | [См. список](../changelog-server.md#23-2) ||
|#

## Исходный код

#|
|| Версия |  Дата выпуска | Ссылка | Список изменений ||
|| **v24.4** | > | > | > ||
|| v.24.4.4.12  | 03.06.25 | [https://github.com/ydb-platform/ydb/tree/24.4.4.12](https://github.com/ydb-platform/ydb/tree/24.4.4.12) | [См. список](../changelog-server.md#24-4-4-12) ||
|| v.24.4.4.2  | 15.04.25 | [https://github.com/ydb-platform/ydb/tree/24.4.4.2](https://github.com/ydb-platform/ydb/tree/24.4.4.2) | [См. список](../changelog-server.md#24-4-4-2) ||
|| **v24.3** | > | > | > ||
|| v.24.3.15.5  | 06.02.25 | [https://github.com/ydb-platform/ydb/tree/24.3.15.5](https://github.com/ydb-platform/ydb/tree/24.3.15.5) | [См. список](../changelog-server.md#24-3-15-5) ||
|| v.24.3.11.14  | 09.01.25 | [https://github.com/ydb-platform/ydb/tree/24.3.11.14](https://github.com/ydb-platform/ydb/tree/24.3.11.14) | [См. список](../changelog-server.md#24-3-11-14) ||
|| v.24.3.11.13  | 24.12.24 | [https://github.com/ydb-platform/ydb/tree/24.3.11.13](https://github.com/ydb-platform/ydb/tree/24.3.11.13) | [См. список](../changelog-server.md#24-3-11-13) ||
|| **v24.2** | > | > | > ||
|| v.24.2.7 | 20.08.24 | [https://github.com/ydb-platform/ydb/tree/24.2.7](https://github.com/ydb-platform/ydb/tree/24.2.7) | [См. список](../changelog-server.md#24-2) ||
|| **v24.1** | > | > | > ||
|| v.24.1.18 | 31.07.24 | [https://github.com/ydb-platform/ydb/tree/24.1.18](https://github.com/ydb-platform/ydb/tree/24.1.18) | [См. список](../changelog-server.md#24-1) ||
|| **v23.4** | > | > | > ||
|| v.23.4.11 | 14.05.24 | [https://github.com/ydb-platform/ydb/tree/23.4.11](https://github.com/ydb-platform/ydb/tree/23.4.11) | [См. список](../changelog-server.md#23-4) ||
|| **v23.3** | > | > | > ||
|| v.23.3.17 | 14.12.23 | [https://github.com/ydb-platform/ydb/tree/23.3.17](https://github.com/ydb-platform/ydb/tree/23.3.17) | — ||
|| v.23.3.13 | 12.10.23 | [https://github.com/ydb-platform/ydb/tree/23.3.13](https://github.com/ydb-platform/ydb/tree/23.3.13) | [См. список](../changelog-server.md#23-3) ||
|| **v23.2** | > | > | > ||
|| v.23.2.12 | 14.08.23 | [https://github.com/ydb-platform/ydb/tree/23.2.12](https://github.com/ydb-platform/ydb/tree/23.2.12) | [См. список](../changelog-server.md#23-2) ||
|#
