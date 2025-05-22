# Сравнение конфигураций кластера {{ ydb-short-name }}: V1 и V2

В {{ ydb-short-name }} существует два основных подхода к управлению конфигурацией кластера: [V1](../configuration-management/configuration-v1/index.md) и [V2](../configuration-management/configuration-v2/index.md). Начиная с версии {{ ydb-short-name }} 25.1, поддерживается конфигурация V2, которая унифицирует управление кластерами {{ ydb-short-name }}, позволяет работать с конфигурацией полностью через [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md), а также автоматизирует наиболее сложные аспекты конфигурации (управление [статической группой](../../reference/configuration/index.md#blob_storage_config) и [State Storage](../../reference/configuration/index.md#domains-state)).

{% include [_](_includes/configuration-version-note.md) %}

В этой статье описываются ключевые различия между этими двумя подходами.

| Характеристика                 | Конфигурация V1                                  | Конфигурация V2                                     |
| ------------------------------ | ------------------------------------------------ | -------------------------------------------------- |
| **Структура конфигурации**     | Раздельная: [статическая](../../devops/configuration-management/configuration-v1/static-config.md) и [динамическая](../../devops/configuration-management/configuration-v1/dynamic-config.md). | [**Единая**](../configuration-management/configuration-v2/config-overview.md) конфигурация. |
| **Управление файлами**         | Статическая: ручное размещение файла на каждом узле.<br>Динамическая: централизованная загрузка через CLI. | Единая: [централизованная загрузка](../configuration-management/configuration-v2/update-config.md) через CLI, автоматическая доставка на все узлы. |
| **Механизм доставки и применения** | Статическая: читается и применяется из локального файла при запуске.<br>Динамическая: через [таблетку `Console`](../../concepts/glossary.md#console). | Полностью автоматически через механизм [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration). [Технические подробности](../../contributor/configuration-v2.md). |
| **Управление State Storage и статической группой** | **Ручное**: через обязательные секции [`domains_config`](../../reference/configuration/index.md#domains-state) и [`blob_storage_config`](../../reference/configuration/index.md#blob_storage_config) в статической конфигурации. | **Автоматическое**: управляется системой [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration). |
| **Рекомендуется для версий {{ ydb-short-name }}** | Все версии до 25.1.                             | Версия 25.1 и выше.                                |

## Конфигурация V1

[Конфигурация V1](../configuration-management/configuration-v1/index.md) кластера {{ ydb-short-name }} состоит из двух частей:

* **Статическая конфигурация:** управляет ключевыми параметрами узлов, включая конфигурацию [State Storage](../../reference/configuration/index.md#domains-state) и [статической группы](../../reference/configuration/index.md#blob_storage_config) (секции `domains_config` и `blob_storage_config` соответственно). Требует ручного размещения одного и того же файла конфигурации на каждом узле кластера. Путь до конфигурации указывается при запуске узла через опцию `--yaml-config`.
* **Динамическая конфигурация:** управляет остальными параметрами кластера. Загружается централизованно с помощью команды `ydb admin config replace` и распространяется на узлы базы данных.

Если ваш кластер работает на конфигурации V1, рекомендуется выполнить [миграцию на конфигурацию V2](migration/migration-to-v2.md).

## Конфигурация V2

Начиная с версии {{ ydb-short-name }} 25.1, поддерживается [конфигурация V2](../configuration-management/configuration-v2/config-overview.md). Ключевые особенности:

* **Единый конфигурационный файл:** вся конфигурация кластера хранится и управляется как единое целое.
* **Централизованное управление:** конфигурация загружается на кластер с помощью команды [`ydb admin cluster config replace`](../configuration-management/configuration-v2/update-config.md) и автоматически доставляется до всех узлов самим кластером {{ ydb-short-name }} через механизм [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration).
* **Ранняя валидация:** проверка корректности происходит ещё до доставки конфигурационного файла на узлы кластера, а не при рестарте серверных процессов.
* **Автоматическое управление State Storage и статической группой:** V2 поддерживает [автоматическую конфигурацию](../configuration-management/configuration-v2/config-overview.md), что позволяет не указывать эти секции в конфигурационном файле вручную.
* **Хранение на узлах:** актуальная конфигурация автоматически сохраняется каждым узлом в специальной директории (указывается опцией `--config-dir` при запуске `ydbd`) и используется при последующих перезапусках.


Использование конфигурации V2 является рекомендуемым для всех кластеров {{ ydb-short-name }} версии 25.1 и выше.
