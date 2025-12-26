# Миграция на конфигурацию V1

Данный документ содержит инструкцию по миграции с [конфигурации V2](../../configuration-management/configuration-v2/config-overview.md) на [конфигурацию V1](../../configuration-management/configuration-v1/index.md).

{% note info %}

Данная инструкция предназначена для аварийных ситуаций, когда после [перехода на конфигурацию V2](./migration-to-v2.md) возникли непредвиденные проблемы и требуется откат на конфигурацию V1, например, для последующего отката на версию {{ ydb-full-name }} ниже v25-1. В штатном режиме работы эта процедура не требуется.

{% endnote %}

## Исходное состояние

Миграция на конфигурацию V1 возможна только в том случае, если в кластере используется [конфигурация V2](../../configuration-management/configuration-v2/config-overview.md). Это может быть достигнуто:

- в результате [миграции на конфигурацию V2](migration-to-v2.md);
- при [первоначальном развёртывании](../../deployment-options/manual/initial-deployment/index.md) кластера.

Узнать текущую версию конфигурации на узлах можно несколькими способами, описанными в статье [Проверка версии конфигурации](../check-config-version.md). Перед началом миграции убедитесь, что кластер работает на конфигурации V2.

## Инструкция по миграции на конфигурацию V1

Для того чтобы перевести кластер {{ ydb-short-name }} на конфигурацию V1, необходимо проделать следующие шаги:

1. Получить текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch --v2-internal-state > config.yaml
    ```

    {% cut "Подробнее" %}

    Аргумент `--v2-internal-state` указывает, что будет получена полная конфигурация кластера, включая параметры настройки [State Storage](../../../reference/configuration/index.md#domains-state) и [статической группы](../../../reference/configuration/index.md#blob_storage_config).

    {% endcut %}

2. Изменить конфигурационный файл `config.yaml`, поменяв значение параметра `self_management_config.enabled` с `true` на `false`:

    ```yaml
    self_management_config:
      enabled: false
    ```

    {% cut "Подробнее" %}

    Данная секция отвечает за управление механизмом [распределённой конфигурации](../../../concepts/glossary.md#distributed-configuration). Установка значения `enabled: false` отключает этот механизм. Далее управление конфигурацией State Storage и статической группы будет осуществляться вручную через секции `domains_config` и `blob_storage_config` соответственно в конфигурационном файле (эти секции были получены на предыдущем шаге при использовании флага `--full`).

    {% endcut %}

3. Загрузить обновлённый конфигурационный файл в кластер с помощью [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config replace -f config.yaml
    ```

4. Перезапустить все узлы кластера с помощью процедуры [rolling-restart](../../../maintenance/manual/node_restarting.md).

    {% cut "Подробнее" %}

    После перезапуска узлов кластер будет переведён в режим ручного управления State Storage и статической группой, но всё ещё будет использовать единый конфигурационный файл, доставляемый через таблетку BSController. Конфигурация узлов при запуске всё ещё будет читаться из директории, указанной в опции `ydbd --config-dir`, и там же сохраняться.

    {% endcut %}

5. Получить текущую конфигурацию кластера с помощью `ydb admin cluster config fetch`:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

    {% cut "Подробнее" %}

    В полученной конфигурации будут отсутствовать секции `domains_config` и `blob_storage_config`, так как они управляются вручную и не должны быть частью динамической конфигурации.

    {% endcut %}

6. Разместить полученный файл `config.yaml` (это будет ваша статическая конфигурация V1) в файловую систему каждого узла кластера.

7. Перезапустить все узлы кластера с помощью процедуры [rolling-restart](../../../maintenance/manual/node_restarting.md), указав путь к статическому конфигурационному файлу через опцию `ydbd --yaml-config` и убрав опцию `ydbd --config-dir`:

    {% list tabs group=manual-systemd %}

    - Вручную

        При ручном запуске добавьте опцию `--yaml-config` к команде `ydbd server`, не указывая опцию `--config-dir`:

        ```bash
        ydbd server --yaml-config /opt/ydb/cfg/config.yaml
        ```

    - С использованием systemd

        При использовании systemd добавьте опцию `--yaml-config` к команде `ydbd server` в конфигурационный файл systemd, а также удалите опцию `--config-dir`:

        ```ini
        ExecStart=/opt/ydb/bin/ydbd server --yaml-config /opt/ydb/config/config.yaml
        ```

        После обновления файла systemd выполните следующую команду, чтобы применить изменения:

        ```bash
        sudo systemctl daemon-reload
        ```

    {% endlist %}

Убедиться в успешном завершении миграции можно, проверив версию конфигурации на узлах кластера одним из способов, описанных в статье [{#T}](../check-config-version.md). На всех узлах кластера должна использоваться конфигурация `v1`.

## Результат

В результате проделанных действий кластер будет переведён в режим конфигурации V1. Конфигурация состоит из двух частей: статической и динамической, управление статической группой и State Storage осуществляется вручную.
