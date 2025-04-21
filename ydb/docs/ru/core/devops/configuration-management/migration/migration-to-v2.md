# Миграция на конфигурацию V2

Данный документ содержит инструкцию по миграции с [конфигурации V1](../../configuration-management/configuration-v2/config-overview.md) на [конфигурацию V2](../../configuration-management/configuration-v2/config-overview.md).

В конфигурации V1 существует два различных механизма применения конфигурационных файлов:

- [статическая конфигурация](../../configuration-management/configuration-v2/config-overview.md#static-config) управляет [узлами хранения](../../../concepts/glossary.md#storage-node) кластера {{ ydb-short-name }} и требует ручного размещения файлов на каждом узле кластера;
- [динамическая конфигурация](../../configuration-management/configuration-v2/config-overview.md#dynamic-config) управляет [узлами базы данных](../../../concepts/glossary.md#database-node) кластера {{ ydb-short-name }} и загружается в кластер централизованно, с помощью команд {{ ydb-short-name }} CLI.

В конфигурации V2 этот процесс унифицирован: единый конфигурационный файл загружается в систему через команды {{ ydb-short-name }} CLI, автоматически доставляясь на все узлы кластера.

Компоненты [State Storage](../../../concepts/glossary.md#state-storage) и [статической группы](../../../concepts/glossary.md#static-group) кластера {{ ydb-short-name }} являются ключевыми для корректной работы кластера. При работе с конфигурацией V1 данные компоненты настраиваются вручную, через задание секций `domains_config` и `blob_storage_config` в конфигурационном файле.
В конфигурации V2 возможна [автоматическая конфигурация](../../configuration-management/configuration-v2/config-overview.md) этих компонентов, без указания соответствующих секций в конфигурационном файле.

## Исходное состояние

Миграция на конфигурацию V2 может быть осуществлена в случае выполнения следующих условий:

1. Кластер {{ydb-short-name}} [обновлен](../../deployment-options/manual/update-executable.md) до версии 25.1 и выше.
1. Кластер {{ydb-short-name}} сконфигурирован с файлом [конфигурации V1](../../configuration-management/configuration-v2/config-overview.md#static-config) `config.yaml`, расположенным в файловой системе узлов и подключенным через аргумент `ydbd --yaml-config`.
1. В конфигурационном файле кластера заданы разделы `domains_config` и `blob_storage_config` для настройки State Storage и статической группы соответственно.

## Проверка текущей версии конфигурации

Перед началом миграции убедитесь, что ваш кластер работает на конфигурации V1. Узнать текущую версию конфигурации на узлах можно несколькими способами, описанными в статье [Проверка версии конфигурации](../configuration-management/check-config-version.md).

Продолжать выполнение данной инструкции следует только в том случае, если узлы работают на версии конфигурации V1. Если на всех узлах уже включена версия V2, миграция не требуется.

## Инструкция по миграции на конфигурацию V2

Для того чтобы перевести кластер {{ ydb-short-name }} на конфигурацию V2, необходимо проделать следующие шаги:

1. Проверить наличие файла [динамической конфигурации](../../configuration-management/configuration-v2/config-overview.md#dynamic-config) в кластере. Для этого необходимо выполнить команду:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

    В случае отсутствия такой конфигурации в кластере, команда выдаст сообщение:

    ```bash
    No config returned.
    ```

    Если файл найден, следует использовать его и пропустить следующий шаг данной инструккции.

1. (В случае отсутствия файла динамической конфигурации в кластере) Выполнить команду генерации файла динамической конфигурации. Файл будет сгенерирован на основе файла статической конфигурации, лежащего на узлах кластера:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config generate > config.yaml
    ```

1. Добавить в полученный на шаге 1 или 2 файл `config.yaml` следующее поле:

    ```yaml
    feature_flags:
        ...
        switch_to_config_v2: true
    ```

    {% cut "Подробнее" }

    Включение данного флага означает, что за хранение конфигурации и операции над ней теперь отвечает таблетка BSController, а не таблетка [Console](../../../concepts/glossary.md#console). Это переключает основной механизм управления конфигурацией кластера.

    {% endcut %}

1. Разместить файл `config.yaml` на все узлы кластера, заместив им предыдущий файл конфигурации.

1. Создать директорию для работы узла {{ ydb-short-name }} с конфигурацией на каждом из узлов. В случае запуска нескольких узлов кластера на одном хосте создайте отдельные директории под каждый узел. Инициализируйте директорию, выполнив команду на каждом из узлов. В параметре `--from-config` укажите путь к файлу `config.yaml`, размещенному на узлах ранее.

    ```bash
    sudo mkdir -p /opt/ydb/config-dir
    sudo chown -R ydb:ydb /opt/ydb/config-dir
    ydb admin node config init --config-dir /opt/ydb/config-dir --from-config /opt/ydb/cfg/config.yaml
    ```

    {% cut "Подробнее" %}

    В дальнейшнем система самостоятельно будет сохранять актуальную конфигурацию в указанные директории.

    {% endcut %}

1. Перезапустить все узлы кластера с помощью процедуры [rolling-restart](../../../maintenance/manual/node_restarting.md), добавив опцию `ydbd --config-dir` при запуске узла с указанием пути до директории, а также убрав опцию `ydbd --yaml-config`.

    {% list tabs group=manual-systemd %}

    - Вручную

        При ручном запуске добавьте опцию `--config-dir`, к команде `ydbd server`, не указывая опцию `--yaml-config`:

        ```bash
        ydbd server --config-dir /opt/ydb/config-dir
        ```

    - С использованием systemd

        При использовании systemd добавьте опцию `--config-dir` к команде `ydbd server` в конфигурационный файл systemd, а также избавьтесь от опции `--yaml-config`:

        ```ini
        ExecStart=/opt/ydb/bin/ydbd server --config-dir /opt/ydb/config-dir
        ```

        После обновления файла systemd выполните следующую команду, чтобы применить изменения:

        ```bash
        sudo systemctl daemon-reload
        ```

    {% endlist %}

1. Загрузить полученный ранее конфигурационный файл `config.yaml` в систему с помощью команды YDB CLI:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 cluster config replace -f config.yaml
    ```

    Команда запросит подтверждение на выполнение операции `This command may damage your cluster, do you want to continue? [y/N]`, в ответ на этот запрос необходимо согласиться и ввести `y`.

    {% cut "Подробнее" %}

    После выполнения команды конфигурационный файл загрузится во внутреннее хранилище таблетки BSController и сохранится в директориях, указанных в опции `--config-dir` на каждом узле. C этого момента любое изменение конфигурации на существующих узлах выполняется с помощью [специальных команд](../configuration-v2/update-config.md) YDB CLI. Также при запуске узла актульная конфигурация будет автоматически загружаться из конфигурационной директории.

    {% endcut %}

1. Получить текущую конфигурацию кластера:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

    Файл `config.yaml` должен совпадать с конфигурационными файлами, разложенными по узлам кластера, за исключением поля `metadata.version`, которое должно быть больше на единицу по сравнению с версией на узлах кластера.

1. Добавить в `config.yaml` в разделе `config` следующий блок:

    ```yaml
    self_management_config:
      enabled: true
    ```

    {% cut "Подробнее" %}

    Данная секция отвечает за включение механизма [распределенной конфигурации](../../../concepts/glossary.md#distributed-configuration) в кластере. Хранение конфигурации и любые операции над ней будут осуществляться через данный механизм.

    {% endcut %}

1. Загрузить обновленный конфигурационный файл в кластер:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 cluster config replace -f config.yaml
    ```

1. Перезапустить все [узлы хранения](../../../concepts/glossary.md#storage-node) кластера с помощью процедуры [rolling restart](../../../reference/ydbops/rolling-restart-scenario.md).

1. При наличии секции `config.domains_config.security_config` в файле `config.yaml`, вынести её на уровень выше, в секцию `config`.

1. Удалить из файла `config.yaml` секции `config.blob_storage_config` и `config.domains_config`.

1. Загрузить обновленный конфигурационный файл в кластер:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 cluster config replace -f config.yaml
    ```

    {% cut "Подробнее" %}

    После загрузки конфигурации кластер {{ ydb-short-name }} будет переведён в режим автоматического управление конфигурацией [State Storage](../../../reference/configuration/index.md#domains-state) и [статической группой](../../../reference/configuration/index.md#blob_storage_config) с помощью механизма распределенной конфигурации.

    {% endcut %}

Убедиться в успешном завершении миграции можно, проверив версию конфигурации на узлах кластера одним из способов, описанных в статье [Проверка версии конфигурации](../configuration-management/check-config-version.md). На всех узлах кластера `Configuration version` должна быть `v2`.

## Результат

В результате проделанных действий кластер будет переведен на режим конфигурации V2. Управление единой конфигурацией осуществляется с помощью [специальных команд](../configuration-v2/update-config.md) YDB CLI, статическая группа и State Storage управляются системой автоматически.
