
Существует несколько вариантов указания того, какие именно исполняемые файлы {{ ydb-short-name }} вы хотите использовать для кластера:

- `ydb_version`: автоматически загрузить один из [официальных релизов {{ ydb-short-name }}](../../../../../downloads/index.md#ydb-server) по номеру версии. Например, `23.4.11`.
- `ydb_archive`: локальный путь файловой системы к архиву дистрибутива {{ ydb-short-name }}, [загруженному](../../../../../downloads/index.md#ydb-server) или иным образом подготовленному заранее.

Для использования [федеративных запросов](../../../../../concepts/query_execution/federated_query/index.md) может потребоваться установка [коннектора](../../../../../concepts/query_execution/federated_query/architecture.md#connectors). Плейбук может развернуть [fq-connector-go](../../../manual/federated-queries/connector-deployment.md#fq-connector-go) на хостах с динамическими узлами. Используйте следующие настройки:

- `ydb_install_fq_connector` — установите в `true` для установки коннектора.
- Выберите один из доступных вариантов развёртывания исполняемых файлов fq-connector-go:
  - `ydb_fq_connector_version`: автоматически загрузить один из [официальных релизов fq-connector-go](https://github.com/ydb-platform/fq-connector-go/releases) по номеру версии. Например, `v0.7.1`.
  - `ydb_fq_connector_git_version`: автоматически скомпилировать исполняемый файл fq-connector-go из исходного кода, загруженного из [официального репозитория GitHub](https://github.com/ydb-platform/fq-connector-go). Значение настройки — это имя ветки, тега или коммита. Например, `main`.
  - `ydb_fq_connector_archive`: локальный путь файловой системы к архиву дистрибутива fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или иным образом подготовленному заранее.
  - `ydb_fq_connector_binary`: локальные пути файловой системы к исполняемому файлу fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или иным образом подготовленному заранее.

- `ydb_tls_dir` — укажите локальный путь к папке с TLS-сертификатами, подготовленными заранее. Она должна содержать файл `ca.crt` и подкаталоги с именами, соответствующими именам хостов узлов, содержащие сертификаты для данного узла. Если не указано, самоподписанные TLS-сертификаты будут сгенерированы автоматически для всего кластера {{ ydb-short-name }}.
- `ydb_brokers` — перечислите FQDN узлов брокеров. Например:

  ```yaml
  ydb_brokers:
      - static-node-1.ydb-cluster.com
      - static-node-2.ydb-cluster.com
      - static-node-3.ydb-cluster.com
  ```

Оптимальное значение настройки `ydb_database_groups` в разделе `vars` зависит от доступных дисков. Предполагая только одну базу данных в кластере, используйте следующую логику:

- Для промышленных развёртываний используйте диски ёмкостью более 800 ГБ с высокой производительностью IOPS, затем выберите значение для этой настройки на основе топологии кластера:
  - Для `block-4-2` установите `ydb_database_groups` на 95% от общего количества дисков, округляя вниз.
  - Для `mirror-3-dc` установите `ydb_database_groups` на 84% от общего количества дисков, округляя вниз.
- Для тестирования {{ ydb-short-name }} на небольших дисках установите `ydb_database_groups` в 1 независимо от топологии кластера.

Значения переменных `system_timezone` и `system_ntp_servers` зависят от свойств инфраструктуры, на которой развёртывается кластер {{ ydb-short-name }}. По умолчанию `system_ntp_servers` включает набор NTP-серверов без учёта географического расположения инфраструктуры, на которой будет развёрнут кластер {{ ydb-short-name }}. Мы настоятельно рекомендуем использовать локальный NTP-сервер для on-premise инфраструктуры и следующие NTP-серверы для облачных провайдеров:

{% list tabs %}

- AWS

  - `system_timezone`: USA/<region_name>
  - `system_ntp_servers`: [169.254.169.123, time.aws.com] [Подробнее](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#configure-time-sync) о настройках NTP-серверов AWS.

- Azure

  - О том, как настраивается синхронизация времени на виртуальных машинах Azure, можно прочитать в [этой](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/time-sync) статье.

- Alibaba

  - Специфика подключения к NTP-серверам в Alibaba описана в [этой статье](https://www.alibabacloud.com/help/en/ecs/user-guide/alibaba-cloud-ntp-server).

- Yandex Cloud

  - `system_timezone`: Europe/Moscow
  - `system_ntp_servers`: [0.ru.pool.ntp.org, 1.ru.pool.ntp.org, ntp0.NL.net, ntp2.vniiftri.ru, ntp.ix.ru, ntps1-1.cs.tu-berlin.de] [Подробнее](https://yandex.cloud/ru/docs/tutorials/infrastructure-management/ntp) о настройках NTP-серверов Yandex Cloud.

{% endlist %}