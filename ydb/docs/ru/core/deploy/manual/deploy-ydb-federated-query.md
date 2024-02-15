# Развертывание YDB с функцией Federated Query

{{ ydb-full-name }} может выполнять [федеративные запросы](../../concepts/federated_query/index.md) ко внешним источникам (например, объектным хранилищам или реляционным СУБД) без необходимости перемещения их данных непосредственно в {{ ydb-short-name }}. В данном разделе мы рассмотрим изменения, которые необходимо внести в конфигурацию {{ ydb-short-name }} и окружающую инфраструктуру для включения функциональности федеративных запросов. 

{% note info %}

Для организации доступа к некоторым из источников данных требуется развертывание специального микросервиса - [коннектора](../../concepts/federated_query/architecture.md#connectors). Ознакомьтесь c [перечнем поддерживаемых источников](../../concepts/federated_query/architecture.md#suppored-datasources), чтобы понять, требуется ли вам установка коннектора.

{% endnote %}

Мы будем исходить из того, что процесс {{ ydb-short-name }}, опционально разворачиваемый коннектор и внешний источник данных размещены на разных физических или виртуальных серверах (в том числе в контейнерах), при этом:
* коннектор должен быть доступен по сети для запросов со стороны {{ ydb-short-name }};
* источник данных должен быть доступен по сети для запросов со стороны коннектора (сам источник может быть развернут в режиме on-prem на мощностях клиента или находиться в публичном облаке).

![Инсталляция {{ ydb-short-name }} FQ](_images/ydb_fq_onprem.png "Инсталляция {{ ydb-short-name }} FQ" =640x)

1. Если для доступа к нужному вам источнику требуется развернуть коннектор, сделайте это [согласно инструкции](./connector.md).
1. Выполните шаги инструкции по развертыванию {{ ydb-short-name }} до [подготовки конфигурационных файлов](./deploy-ydb-on-premises.md#config) включительно.
1. В конфигурационном файле {{ ydb-short-name }} в секции `query_service_config` добавьте подсекцию `generic` по приведённому ниже образцу. В полях `connector.endpoint.host` и `connector.endpoint.port` укажите сетевой адрес коннектора, а в поле `connector.use_ssl` укажите, использует ли коннектор шифрованные соединения:
    ```yaml
    query_service_config:
        generic:
            connector:
                endpoint:
                    host: connector_host  # имя хоста, где развёрнут коннектор
                    port: 50051           # номер порта, на котором развёрнут слушающий сокет коннектора
                use_ssl: true             # флаг, включающий шифрование соединений
            default_settings:
                - name: DateTimeFormat
                  value: string
                - name: UseNativeProtocolForClickHouse
                  value: "true"
                - name: UsePredicatePushdown
                  value: "true"
    ```
1. В конфигурационном файле {{ ydb-short-name }} добавьте секцию `feature_flags` следующего содержания:
    ```yaml
    feature_flags:
        enable_external_data_sources: true
        enable_script_execution_operations: true
    ```
1. Продолжайте развертывание {{ ydb-short-name }} по [инструкции](./deploy-ydb-federated-query.md). 
