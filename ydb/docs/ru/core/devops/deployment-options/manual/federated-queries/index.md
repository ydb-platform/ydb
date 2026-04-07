# Развёртывание {{ ydb-short-name }} с функцией Federated Query

{% note warning %}

Данная функциональность находится в режиме "Experimental".

{% endnote %}

## Общая схема инсталляции{#general-scheme}

{{ ydb-full-name }} может выполнять [федеративные запросы](../../../../concepts/query_execution/federated_query/index.md) ко внешним источникам (например, объектным хранилищам или реляционным СУБД) без необходимости перемещения их данных непосредственно в {{ ydb-short-name }}. В данном разделе мы рассмотрим изменения, которые необходимо внести в конфигурацию {{ ydb-short-name }} и окружающую инфраструктуру для включения функциональности федеративных запросов.

{% note info %}

Для организации доступа к некоторым из источников данных требуется развёртывание специального микросервиса - [коннектора](../../../../concepts/query_execution/federated_query/architecture.md#connectors). Ознакомьтесь c [перечнем поддерживаемых источников](../../../../concepts/query_execution/federated_query/architecture.md#supported-datasources), чтобы понять, требуется ли вам установка коннектора.

{% endnote %}

Кластер {{ ydb-short-name }} и внешние источники данных в варианте production-инсталляции должны развёртываться на разных физических или виртуальных серверах, в том числе в облаках. Если для доступа к определённому источнику требуется развёртывание коннектора, это необходимо сделать на тех же серверах, на которых развёрнуты динамические узлы {{ ydb-short-name }}. Иными словами, на каждый процесс `ydbd`, работающий в режиме динамического узла, должен приходиться один локальный процесс коннектора.

При этом должны выполняться следующие требования:

* внешний источник данных должен быть доступен по сети для запросов со стороны {{ ydb-short-name }} или со стороны коннектора (при его наличии);
* коннектор должен быть доступен по сети для запросов со стороны {{ ydb-short-name }} (что достигается тривиальным образом благодаря работе этих процессов на одном и том же хосте).

![Инсталляция {{ ydb-short-name }} FQ](_images/ydb_fq_onprem.png "Инсталляция {{ ydb-short-name }} FQ" =1024x)

{% note info %}

В настоящее время мы не поддерживаем развёртывание коннектора в {{k8s}}, но планируем добавить её в ближайшем будущем.

{% endnote %}

## Пошаговое руководство

1. Выполните шаги инструкции по развёртыванию динамического узла {{ ydb-short-name }} до [подготовки конфигурационных файлов](../initial-deployment/index.md#config) включительно.
2. Если для доступа к нужному вам источнику требуется развернуть коннектор, сделайте это [согласно инструкции](./connector-deployment.md).
3. [В конфигурационном файле](../../../../reference/configuration/index.md) {{ ydb-short-name }} в секции `feature_flags` включите флаг `enable_external_data_sources`:

```yaml
feature_flags:
  enable_external_data_sources: true
```

4. [В конфигурационный файл](../../../../reference/configuration/index.md) {{ ydb-short-name }} добавьте [настройки внешних источников данных](../../../../reference/configuration/query_service_config.md).

{% list tabs %}

- Без использования коннектора

    ```yaml
    query_service_config:
      generic:
        default_settings:
        - name: UsePredicatePushdown
          value: "true"
      all_external_data_sources_are_available: false
      available_external_data_sources:
      - ObjectStorage
    ```

- С использованием коннектора

    ```yaml
    query_service_config:
      generic:
        connector:
          endpoint:
            host: localhost                   # имя хоста, где развернут коннектор
            port: 2130                        # номер порта коннектора
          use_ssl: false                      # флаг, включающий шифрование соединений
          ssl_ca_crt: "/opt/ydb/certs/ca.crt" # путь к сертификату CA
        default_settings:
        - name: UsePredicatePushdown
          value: "true"
      all_external_data_sources_are_available: false
      available_external_data_sources:
      - ClickHouse
      - MySQL
    ```

{% endlist %}

5. Продолжайте развёртывание динамического узла {{ ydb-short-name }} по [инструкции](../initial-deployment/index.md).
