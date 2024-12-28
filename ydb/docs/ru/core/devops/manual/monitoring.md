# Настройка мониторинга кластера {{ ydb-short-name }}

На этой странице рассказано, как настроить мониторинг кластера {{ ydb-short-name }}.

{{ ydb-short-name }} предоставляет множество сенсоров состояния системы. Мгновенные значения сенсоров можно посмотреть в веб-интерфейсе:

```text
http://<ydb-server-address>:<ydb-port>/counters/
```

где:

- `<ydb-server-address>` – адрес сервера {{ ydb-short-name }}.

  Для локального однонодового кластера YDB, запущенного с помощью инструкции по [Быстрому началу работы](../../quickstart.md), используйте адрес `localhost`.

- `<ydb-port>` – порт {{ ydb-short-name }}. Значение по умолчанию: 8765.

Связанные сенсоры объединены в подгруппы (например `counters auth`). Чтобы посмотреть значения сенсоров только определенной подгруппы, перейдите по URL следующего вида:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/
```

- `<servicename>` — имя подгруппы сенсоров.

Например, данные об утилизации аппаратных ресурсов сервера доступны по следующему URL:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=utils
```

Для сбора значений метрик вы можете использовать популярный инструмент с открытым исходным кодом [Prometheus](https://prometheus.io/). Значения сенсоров {{ ydb-short-name }} в [формате Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) доступны по URL следующего вида:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus
```

- `<servicename>` — имя подгруппы сенсоров.

Визуализировать данные можно с помощью любой системы, которая поддерживает формат Prometheus, например [Zabbix](https://www.zabbix.com/ru/), [Amazon CloudWatch](https://aws.amazon.com/ru/cloudwatch/) или [Grafana](https://grafana.com/):

![grafana-actors](../../_assets/grafana-actors.png)

## Настройка мониторинга с помощью Prometheus и Grafana {#prometheus-grafana}

Чтобы настроить мониторинг кластера {{ ydb-short-name }} с помощью [Prometheus](https://prometheus.io/) и [Grafana](https://grafana.com/):

1. [Установите и запустите](https://prometheus.io/docs/prometheus/latest/getting_started/#downloading-and-running-prometheus) Prometheus, используя [файл конфигурации](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_ydb_prometheus.yml).

    Перед использованием, файл конфигурации Prometheus необходимо отредактировать:

    1. В секции `targets` укажите адреса всех серверов кластера {{ ydb-short-name }} и порты каждого из узлов (статического и динамического), работающих на сервере.

        Например, для кластера из трёх серверов, где на каждом сервере работает один статический и два динамических узла, необходимо прописать девять адресов для всех подгрупп сенсоров, кроме дисковых (для дисковых подгрупп сенсоров необходимо указать только адреса статических узлов):

        ```json
        static_configs:
        - targets:
          - ydb-s1.example.com:8765
          - ydb-s1.example.com:8766
          - ydb-s1.example.com:8767
          - ydb-s2.example.com:8765
          - ydb-s2.example.com:8766
          - ydb-s2.example.com:8767
          - ydb-s3.example.com:8765
          - ydb-s3.example.com:8766
          - ydb-s3.example.com:8767
        ```

        Для локального однонодового кластера YDB, в секции `targets` прописывается один адрес:

        ```json
        - targets: ["localhost:8765"]
        ```

    1. При необходимости, укажите TLS сертификат в секции `tls_config`:

       ```json
       tls_config:
           ca_file: '<ydb-prometheus-ca-file>'
       ```

       Пропустите этот шаг для локального однонодового кластера {{ ydb-short-name }}.

1. [Установите и запустите](https://grafana.com/docs/grafana/latest/getting-started/getting-started/) Grafana.
1. [Создайте](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source) источник данных с типом `prometheus` в Grafana и подсоедините его к запущенному экземпляру Prometheus.
1. Загрузите [дашборды {{ ydb-short-name }}](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/helm/ydb-prometheus/dashboards) в Grafana.

    Вы можете загрузить дашборды с помощью инструмента [Import](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard) Grafana UI или выполнить [скрипт](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_upload_dashboards.sh). Обратите внимание, что скрипт использует [базовую аутентификацию](https://grafana.com/docs/grafana/latest/http_api/create-api-tokens-for-org/#authentication) в Grafana. Для других случаев модифицируйте скрипт.

    Ознакомьтесь со [справочником по дашбордам Grafana](../../reference/observability/metrics/grafana-dashboards.md).
