# Настройка мониторинга кластера {{ ydb-short-name }}

Страница входит в раздел [Обзор наблюдаемости](index.md) и описывает настройку сбора метрик кластера {{ ydb-short-name }} в [Prometheus](https://prometheus.io/) и визуализацию в [Grafana](https://grafana.com/). На каждом узле метрики также доступны во встроенном HTTP-интерфейсе мониторинга — раздел [Доступ к метрикам через веб-интерфейс](#web-metrics).

{% note tip %}

Перед началом работы ознакомьтесь с [описанием метрик](../../reference/observability/metrics/index.md) и [справочником по дашбордам Grafana](../../reference/observability/metrics/grafana-dashboards.md).

Определите, как развернут кластер {{ ydb-short-name }}:

- по [одному из способов развертывания кластера](../deployment-options/index.md) (Ansible, Kubernetes или вручную);
- либо в конфигурации [быстрого старта](../../quickstart.md) (одноузловой локальный кластер).

{% endnote %}

## Настройка мониторинга с помощью Prometheus и Grafana {#prometheus-grafana}

### Подготовка к установке {#installation-preparation}

- [Установите](https://prometheus.io/docs/prometheus/latest/getting_started) Prometheus.
- [Установите](https://grafana.com/docs/grafana/latest/setup-grafana/) Grafana.

Полный цикл развертывания Prometheus и Grafana в этом разделе не рассматривается — обратитесь к документации выбранных продуктов.

### Варианты подготовки конфигурации сбора метрик {#prometheus-config-variants}

Конфигурацию сбора метрик с узлов {{ ydb-short-name }} можно подготовить одним из способов ниже. Во всех случаях потребуется файл `prometheus_ydb.yml` — в нем описан сбор метрик {{ ydb-short-name }}.

Список [узлов хранения](../../concepts/glossary.md#storage-node) и [динамических узлов](../../concepts/glossary.md#dynamic) базы данных указывается в файлах `ydbd-storage.yml` и `ydbd-database.yml` соответственно. Пути к этим файлам задаются в `prometheus_ydb.yml` (подробнее — в разделе [Запуск Prometheus с подготовленной конфигурацией](#prometheus-start)).

{% list tabs %}

- Ansible (с TLS)

  Рекомендуемый способ получить согласованный набор файлов — сгенерировать конфигурацию сбора метрик с использованием плейбука `generate_promconf`.

  Перейдите в рабочий каталог сценария Ansible для вашего кластера:

  ```bash
  cd <path_to_ansible_project>
  ```

  Запустите плейбук генерации конфигурации для Prometheus:

  ```bash
  ansible-playbook ydb_platform.ydb.generate_promconf
  ```

  Плейбук создаст каталог `promconf` со следующим содержимым:

  - `prometheus_ydb.yml` — файл конфигурации Prometheus.
  - `ydbd-storage.yml` — список узлов хранения кластера.
  - `ydbd-database.yml` — список динамических узлов базы данных.
  - `ca.crt` — сертификат, использованный при развертывании кластера.
  - `grafana-dashboards` — каталог с шаблонами дашбордов для Grafana. Шаблоны загружаются из [репозитория GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/helm/ydb-prometheus/dashboards).

  Проверьте содержимое сгенерированных файлов `ydbd-storage.yml` и `ydbd-database.yml`. Список узлов и портов должен совпадать с фактической топологией кластера, в том числе с узлами, отображаемыми во [встроенном UI кластера](../../reference/embedded-ui/ydb-monitoring.md).

  Пример проверки содержимого каталога с конфигурацией:

  ```bash
  cd promconf
  ls -la
  ```

  Пример вывода:

  ```text
  -rw-rw-r-- 1 1818 ca.crt
  drwxrwxr-x 2 4096 grafana-dashboards
  -rw-rw-r-- 1 17532 prometheus_ydb.yml
  -rw-rw-r-- 1 165 ydbd-database.yml
  -rw-rw-r-- 1 164 ydbd-storage.yml
  ```

  Проверка доступности узлов — по HTTPS; см. [Метрики в формате Prometheus](#web-metrics-prometheus).

- Вручную без TLS

  Скопируйте файлы из каталога [ydb/deploy/prometheus](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus) репозитория {{ ydb-short-name }}.

  Заполните секции `targets` в [`ydbd-storage.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-storage.yml) и [`ydbd-database.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-database.yml): укажите хосты и порты мониторинга (`--mon-port`) всех узлов хранения и динамических узлов баз данных, с которых нужно собирать метрики (как определить порт, см. [Как определить порт мониторинга](#web-metrics-mon-port)).

  В [`prometheus_ydb.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/prometheus_ydb.yml) для задач сбора метрик задайте `scheme: http` и отключите или удалите параметры `tls_config`.

  Проверка доступности узлов — по HTTP; см. [Метрики в формате Prometheus](#web-metrics-prometheus).

- Вручную с TLS

  Скопируйте файлы из каталога [ydb/deploy/prometheus](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus) репозитория {{ ydb-short-name }}.

  Заполните секции `targets` в [`ydbd-storage.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-storage.yml) и [`ydbd-database.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-database.yml): укажите хосты и порты мониторинга (`--mon-port`) всех узлов хранения и динамических узлов баз данных, с которых нужно собирать метрики (как определить порт, см. [Как определить порт мониторинга](#web-metrics-mon-port)).

  В [`prometheus_ydb.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/prometheus_ydb.yml) для задач сбора метрик задайте `scheme: https` и настройте `tls_config`. Укажите путь к [сертификату центра сертификации](../deployment-options/manual/initial-deployment/deployment-preparation.md#tls-certificates) (CA), которым подписаны сертификаты TLS кластера:

  ```yaml
  scheme: https
  tls_config:
      ca_file: '<ydb-ca-file>'
  ```

  Убедитесь, что все пути в `tls_config` указывают на существующие файлы и что пользователь, под которым запущен Prometheus, имеет права на их чтение.

  Если в вашей конфигурации используются клиентский сертификат и ключ, добавьте их в `tls_config`:

  ```yaml
  tls_config:
      ca_file: '<ydb-ca-file>'
      cert_file: '<ydb-client-cert-file>'
      key_file: '<ydb-client-key-file>'
  ```

  Проверка доступности узлов — по HTTPS (`curl` с `https://`, см. [Метрики в формате Prometheus](#web-metrics-prometheus)). Укажите в `curl` флаг `--cacert` с тем же путём, что в `ca_file`.

- Локальный однонодовый кластер (быстрый старт)

  Если {{ ydb-short-name }} запущен локально в конфигурации «один узел» и мониторинг слушает на одном порту (часто `8765`), в обоих файлах — `ydbd-storage.yml` и `ydbd-database.yml` — в секции `targets` укажите один и тот же адрес, например `localhost:8765` или `<hostname>:8765`.

{% endlist %}

Независимо от выбранного способа подготовки конфигурации шаги запуска и проверки Prometheus ниже одинаковы.

### Запуск Prometheus с подготовленной конфигурацией {#prometheus-start}

Отредактированные файлы положите в любую удобную директорию на машине, где запускается Prometheus (рядом с бинарником или отдельно, например `/etc/prometheus`). Файлы `prometheus_ydb.yml`, `ydbd-storage.yml` и `ydbd-database.yml` расположите в одной папке.

В `prometheus_ydb.yml` в каждой задаче секции `scrape_configs` параметр `file_sd_configs` указывает, из каких файлов брать список целей — `ydbd-storage.yml` и `ydbd-database.yml`. По умолчанию пути в `file_sd_configs` относительные: Prometheus ищет эти файлы относительно рабочей директории процесса при старте. Если задать абсолютные пути, запускать Prometheus можно из любой рабочей директории.

Если Prometheus поднимается только для {{ ydb-short-name }}, в параметре `--config.file` укажите полный или относительный путь к `prometheus_ydb.yml`. Перед запуском перейдите в каталог с конфигурацией и запустите процесс из него:

```bash
cd <path_to_config_dir>
prometheus --config.file=prometheus_ydb.yml
```

Если Prometheus уже используется для других систем, не подменяйте основной конфиг файлом `prometheus_ydb.yml` в `--config.file`. Добавьте в существующий файл конфигурации (обычно `prometheus.yml`) задачи из секции `scrape_configs` в `prometheus_ydb.yml` — все записи с `job_name`, начинающимся с `ydb/`.

Убедитесь, что `ydbd-storage.yml` и `ydbd-database.yml` доступны по путям в `file_sd_configs` перенесенных задач (см. абзац про `file_sd_configs` [выше](#prometheus-start)).

Секцию `global:` из `prometheus_ydb.yml` переносите только при необходимости и сверьте значения с уже заданными в вашем конфиге. Запускайте Prometheus с вашим конфигом, например:

```bash
prometheus --config.file=<your_prometheus.yml>
```

После изменений проверьте конфигурацию:

```bash
promtool check config <your_prometheus.yml>
```

Проверьте, что Prometheus запущен и отвечает:

```bash
curl "http://localhost:9090/-/healthy"
```

В веб-интерфейсе Prometheus (как правило, порт `9090`) откройте **Status** → **Targets** и убедитесь, что группы опроса метрик находятся в состоянии **UP** (успешный сбор). Исключение — группа, связанная с топиками: при отсутствии топиков в базе данных для нее может отображаться ответ `204 No content`. Это не признак ошибки конфигурации.

### Настройка Grafana {#grafana-setup}

#### Подключение Prometheus как источника данных {#grafana-data-source}

1. Откройте веб-интерфейс Grafana.
1. Перейдите в **Connections** → **Data sources** → **Add data source**.
1. Выберите тип **Prometheus**.
1. В поле **Name** укажите произвольное имя источника данных, например `ydb`.
1. В поле **Prometheus server URL** укажите URL экземпляра Prometheus, в котором уже настроен сбор метрик с кластера {{ ydb-short-name }} (например, `http://localhost:9090`, если Grafana и Prometheus на одной машине и Prometheus слушает стандартный порт).
1. При необходимости заполните поля аутентификации, TLS и таймаутов в соответствии с политикой вашей инсталляции.
1. Нажмите **Save & test** в нижней части экрана. При корректной настройке отобразится сообщение об успешном запросе к API Prometheus (например, `Successfully queried the Prometheus API`).

Дополнительно см. [инструкцию Prometheus по созданию источника данных в Grafana](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source).

#### Импорт дашбордов {#grafana-dashboards-import}

Готовые дашборды {{ ydb-short-name }} находятся в [репозитории](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/helm/ydb-prometheus/dashboards). Если вы использовали плейбук Ansible для генерации конфигурации Prometheus, шаблоны дашбордов будут размещены в подкаталоге `grafana-dashboards`. Импортируйте JSON-файлы в Grafana через веб-интерфейс или [provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).

Состав панелей и рекомендации по использованию дашбордов приведены в [справочнике по дашбордам Grafana](../../reference/observability/metrics/grafana-dashboards.md).

### Результат {#result}

{% cut "Пример дашборда в Grafana" %}

После импорта откройте дашборд **YDB Essential Metrics**. В верхней части выберите источник данных Prometheus (как в [подключении](#grafana-data-source)) и имя базы данных.

![пример дашборда YDB Essential Metrics в Grafana](../../_assets/grafana.png)

{% endcut %}

После настройки убедитесь, что цели {{ ydb-short-name }} в Prometheus в состоянии **UP** (см. [Запуск Prometheus с подготовленной конфигурацией](#prometheus-start)), а на дашборде **YDB Essential Metrics** отображаются метрики.

## Доступ к метрикам через веб-интерфейс {#web-metrics}

Помимо сбора в Prometheus, каждый узел кластера предоставляет встроенный HTTP-интерфейс для просмотра метрик в браузере. Интерфейс показывает текущие значения метрик на момент запроса, без истории; непрерывный сбор и хранение настраивают в Prometheus (см. [выше](#prometheus-grafana)). Интерфейс слушает порт `--mon-port` (по умолчанию `8765`) на хосте узла.

Главная страница — `http://<ydb-server-address>:<ydb-port>/counters/`: на ней отображается список групп метрик (подсистем). Имена групп и метрик — в [описании метрик](../../reference/observability/metrics/index.md).

где:

- `<ydb-server-address>` — адрес сервера {{ ydb-short-name }};
- `<ydb-port>` — порт мониторинга узла, параметр `--mon-port` при запуске. Значение по умолчанию: `8765`. Как определить порт на конкретном хосте, см. [ниже](#web-metrics-mon-port).

При включенном TLS на узле используйте схему `https://` в URL.

### Как определить порт мониторинга (`--mon-port`) {#web-metrics-mon-port}

Если порт неизвестен, определите его на хосте, где запущен узел {{ ydb-short-name }} (по SSH или в локальной консоли). Если в кластере несколько серверов, при необходимости выполните команду на каждом из них.

```bash
ps aux | grep ydbd
```

![пример вывода ps aux с параметром --mon-port у процессов ydbd](../../_assets/mon-port.png)

В выводе может быть несколько процессов `ydbd` с разными значениями `--mon-port` (например, статический и динамический узел на одном сервере). Добавьте в мониторинг все порты тех узлов, метрики которых нужно собирать. На скриншоте выделены отдельные значения только для примера — ориентируйтесь на фактический вывод команды на ваших хостах.

### Группы метрик на главной странице {#web-metrics-groups}

На главной странице перечислены группы метрик по подсистемам — `auth`, `compile`, `grpc`, `kqp`, `pdisks`, `vdisks` и другие. Каждая группа — ссылка на страницу с метриками этой подсистемы.

![пример веб-интерфейса мониторинга YDB со списком групп метрик](../../_assets/monitoring-UI.png)

### Просмотр метрик группы (подсистемы) {#web-metrics-subgroup}

Чтобы открыть метрики одной группы (подсистемы), перейдите по URL:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/
```

- `<servicename>` — имя группы (подсистемы). Доступные группы отображаются на главной странице веб-интерфейса мониторинга (см. [Группы метрик на главной странице](#web-metrics-groups)).

Используйте `http` или `https` в соответствии с настройкой TLS кластера (как в [Метриках в формате Prometheus](#web-metrics-prometheus)).

Например, метрики утилизации ресурсов сервера — в группе `utils`:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=utils
```

### Метрики в формате Prometheus {#web-metrics-prometheus}

Тот же узел отдает метрики в [формате Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) — по URL с суффиксом `/prometheus`. Именно эти адреса опрашивает Prometheus (параметр `metrics_path` в `scrape_configs`).

{% list tabs %}

- Без TLS

  ```text
  http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus
  ```

  Проверить доступность эндпоинта:

  ```bash
  curl "http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus"
  ```

- С TLS

  ```text
  https://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus
  ```

  Проверить доступность эндпоинта:

  ```bash
  curl --cacert <path-to-ca.crt> "https://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus"
  ```

  Укажите в `--cacert` тот же путь, что в `ca_file` в `tls_config` (см. [подготовку конфигурации](#prometheus-grafana)). При самоподписанном сертификате для диагностики можно добавить флаг `-k`.

{% endlist %}

### Связь с конфигурацией Prometheus {#web-metrics-prom-config}

В шаблонном `prometheus_ydb.yml` хосты и порты совпадают с `ydbd-storage.yml` и `ydbd-database.yml`. Для каждой группы метрик в `scrape_configs` задан `metrics_path` вида `/counters/counters=<servicename>/prometheus` — те же подсистемы, что в списке на [главной странице](#web-metrics-groups) веб-интерфейса.

Другие системы с поддержкой формата Prometheus (Zabbix, Amazon CloudWatch и др.) подключают к тем же URL.

## См. также {#see-also}

- [{#T}](../../reference/observability/metrics/index.md)
- [{#T}](../../reference/observability/metrics/grafana-dashboards.md)
- [{#T}](../../quickstart.md)
- [{#T}](../deployment-options/index.md)
