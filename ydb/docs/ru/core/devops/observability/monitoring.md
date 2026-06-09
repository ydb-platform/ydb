# Настройка мониторинга кластера {{ ydb-short-name }}

Страница описывает настройку сбора метрик кластера {{ ydb-short-name }} в [Prometheus](https://prometheus.io/) и визуализацию в [Grafana](https://grafana.com/). Дополнительно на каждом узле метрики можно просмотреть в браузере по встроенному HTTP-интерфейсу мониторинга — см. раздел [Доступ к метрикам через веб-интерфейс](#web-metrics).

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

Конфигурацию сбора метрик с узлов {{ ydb-short-name }} можно подготовить одним из способов ниже. Во всех случаях в конфигурацию Prometheus должно быть передано содержимое файла `prometheus_ydb.yml`. Список статических и динамических узлов, с которых требуется осуществлять сбор метрик, указывается в файлах `ydbd-storage.yml` и `ydbd-database.yml` соответственно. Пути к указанным файлам задаются относительно рабочей директории процесса Prometheus или указываются абсолютными путями.

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

  Проверьте содержимое сгенерированных файлов `ydbd-storage.yml` и `ydbd-database.yml`. Список узлов и портов должен совпадать с фактической топологией кластера, в том числе с узлами, отображаемыми в интерфейсе {{ ydb-short-name }}.

  Пример проверки содержимого каталога с конфигурацией:

  ```bash
  cd promconf
  ls -la

  -rw-rw-r-- 1 1818 ca.crt
  drwxrwxr-x 2 4096 grafana-dashboards
  -rw-rw-r-- 1 17532 prometheus_ydb.yml
  -rw-rw-r-- 1 165 ydbd-database.yml
  -rw-rw-r-- 1 164 ydbd-storage.yml
  ```

- Вручную без TLS

  Скопируйте файлы из каталога [ydb/deploy/prometheus](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus) репозитория {{ ydb-short-name }}.

  Заполните секции `targets` в [`ydbd-storage.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-storage.yml) и [`ydbd-database.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-database.yml): укажите хосты и порты мониторинга (`--mon-port`) всех узлов хранения и динамических узлов баз данных, с которых нужно собирать метрики (как определить порт, см. блок «Как определить значение параметра `<ydb-port>`» в разделе [Доступ к метрикам через веб-интерфейс](#web-metrics)).

  В [`prometheus_ydb.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/prometheus_ydb.yml) для задач сбора метрик задайте **`scheme: http`** и отключите или удалите параметры `tls_config`.

- Вручную с TLS

  Скопируйте файлы из каталога [ydb/deploy/prometheus](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus) репозитория {{ ydb-short-name }}.

  Заполните секции `targets` в [`ydbd-storage.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-storage.yml) и [`ydbd-database.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-database.yml): укажите хосты и порты мониторинга (`--mon-port`) всех узлов хранения и динамических узлов баз данных, с которых нужно собирать метрики (как определить порт, см. блок «Как определить значение параметра `<ydb-port>`» в разделе [Доступ к метрикам через веб-интерфейс](#web-metrics)).

  В [`prometheus_ydb.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/prometheus_ydb.yml) для задач сбора метрик задайте **`scheme: https`** и настройте `tls_config`. Укажите путь к [сертификату центра сертификации](../deployment-options/manual/initial-deployment/deployment-preparation.md#tls-certificates) (CA), которым подписаны сертификаты TLS кластера:

  ```yaml
  scheme: https
  tls_config:
      ca_file: '<ydb-ca-file>'
  ```

  Файл CA (например, `ca.crt`) должен быть доступен Prometheus по указанному пути. Все файлы в `tls_config` должны быть читаемы пользователем, под которым запущен Prometheus.

  Если в вашей конфигурации используются клиентский сертификат и ключ, добавьте их в `tls_config`:

  ```yaml
  tls_config:
      ca_file: '<ydb-ca-file>'
      cert_file: '<ydb-client-cert-file>'
      key_file: '<ydb-client-key-file>'
  ```

- Локальный однонодовый кластер (быстрый старт)

  Если {{ ydb-short-name }} запущен локально в конфигурации «один узел» и мониторинг слушает на одном порту (часто `8765`), в **обоих** файлах — `ydbd-storage.yml` и `ydbd-database.yml` — в секции `targets` укажите один и тот же адрес, например `localhost:8765` или `<hostname>:8765`.

{% endlist %}

### Запуск Prometheus с подготовленной конфигурацией {#prometheus-start}

Отредактированные файлы положите в **любую** удобную директорию на машине, где запускается Prometheus (рядом с бинарником или отдельно, например `/etc/prometheus`). Файлы `prometheus_ydb.yml`, `ydbd-storage.yml` и `ydbd-database.yml` держите в **одной** папке: в шаблоне к файлам списков целей обращаются **относительные** пути, и Prometheus ищет их относительно **рабочей директории процесса** при старте. Если в `file_sd_configs` заданы абсолютные пути к `ydbd-storage.yml` и `ydbd-database.yml`, запускать Prometheus можно из любой рабочей директории.

Если Prometheus поднимается **только для {{ ydb-short-name }}**, в параметре `--config.file` укажите **полный или относительный путь** к `prometheus_ydb.yml`. Перед запуском перейдите в каталог с конфигурацией и запустите процесс из него:

```bash
cd <путь_к_каталогу_с_конфигами>
prometheus --config.file=prometheus_ydb.yml
```

Если Prometheus **уже** используется для других систем, **не** подменяйте основной конфиг файлом `prometheus_ydb.yml` в `--config.file`. Добавьте в существующий файл конфигурации (обычно `prometheus.yml`) задачи из секции `scrape_configs` в `prometheus_ydb.yml` — все записи с `job_name`, начинающимся с `ydb/`. 

Файлы `ydbd-storage.yml` и `ydbd-database.yml` разместите в каталоге, откуда Prometheus читает пути в `file_sd_configs`, или укажите для них абсолютные пути в этих задачах.

Секцию `global:` из `prometheus_ydb.yml` переносите только при необходимости и сверьте значения с уже заданными в вашем конфиге. Запускайте Prometheus с вашим конфигом, например:

```bash
prometheus --config.file=<ваш_prometheus.yml>
```

После изменений проверьте конфигурацию:

```bash
promtool check config <ваш_prometheus.yml>
```

Проверьте, что Prometheus запущен и отвечает:

```bash
curl "http://localhost:9090/-/healthy"
```

В веб-интерфейсе Prometheus (как правило, порт `9090`) откройте **Status** → **Target** и убедитесь, что группы опроса метрик находятся в состоянии успешного сбора. Исключение — группа, связанная с топиками: при отсутствии топиков в базе данных для нее может отображаться ответ **`204 No content`** — это не признак ошибки конфигурации.

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

После настройки Prometheus и Grafana проверьте на узлах кластера порты мониторинга (`--mon-port`) и группы метрик из `ydbd-storage.yml` и `ydbd-database.yml`. Для этого откройте в браузере URL веб-интерфейса `/counters/` — см. раздел [Доступ к метрикам через веб-интерфейс](#web-metrics).

## Доступ к метрикам через веб-интерфейс {#web-metrics}

Каждый узел кластера отдает собственный набор метрик по HTTP или HTTPS. Имена групп и метрик поясняются в [описании метрик](../../reference/observability/metrics/index.md). Мгновенные значения можно открыть в браузере по базовому пути `/counters/` на том же хосте и порту мониторинга (`--mon-port`), которые участвуют в опросе Prometheus:

```text
http://<ydb-server-address>:<ydb-port>/counters/
```

где:

- `<ydb-server-address>` – адрес сервера {{ ydb-short-name }};
- `<ydb-port>` – порт {{ ydb-short-name }}, указанный в параметре `--mon-port` при запуске узла. Значение по умолчанию: `8765`.

При включенном TLS на узле используйте схему `https://` в URL.

В шаблонном `prometheus_ydb.yml` хосты и порты узлов совпадают с теми, что заданы в `ydbd-storage.yml` и `ydbd-database.yml`; для каждой группы метрик в `scrape_configs` задается `metrics_path`, обычно вида `/counters/counters=<имя_подсистемы>/prometheus`. Подсистемы (`auth`, `compile`, `grpc`, `kqp`, `pdisks`, `vdisks` и другие) совпадают с группами, которые видны на странице `/counters/` в браузере.

{% cut "Как определить значение параметра `<ydb-port>`" %}

Определите значение параметра `<ydb-port>` командой на **том хосте (сервере)**, где запущен узел {{ ydb-short-name }}: по SSH к этой машине или в локальной консоли на ней. Если в кластере несколько серверов, при необходимости выполните команду на каждом из них.

```bash
ps aux | grep ydbd
```

![пример вывода ps aux с параметром --mon-port у процессов ydbd](../../_assets/mon-port.png)

В выводе может быть несколько процессов `ydbd` с **разными** значениями `--mon-port` (например, статический и динамический узел на одном сервере). Добавьте в мониторинг **все** порты тех узлов, метрики которых нужно собирать. На скриншоте выделены отдельные значения только для примера — ориентируйтесь на фактический вывод команды на ваших хостах.

{% endcut %}

{% cut "Пример интерфейса" %}

![пример веб-интерфейса мониторинга YDB со списком групп метрик](../../_assets/monitoring-UI.png)

{% endcut %}

Связанные метрики объединены в подгруппы (например, `counters auth`). Чтобы посмотреть значения метрик только определенной подгруппы, перейдите по URL следующего вида:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/
```

- `<servicename>` — имя подгруппы метрик.

Например, данные об утилизации аппаратных ресурсов сервера объединены в подгруппу `utils` и доступны по следующему URL:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=utils
```

Тот же узел отдает метрики в [формате Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) по пути с суффиксом `/prometheus` (его использует Prometheus в `metrics_path`):

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus
```

- `<servicename>` — имя подгруппы метрик.

Пример проверки доступности эндпоинта метрик:

```bash
curl "http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/prometheus"
```

Для TLS используйте `https://`; при необходимости для диагностики самоподписанного сертификата добавьте флаг `-k`.

Непрерывный сбор и визуализацию настраивают в разделе [Настройка мониторинга с помощью Prometheus и Grafana](#prometheus-grafana); другие системы с поддержкой формата Prometheus (например, Zabbix или Amazon CloudWatch) подключают к тем же URL по аналогии.

## См. также {#see-also}

- [Описание метрик](../../reference/observability/metrics/index.md)
- [Справочник по дашбордам Grafana](../../reference/observability/metrics/grafana-dashboards.md)
- [Быстрый старт](../../quickstart.md)
- [Способы развертывания](../deployment-options/index.md)
