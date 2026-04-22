# Настройка мониторинга кластера {{ ydb-short-name }}

Страница описывает настройку сбора метрик кластера {{ ydb-short-name }} в [Prometheus](https://prometheus.io/) и визуализацию в [Grafana](https://grafana.com/), а также просмотр метрик в браузере на узле. В типичной эксплуатации метрики собирают со всех узлов, агрегируют и строят графики в системах мониторинга; веб-интерфейс `/counters/` используют для проверки портов, сопоставления с конфигурацией сбора и точечной диагностики (см. раздел [Доступ к метрикам через веб-интерфейс](#web-metrics)).

{% note tip %}

Перед началом работы ознакомьтесь с [описанием метрик](../../reference/observability/metrics/index.md) и [справочником по дашбордам Grafana](../../reference/observability/metrics/grafana-dashboards.md).

Определите, как развернут {{ ydb-short-name }}:

- по [одному из способов развертывания кластера](../deployment-options/index.md) (Ansible, Kubernetes или вручную);
- либо в конфигурации [быстрого старта](../../quickstart.md) (одноузловой локальный кластер).

{% endnote %}

## Настройка мониторинга с помощью Prometheus и Grafana {#prometheus-grafana}

### Подготовка к установке {#installation-preparation}

- [Установите](https://prometheus.io/docs/prometheus/latest/getting_started) Prometheus.
- [Установите](https://grafana.com/docs/grafana/latest/setup-grafana/) Grafana.

Полный цикл развертывания Prometheus и Grafana в этом разделе не рассматривается — обратитесь к документации выбранных продуктов.

### Варианты подготовки конфигурации сбора метрик {#prometheus-config-variants}

Конфигурацию опроса узлов {{ ydb-short-name }} можно подготовить одним из способов ниже. Во всех случаях Prometheus должен получить файл `prometheus_ydb.yml` и связанные с ним списки целей; пути к подключаемым файлам в `prometheus_ydb.yml` задаются относительно рабочей директории процесса Prometheus или указываются абсолютными путями.

{% list tabs %}

- Ansible (с TLS)

  Рекомендуемый способ получить согласованный набор файлов (в том числе с TLS) — сгенерировать конфигурацию плейбуком развертывания.

  Перейдите в рабочий каталог сценария Ansible для вашего кластера:

  ```bash
  cd <path_to_ansible_project>
  ```

  Запустите плейбук генерации конфигурации для Prometheus:

  ```bash
  ansible-playbook ydb_platform.ydb.generate_promconf
  ```

  Плейбук создаст каталог (например, `promconf`) с `prometheus_ydb.yml`, файлами списков узлов и при необходимости файлами для TLS. Часто вместе с конфигурацией подтягиваются актуальные шаблоны дашбордов для Grafana из [репозитория {{ ydb-short-name }}](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/helm/ydb-prometheus/dashboards).

  Проверьте содержимое сгенерированных файлов списков целей (имена в вашей сборке должны соответствовать ссылкам внутри `prometheus_ydb.yml`, обычно это `ydbd-storage.yml` и `ydbd-database.yml`). Список узлов и портов должен совпадать с фактической топологией кластера, в том числе с узлами, отображаемыми в интерфейсе {{ ydb-short-name }}.

  Пример проверки содержимого каталога с конфигурацией:

  ```bash
  cd promconf
  ls -la
  ```

- Вручную без TLS

  Скопируйте файлы из каталога [ydb/deploy/prometheus](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus) репозитория {{ ydb-short-name }}.

  Заполните секции `targets` в [`ydbd-storage.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-storage.yml) и [`ydbd-database.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-database.yml): укажите хосты и порты мониторинга (`--mon-port`) всех узлов хранения и динамических узлов баз данных, с которых нужно собирать метрики (как определить порт, см. блок «Как определить значение параметра `<ydb-port>`» в разделе [Доступ к метрикам через веб-интерфейс](#web-metrics)).

  В [`prometheus_ydb.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/prometheus_ydb.yml) для джобов сбора метрик задайте **`scheme: http`** и отключите или удалите параметры `tls_config`, если шаблон из репозитория ориентирован на HTTPS.

  Пример быстрой проверки заполненных целей:

  ```bash
  cat ydbd-storage.yml
  cat ydbd-database.yml
  ```

- Вручную с TLS

  Скопируйте файлы из каталога [ydb/deploy/prometheus](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus) репозитория {{ ydb-short-name }}.

  Заполните секции `targets` в [`ydbd-storage.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-storage.yml) и [`ydbd-database.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/ydbd-database.yml): укажите хосты и порты мониторинга (`--mon-port`) всех узлов хранения и динамических узлов баз данных, с которых нужно собирать метрики (как определить порт, см. блок «Как определить значение параметра `<ydb-port>`» в разделе [Доступ к метрикам через веб-интерфейс](#web-metrics)).

  В [`prometheus_ydb.yml`](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/prometheus/prometheus_ydb.yml) для джобов сбора метрик задайте **`scheme: https`** и настройте `tls_config`. Укажите путь к [сертификату центра сертификации](../deployment-options/manual/initial-deployment/deployment-preparation.md#tls-certificates) (CA), которым подписаны сертификаты TLS кластера:

  ```yaml
  scheme: https
  tls_config:
      ca_file: '<ydb-ca-file>'
  ```

  Файл CA (например, `ca.crt`) должен быть доступен Prometheus по указанному пути. Если в вашей конфигурации используются клиентский сертификат и ключ, добавьте их в `tls_config` и проверьте доступность файлов для процесса Prometheus.

  Пример быстрой проверки заполненных целей:

  ```bash
  cat ydbd-storage.yml
  cat ydbd-database.yml
  ```

  Шаблон `prometheus_ydb.yml` в репозитории может отличаться от конфигурации, оптимальной для вашего кластера; канонический вариант для промышленного окружения надежнее получать генерацией через Ansible. При необходимости помощи с ручной сборкой TLS-конфигурации обратитесь к команде сопровождения {{ ydb-short-name }}.

- Локальный однонодовый кластер (быстрый старт)

  Если {{ ydb-short-name }} запущен локально в конфигурации «один узел» и мониторинг слушает на одном порту (часто `8765`), в **обоих** файлах — `ydbd-storage.yml` и `ydbd-database.yml` — в секции `targets` укажите один и тот же адрес, например `localhost:8765` или `<hostname>:8765`.

{% endlist %}

### Запуск Prometheus с подготовленной конфигурацией {#prometheus-start}

Отредактированные файлы положите в **любую** удобную директорию на машине, где запускается Prometheus (рядом с бинарником или отдельно, например `/etc/prometheus`). В параметре `--config.file` укажите **полный или относительный путь** к `prometheus_ydb.yml`. Файлы `prometheus_ydb.yml`, `ydbd-storage.yml` и `ydbd-database.yml` держите в **одной** папке: в шаблоне к файлам списков целей обращаются **относительные** пути, и Prometheus ищет их относительно **рабочей директории процесса** при старте. Перед запуском Prometheus перейдите в эту папку и запустите процесс из нее:

```bash
cd <путь_к_каталогу_с_конфигами>
prometheus --config.file=prometheus_ydb.yml
```

Альтернативный вариант — указать в `prometheus_ydb.yml` абсолютные пути к файлам целей.

Проверьте, что Prometheus запущен и отвечает:

```bash
curl "http://localhost:9090/-/healthy"
```

В веб-интерфейсе Prometheus (как правило, порт `9090`) откройте **Status** → **Target health** и убедитесь, что группы опроса метрик находятся в состоянии успешного сбора. Исключение — группа, связанная с топиками: при отсутствии топиков в базе данных для нее может отображаться ответ **`204 No content`** — это не обязательно признак ошибки конфигурации.

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

Готовые дашборды {{ ydb-short-name }} находятся в [репозитории](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/helm/ydb-prometheus/dashboards). Если вы использовали плейбук Ansible для генерации конфигурации Prometheus, шаблоны дашбордов могут уже лежать в каталоге рядом с `promconf` (название подкаталога зависит от версии сценария). Импортируйте JSON-файлы в Grafana через веб-интерфейс, [provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) или [скрипт загрузки](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_upload_dashboards.sh). Скрипт по умолчанию рассчитан на [базовую аутентификацию](https://grafana.com/docs/grafana/latest/http_api/create-api-tokens-for-org/#authentication) Grafana и при необходимости изменяется под вашу среду.

Для импорта через интерфейс Grafana используйте функцию **Import** в меню дашбордов.

Состав панелей и рекомендации по использованию дашбордов приведены в [справочнике по дашбордам Grafana](../../reference/observability/metrics/grafana-dashboards.md). На дашборде **YDB Essential Metrics** в верхней части выбирают источник данных и имя базы данных.

### Результат {#result}

{% cut "Пример дашборда в Grafana" %}

![-](../../_assets/grafana.png)

{% endcut %}

После настройки Prometheus и Grafana приведенные ниже URL помогут убедиться, что на узле доступны те же подсистемы метрик и порты, которые указаны в `ydbd-storage.yml` и `ydbd-database.yml` в разделе [Настройка мониторинга с помощью Prometheus и Grafana](#prometheus-grafana).

## Доступ к метрикам через веб-интерфейс {#web-metrics}

Каждый узел кластера отдает собственный набор метрик по HTTP или HTTPS. Имена групп и метрик поясняются в [описании метрик](../../reference/observability/metrics/index.md). Мгновенные значения можно открыть в браузере по базовому пути `/counters/` на том же хосте и порту мониторинга (`--mon-port`), которые участвуют в опросе Prometheus:

```text
http://<ydb-server-address>:<ydb-port>/counters/
```

где:

- `<ydb-server-address>` – адрес сервера {{ ydb-short-name }};
- `<ydb-port>` – порт {{ ydb-short-name }}, указанный в параметре `--mon-port` при запуске узла. Значение по умолчанию: `8765`.

При включенном TLS на узле используйте схему `https://` в URL.

В шаблонном **`prometheus_ydb.yml`** хосты и порты целей совпадают с теми, что заданы в **`ydbd-storage.yml`** и **`ydbd-database.yml`**; для каждой группы метрик в `scrape_configs` задается **`metrics_path`**, обычно вида `/counters/counters=<имя_подсистемы>/prometheus`. Подсистемы (`auth`, `compile`, `grpc`, `kqp`, `pdisks`, `vdisks` и другие) совпадают с группами, которые видны на странице `/counters/` в браузере.

{% cut "Как определить значение параметра `<ydb-port>`" %}

Определите значение параметра `<ydb-port>` командой на **том хосте (сервере)**, где запущен узел {{ ydb-short-name }}: по SSH к этой машине или в локальной консоли на ней. Если в кластере несколько серверов, при необходимости выполните команду на каждом из них.

```bash
ps aux | grep ydbd
```

![-](../../_assets/mon-port.png)

В выводе может быть несколько процессов `ydbd` с **разными** значениями `--mon-port` (например, статический и динамический узел на одном сервере). Добавьте в мониторинг **все** порты тех узлов, метрики которых нужно собирать. На скриншоте выделены отдельные значения только для примера — ориентируйтесь на фактический вывод команды на ваших хостах.

{% endcut %}

{% cut "Пример интерфейса" %}

![-](../../_assets/monitoring-UI.png)

{% endcut %}

Связанные метрики объединены в подгруппы (например, `counters auth`). Чтобы посмотреть значения метрик только определенной подгруппы, перейдите по URL следующего вида:

```text
http://<ydb-server-address>:<ydb-port>/counters/counters=<servicename>/
```

- `<servicename>` — имя подгруппы метрик.

Например, данные об утилизации аппаратных ресурсов сервера доступны по следующему URL:

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
