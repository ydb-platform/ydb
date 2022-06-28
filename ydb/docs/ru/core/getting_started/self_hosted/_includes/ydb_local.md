# Запуск {{ ydb-short-name }} из бинарного файла

В данном разделе описывается процесс разворачивания локального одноузлового кластера {{ ydb-short-name }} c использованием собранного бинарного файла. В настоящее время поддерживается **только сборка для Linux**. Сборки под Windows и MacOS будут добавлены позже.

## Параметры соединения {#conn}

В результате исполнения описанных ниже шагов вы получите запущенную на локальной машине базу данных YDB, с которой можно соединиться по следующим реквизитам:

- [Эндпоинт](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
- [Путь базы данных](../../../concepts/connect.md#database): `/Root/test`
- [Аутентификация](../../../concepts/connect.md#auth-modes): Анонимная (без аутентификации)

## Установка {#install}

Создайте рабочую директорию. Запустите в ней скрипт скачивания архива с исполняемым файлом `ydbd` и необходимыми для работы {{ ydb-short-name }} библиотеками, а также набора скриптов и вспомогательных файлов для запуска и остановки сервера:

```bash
curl https://binaries.ydb.tech/local_scripts/install.sh | bash
```

{% include [wget_auth_overlay.md](wget_auth_overlay.md) %}

## Запуск {#start}

Локальный сервер YDB может быть запущен в режиме работы с диском или в памяти:

{% list tabs %}

- Хранение данных на диске

  - Для хранения данных на диске при первом запуске скрипта в рабочей директории будет создан файл `ydb.data` размером 64ГБ. Убедитесь, что у вас есть достаточно свободного места для его создания.

  - Выполните следующую команду из рабочей директории:

    ``` bash
    ./start.sh disk
    ```

- Хранение данных в памяти

  - При хранении данных в памяти остановка сервера приведет к их потере.

  - Выполните следующую команду из рабочей директории:

    ``` bash
    ./start.sh ram
    ```

{% endlist %}

Запуск сервера YDB производится в контексте текущего окна терминала. Закрытие окна терминала приведет к остановке сервера.

Если при запуске вы получаете ошибку `Failed to set up IC listener on port 19001 errno# 98 (Address already in use)`, то возможно сервер уже был запущен ранее, и вам нужно остановить его скриптом `stop.sh` (см. ниже).

## Остановка {#stop}

Для остановки сервера выполните команду в рабочей директории:

``` bash
./stop.sh
```

## Выполнение запросов через YDB CLI {#cli}

[Установите YDB CLI](../../../reference/ydb-cli/install.md) и выполнить запросы, как описано в статье [YDB CLI - Начало работы](../../cli.md), используя эндпоинт и путь базы данных в [начале данной статьи](#conn), например:

```bash
ydb -e grpc://localhost:2136 -d /Root/test scheme ls
```

## Работа с базой данных через Web UI {#web-ui}

Для работы со структурой и данными в базе данных также доступен встроенный в процесс `ydbd` web-интерфейс по адресу `http://localhost:8765`. Подробней возможности встроенного веб-интерфейса описаны в разделе [Embedded UI](../../../maintenance/embedded_monitoring/ydb_monitoring.md).


## Мониторинг базы данных с помощью Grafana и Prometheus {#dashboards}

Локальный сервер YDB может быть интегрирован с [Prometheus](https://prometheus.io/) и [Grafana](https://grafana.com/), популярными инструментами с открытым исходным кодом для сбора и визуализации метрик. Чтобы настроить стандартные [дашборды](../../../troubleshooting/grafana_dashboards.html), следуйте этой инструкции:

1. [Установите и запустите](https://prometheus.io/docs/prometheus/latest/getting_started/#downloading-and-running-prometheus) Prometheus с этим [файлом конфигурации](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_ydb_prometheus.yml).
1. [Установите и запустите](https://grafana.com/docs/grafana/latest/getting-started/getting-started/) Grafana.
1. [Создайте](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source) источник данных с типом "prometheus" в Grafana и подсоедините его к инстансу Prometheus.
1. Загрузите [дашборды](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/) в Grafana. Вы можете загрузить дашборды в ручном режиме через функцию Grafana UI [Import](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard) или использовать простой [скрипт](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_upload_dashboards.sh). Пожалуйста, обратите внимание на то, что скрипт использует [базовую аутентификацию](https://grafana.com/docs/grafana/latest/http_api/create-api-tokens-for-org/#authentication) в Grafana. Для других случаев модифицируйте скрипт.


## Дополнительные возможности {#advanced}

Описание развертывания многоузловых кластеров и их конфигурирования находится в разделе [Управление кластером](../../../deploy/index.md).