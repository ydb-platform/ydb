# Начало работы

В этой инструкции вы [развернете](#install) одноузловой локальный [кластер {{ ydb-short-name }}](../concepts/databases.md#cluster) и [выполните](#queries) простые запросы к [базе данных](../concepts/databases.md#database).

## Разверните кластер {{ ydb-short-name }} {#install}

Разверните кластер {{ ydb-short-name }} из архива с исполняемым файлом или используйте Docker-образ.

{% list tabs %}

- Bin

  {% note info %}

  В настоящее время поддерживается сборка только для Linux. Сборки для Windows и macOS будут добавлены позже.

  {% endnote %}

  1. Создайте рабочую директорию и перейдите в нее:

      ```bash
      mkdir ~/ydbd && cd ~/ydbd
      ```

  1. Скачайте и запустите скрипт установки:

      ```bash
      curl https://binaries.ydb.tech/local_scripts/install.sh | bash
      ```

      Будет загружен и распакован архив с исполняемым файлом `ydbd`, библиотеками, конфигурационными файлами, а также скриптами для запуска и остановки кластера.
  
  1. Запустите кластер в одном из режимов хранения данных:

      * Данные в памяти:

        ```bash
        ./start.sh ram
        ```

        При хранении данных в памяти остановка кластера приведет к их потере.

      * Данные на диске:

        ```bash
        ./start.sh disk
        ```

        При первом запуске скрипта в рабочей директории будет создан файл `ydb.data` размером 80 ГБ. Убедитесь, что у вас есть достаточно свободного места для его создания.

      Результат:

      ```text
      Starting storage process...
      Initializing storage ...
      Registering database ...
      Starting database process...

      Database started. Connection options for YDB CLI:

      -e grpc://localhost:2136 -d /Root/test
      ```

- Docker

  1. Загрузите актуальную версию Docker-образа:

      ```bash
      docker pull {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
      ```

      Убедитесь, что загрузка прошла успешно:

      ```bash
      docker image list | grep {{ ydb_local_docker_image }}
      ```

      Результат:

      ```text
      cr.yandex/yc/yandex-docker-local-ydb   latest    c37f967f80d8   6 weeks ago     978MB
      ```

  1. Запустите Docker-контейнер:

      ```bash
      docker run -d --rm --name ydb-local -h localhost \
        -p 2135:2135 -p 2136:2136 -p 8765:8765 \
        -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
        -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
        -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
        {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
      ```

      При успешном запуске будет выведен идентификатор запущенного контейнера. Инициализация контейнера может занять несколько минут, до окончания инициализации база данных будет недоступна.

{% endlist %}

## Подключитесь к БД {#connect}

Вы можете использовать для подключения к базе данных YDB Embedded UI кластера или [интерфейс командной строки {{ ydb-short-name }} CLI](../reference/ydb-cli/index.md).

{% list tabs %}

- YDB UI

  1. Откройте в браузере страницу:

      ```http
      http://localhost:8765
      ```

  1. В блоке **Database list** выберите базу данных:

      * `/Root/test` — если вы развернули кластер с помощью исполняемого файла;
      * `/local` — если вы использовали Docker-образ.

- YDB CLI

  1. Установите {{ ydb-short-name }} CLI:

      * Для Linux или macOS:

        ```bash
        curl -sSL https://storage.yandexcloud.net/yandexcloud-ydb/install.sh | bash
        ```

        {% note info %}

        Скрипт дополнит переменную `PATH`, только если его запустить в командной оболочке bash или zsh. Если вы запустили скрипт в другой оболочке, добавьте путь до CLI в переменную `PATH` самостоятельно.

        {% endnote %}

      * Для Windows:

        ```cmd
        @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb/install.ps1'))"
        ```

        Укажите, нужно ли добавить путь к исполняемому в переменную окружения `PATH`:

        ```text
        Add ydb installation dir to your PATH? [Y/n]
        ```

        {% note info %}

        {{ ydb-short-name }} CLI использует символы Юникода в выводе некоторых команд. При некорректном отображении таких символов в консоли Windows, переключите кодировку на UTF-8:

        ```cmd
        chcp 65001
        ```

        {% endnote %}

      Чтобы обновить переменные окружения, перезапустите сеанс командной оболочки.
  
  1. Сохраните параметры соединения с БД в [профиле {{ ydb-short-name }} CLI](../reference/ydb-cli/profile/index.md):

      ```bash
      ydb config profile create quickstart --endpoint grpc://localhost:2136 --database <path_database>
      ```

      * `path_database` — путь базы данных. Укажите одно из значений:

        * `/Root/test` — если вы развернули кластер с помощью исполняемого файла;
        * `/local` — если вы использовали Docker-образ.

  1. Проверьте подключение к БД:

      ```bash
      ydb --profile quickstart scheme ls
      ```

      Результат:

      ```text
      .sys_health .sys
      ```

{% endlist %}

## Выполните запросы к БД {#queries}

Выполните запросы к базе данных используя YDB Embedded UI кластера или [интерфейс командной строки {{ ydb-short-name }} CLI](../reference/ydb-cli/index.md).

{% list tabs %}

- YDB UI

  1. Создайте таблицы в базе данных:
  
      В блоке **Query** введите текст запроса:

      {% include [create-tables](../_includes/create-tables.md) %}

      Нажмите кнопку **Run Script**.

  1. Добавьте данные в созданные таблицы:
  
      В блоке **Query** введите текст запроса:

      {% include [upsert](../_includes/upsert.md) %}

      Нажмите кнопку **Run Script**.

  1. Выберите данных из таблицы `series`:

      В блоке **Query** введите текст запроса:

      {% include [upsert](../_includes/select.md) %}

      Нажмите кнопку **Run Script**.

      Ниже отобразится результат выполнения запроса:

      ```text
      series_id	series_title	release_date
      1	IT Crowd	13182
      2	Silicon Valley	16166
      ```

  1. Удалите данные из таблицы `episodes`:

      В блоке **Query** введите текст запроса:

      {% include [upsert](../_includes/delete.md) %}

      Нажмите кнопку **Run Script**.

- YDB CLI

  1. Создайте таблицы в базе данных:

      Сохраните текст запроса в файл `create-table.sql`:

      {% include [create-tables](../_includes/create-tables.md) %}

      Выполните запрос:

      ```bash
      ydb --profile quickstart yql --file create-table.sql
      ```

  1. Добавьте данные в созданные таблицы:
  
      Сохраните текст запроса в файл `upsert.sql`:

      {% include [upsert](../_includes/upsert.md) %}

      Выполните запрос:

      ```bash
      ydb --profile quickstart yql --file upsert.sql
      ```

  1. Выберите данных из таблицы `series`:

      Сохраните текст запроса в файл `select.sql`:

      {% include [select](../_includes/select.md) %}

      Выполните запрос:

      ```bash
      ydb --profile quickstart yql --file select.sql
      ```

      Результат:

      ```text
      ┌───────────┬──────────────────┬──────────────┐
      | series_id | series_title     | release_date |
      ├───────────┼──────────────────┼──────────────┤
      | 1         | "IT Crowd"       | "2006-02-03" |
      ├───────────┼──────────────────┼──────────────┤
      | 2         | "Silicon Valley" | "2014-04-06" |
      └───────────┴──────────────────┴──────────────┘
      ```

  1. Удалите данные из таблицы `episodes`:

      Сохраните текст запроса в файл `delete.sql`:

      {% include [delete](../_includes/delete.md) %}

      Выполните запрос:

      ```bash
      ydb --profile quickstart yql --file delete.sql
      ```

      Просмотрите таблицу `episodes`:

      ```bash
      ydb --profile quickstart yql --script "SELECT * FROM episodes;"
      ```

      Результат:

      ```text
      ┌──────────────┬────────────┬───────────┬───────────┬──────────────────────────┐
      | air_date     | episode_id | season_id | series_id | title                    |
      ├──────────────┼────────────┼───────────┼───────────┼──────────────────────────┤
      | "2006-02-03" | 1          | 1         | 1         | "Yesterday's Jam"        |
      ├──────────────┼────────────┼───────────┼───────────┼──────────────────────────┤
      | "2006-02-03" | 2          | 1         | 1         | "Calamity Jen"           |
      ├──────────────┼────────────┼───────────┼───────────┼──────────────────────────┤
      | "2014-04-06" | 1          | 1         | 2         | "Minimum Viable Product" |
      └──────────────┴────────────┴───────────┴───────────┴──────────────────────────┘
      ```

      Из таблицы удалена строка о сериале The Cap Table.

{% endlist %}

## Остановите кластер {#stop}

По окончании работы остановите кластер {{ ydb-short-name }}.

{% list tabs %}

- Bin

  Чтобы остановить кластер, выполните команду в директории `~/ydbd`:

  ``` bash
  ./stop.sh
  ```

- Docker

  Чтобы остановить Docker-контейнер с кластером, выполните команду:

    ```bash
    docker kill ydb-local
    ```

{% endlist %}

## Что дальше {#advanced}

* Изучите [концепции {{ ydb-short-name }}](../concepts/index.md).
* Узнайте подробнее об этих и других способах [развертывания {{ ydb-short-name }}](../deploy/index.md).
* Посмотрите, как работать с БД {{ ydb-short-name }} при помощи [SDK](../reference/ydb-sdk/index.md).
* Ознакомьтесь с языком запросов [YQL](../yql/reference/index.md).
