# Работа с {{ ydb-short-name }} в Docker

{% list tabs %}

- Перед началом работы

    Создайте каталог для тестирования YDB и используйте его в качестве текущего рабочего каталога:

    ```bash
      mkdir ~/ydbd && cd ~/ydbd
      mkdir ydb_data
      mkdir ydb_certs
    ```

- Запуск Docker

    Для запуска {{ ydb-short-name }} в Docker необходимо выполнить следующую команду:

    ```bash
      docker run \
      -d \
      --rm \
      --name ydb-local \
      -h localhost \
      -p 2135:2135 \
      -p 2136:2136 \
      -p 8765:8765 \
      -p 5432:5432 \
      -v $(pwd)/ydb_certs:/ydb_certs \
      -v $(pwd)/ydb_data:/ydb_data \
      -e GRPC_TLS_PORT=2135 \
      -e GRPC_PORT=2136 \
      -e MON_PORT=8765 \
      -e YDB_USE_IN_MEMORY_PDISKS=1 \
      ydbplatform/local-ydb:latest
    ```
    
    Подробнее про переменные окружения, используемые при запуске Docker контейнера с {{ ydb-short-name }} можно узнать [тут](environment.md)

- Веб-интерефейс

    Веб-интерфейс  {{ ydb-short-name }} доступен на [http://localhost:8765⁠](http://localhost:8765⁠).

- Остановка Docker

    Для остановки {{ ydb-short-name }} в Docker небходимо выполнить следующую команду:

    ```bash
      docker stop ydb-local
    ```

    Если при запуске был указан флаг `--rm`, то контейнер будет удалён после остановки.

- Удаление Docker контейнера {{ ydb-short-name }}

    Для удаления Docker контейнера содержащего {{ ydb-short-name }} необходимо выполнить следующую команду:

    ```bash
      docker kill ydb-local
    ```

    Если вы захотите очистить фаловую систему то можете удалить вашу рабочу директорию с помощью команды `rm -rf ~/ydbd`, при этом все данные внутри локального кластера YDB будут потеряны.


{% endlist %}
