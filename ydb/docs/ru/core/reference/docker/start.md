# Запуск {{ ydb-short-name }} в Docker

## Перед началом работы

Создайте каталог для тестирования {{ ydb-short-name }} и используйте его в качестве текущего рабочего каталога:

```bash
mkdir ~/ydbd && cd ~/ydbd
mkdir ydb_data
mkdir ydb_certs
```

## Запуск контейнера с {{ ydb-short-name }} в Docker

Пример команды запуска {{ ydb-short-name }} в Docker с подробными комментариями:

```bash
docker run \
    -d \ # запуск в фоне
    --rm \ # автоматическое удаление после установки
    --name ydb-local \ # имя контейнера
    -h localhost \ # хостейм
    -p 2135:2135 \ # открытие внешнего доступа к grpcs порту
    -p 2136:2136 \ # открытие внешнего доступа к grpc порту
    -p 8765:8765 \ # открытие внешнего доступа к http порту
    -p 5432:5432 \ # открытие внешнего доступа к порту, обеспечивающему PostgreSQL-совместимость
    -v $(pwd)/ydb_certs:/ydb_certs \ # директория для TLS сертификатов
    -v $(pwd)/ydb_data:/ydb_data \ # рабочая директория
    -e GRPC_TLS_PORT=2135 \ # grpcs порт
    -e GRPC_PORT=2136 \ # grpc порт
    -e MON_PORT=8765 \ # http порт
    -e YDB_USE_IN_MEMORY_PDISKS=1 \ # хранение данных только в оперативной памяти
    {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }} # имя и тег образа
```

Подробнее про переменные окружения, используемые при запуске Docker контейнера с {{ ydb-short-name }} можно узнать в разделе [{#T}](environment.md)

При указанных в примере выше параметрах и запуске Docker локально, [Embedded UI](../embedded-ui/index.md) {{ ydb-short-name }} будет доступен по адресу [http://localhost:8765⁠](http://localhost:8765⁠).

Подробнее про остановку и удаление Docker контейнера с {{ ydb-short-name }} можно узнать в разделе [{#T}](cleanup.md)
