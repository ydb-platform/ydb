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
docker_args=(
    -d                              # запуск в фоне
    --rm                            # автоматическое удаление после установки
    --name ydb-local                # имя контейнера
    --hostname localhost            # хостейм
    --platform linux/amd64          # платформа
    -p 2135:2135                    # открытие внешнего доступа к grpcs порту
    -p 2136:2136                    # открытие внешнего доступа к grpc порту
    -p 8765:8765                    # открытие внешнего доступа к http порту
    -p 5432:5432                    # открытие внешнего доступа к порту, обеспечивающему PostgreSQL-совместимость
    -p 9092:9092                    # открытие внешнего доступа к порту, обеспечивающему Kafka-совместимость
    -v $(pwd)/ydb_certs:/ydb_certs  # директория для TLS сертификатов
    -v $(pwd)/ydb_data:/ydb_data    # рабочая директория
    -e GRPC_TLS_PORT=2135           # grpcs порт должен соответствовать тому, что опубликовано выше
    -e GRPC_PORT=2136               # grpc порт должен соответствовать тому, что опубликовано выше
    -e MON_PORT=8765                # http  порт должен соответствовать тому, что опубликовано выше
    -e YDB_KAFKA_PROXY_PORT=9092    # порт Kafka должен соответствовать тому, что опубликовано выше
    {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
)

docker run "${docker_args[@]}"
```

{% include [index.md](_includes/rosetta.md) %}

Подробнее про переменные окружения, используемые при запуске Docker-контейнера с {{ ydb-short-name }}, можно узнать в разделе [{#T}](configuration.md).

При указанных в примере выше параметрах и запуске Docker локально, [Embedded UI](../embedded-ui/index.md) {{ ydb-short-name }} будет доступен по адресу [http://localhost:8765⁠](http://localhost:8765).

Подробнее про остановку и удаление Docker-контейнера с {{ ydb-short-name }} можно узнать в разделе [{#T}](cleanup.md).

### Переопределение файла конфигурации

По умолчанию при запуске контейнера Docker для {{ ydb-short-name }} используется встроенный [файл конфигурации](../configuration/index.md), который обеспечивает стандартные параметры работы. Для переопределения файла конфигурации при запуске контейнера можно использовать аргумент `--config-path`, указав путь к своему файлу конфигурации, который предварительно примонтирован в контейнер:

```bash
docker run "${docker_args[@]}" --config-path /path/to/your/config/file
```

Для пользователей, не имеющих опыта работы с Docker, важно понимать, как правильно монтировать файл конфигурации в контейнер. Ниже приведен пошаговый пример:

1. Запустите контейнер без указания файла конфигурации и без монтирования директории данных, чтобы он сгенерировал конфигурацию по умолчанию:

   ```bash
   docker run -d \
     --rm \
     --name ydb-local \
     --hostname localhost \
     --platform linux/amd64 \
     -v $(pwd)/ydb_certs:/ydb_certs \
     -e GRPC_TLS_PORT=2135 \
     -e GRPC_PORT=2136 \
     -e MON_PORT=8765 \
     {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
   ```

2. Создайте директорию для файлов конфигурации и скопируйте сгенерированный файл конфигурации из контейнера прямо в нее:

   ```bash
   mkdir ydb_config
   docker cp ydb-local:/ydb_data/cluster/kikimr_configs/config.yaml ydb_config/my-ydb-config.yaml
   ```

3. Остановите контейнер, если он все еще запущен, и удалите созданную директорию данных:

   ```bash
   docker stop ydb-local
   rm -rf ydb_data
   ```

4. Отредактируйте скопированный файл конфигурации `ydb_config/my-ydb-config.yaml` по своему усмотрению.

5. При запуске контейнера используйте флаг `-v` для монтирования директории с вашим файлом конфигурации в контейнер:

   ```bash
   docker_args=(
       -d
       --rm
       --name ydb-local
       --hostname localhost
       --platform linux/amd64
       -p 2135:2135
       -p 2136:2136
       -p 8765:8765
       -v $(pwd)/ydb_certs:/ydb_certs
       -v $(pwd)/ydb_data:/ydb_data
       -v $(pwd)/ydb_config:/ydb_config
       -e GRPC_TLS_PORT=2135
       -e GRPC_PORT=2136
       -e MON_PORT=8765
       {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
   )
   
   docker run "${docker_args[@]}" --config-path /ydb_config/my-ydb-config.yaml
   ```

В этом примере:

- `$(pwd)/ydb_config` - локальная директория на вашем компьютере с файлом конфигурации
- `/ydb_config` - директория внутри контейнера, куда будет смонтирована ваша локальная директория
- `/ydb_config/my-ydb-config.yaml` - путь к файлу конфигурации внутри контейнера

Таким образом, ваш локальный файл конфигурации становится доступным внутри контейнера по указанному пути.
