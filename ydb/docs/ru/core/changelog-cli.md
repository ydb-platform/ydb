# Список изменений {{ ydb-short-name }} CLI

## Версия 2.28.0 {#2-28-0}

Дата выхода 19 декабря 2025. Для обновления до версии **2.28.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлены режимы транзакций `snapshot-ro` и `snapshot-rw` в опцию `--tx-mode` [команды](./reference/ydb-cli/table-query-execute.md) `{{ ydb-cli }} table query execute`.
* Добавлена поддержка переменной окружения `NO_COLOR` для отключения ANSI-цветов в {{ ydb-short-name }} CLI (см. [no-color.org](https://no-color.org/)).
* Добавлен простой прогресс-бар для неинтерактивного stderr.
* Добавлено свойство `omit-indexes` в опцию `--item` [команды](./reference/ydb-cli/tools-copy.md) `{{ ydb-cli }} tools copy`, позволяющее копировать таблицы без индексов.
* Добавлена подкоманда `import files` в [команду](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload vector` для заполнения таблицы из CSV или Parquet файлов.
* Добавлена подкоманда `import generate` в [команду](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload vector` для заполнения таблицы случайными данными.
* **_(Требуется сервер v26.1+)_** Изменения в ранее добавленной команде `{{ ydb-cli }} admin cluster state fetch`:
  * Переименована в `{{ ydb-cli }} admin cluster diagnostics collect`.
  * Добавлена опция `--no-sanitize`, отключающая санитизацию и сохраняющая чувствительные данные в выводе.
  * Добавлена опция `--output` для указания пути к выходному файлу `.tar.bz2`.

### Исправления ошибок

* Исправлена ошибка, при которой [команда](./reference/ydb-cli/export-import/tools-restore.md) `{{ ydb-cli }} tools restore` могла аварийно завершаться с ошибкой `mutex lock failure (Invalid argument)` из-за внутреннего состояния гонки.
* Исправлено восстановление представлений (views), содержащих именованные выражения, и представлений, обращающихся к вторичным индексам, в [команде](./reference/ydb-cli/export-import/tools-restore.md) `{{ ydb-cli }} tools restore`.

## Версия 2.27.0 {#2-27-0}

Дата выхода 30 октября 2025. Для обновления до версии **2.27.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена опция `--exclude` в [команду](./reference/ydb-cli/export-import/import-s3.md) `{{ ydb-cli }} import s3`, позволяющая исключать схемные объекты из импорта по шаблону имени.
* Добавлена поддержка объектов типа [трансфер](./concepts/transfer.md) при выполнении [команды](./reference/ydb-cli/export-import/tools-dump.md) `{{ ydb-cli }} tools dump` и [команды](./reference/ydb-cli/export-import/tools-restore.md) `{{ ydb-cli }} tools restore`.
* Добавлена новая опция `--retention-period` в подкоманды `{{ ydb-cli }} topic`. Использование устаревшей опции `--retention-period-hours` не рекомендуется.
* В [команде](./reference/ydb-cli/topic-consumer-add.md) `{{ ydb-cli }} topic consumer add` появилась новая опция `--availability-period`, которая переопределяет гарантию удержания для читателя.
* В [командах](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload vector` добавлены подкоманды `build-index` и `drop-index`.
* **_(Требуется сервер v26.1+)_** Добавлена команда `{{ ydb-cli }} admin cluster state fetch` для сбора информации о состоянии узлов кластера и метриках.

### Исправления ошибок

* Исправлена ошибка, из-за которой команда `{{ ydb-cli }} debug ping` аварийно завершалась.

## Версия 2.26.0 {#2-26-0}

Дата выхода 25 сентября 2025. Для обновления до версии **2.26.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлены опции `--no-merge` и `--no-cache` в [команду](./reference/ydb-cli/commands/monitoring-healthcheck.md) `{{ ydb-cli }} monitoring healthcheck`.
* Добавлена статистика времени компиляции запроса в [команды](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload * run`.
* Добавлена опция `--retries` в [команду](./reference/ydb-cli/export-import/tools-restore.md) `{{ ydb-cli }} tools restore`, позваляющая задать количество повторных попыток для каждого запроса загрузки данных.
* **_(Требуется сервер v25.4+)_** Добавлена опция `--replace-sys-acl` в [команду](./reference/ydb-cli/export-import/tools-restore.md) `{{ ydb-cli }} tools restore`, которая задаёт, нужно ли заменять ACL для системных объектов.

## Версия 2.25.0 {#2-25-0}

Дата выхода 1 сентября 2025. Для обновления до версии **2.25.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена финальная статистика выполнения в [команды](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload * run`.
* Добавлена опция `--start-offset` в [команду](./reference/ydb-cli/topic-read.md) `{{ ydb-cli }} topic read`, которая задаёт начальную позицию для чтения из выбранной партиции.
* **_(Требуется сервер v25.3+)_** Добавлен новый способ указания путей в [командах](./reference/ydb-cli/export-import/export-s3.md) `{{ ydb-cli }} export s3` и `{{ ydb-cli }} import s3` с новой опцией `--include` вместо опции `--item`.
* **_(Требуется сервер v25.3+)_** Добавлена поддержка функций шифрования в [командах](./reference/ydb-cli/export-import/export-s3.md) `{{ ydb-cli }} export s3` и `{{ ydb-cli }} import s3`.
* **_(Требуется сервер v25.3+)_** **_(Экспериментально)_** Добавлены [команды](./reference/ydb-cli/commands/bridge/index.md) `{{ ydb-cli }} admin cluster bridge` для управления кластером в [режиме bridge](./concepts/bridge.md): `list`, `switchover`, `failover`, `takedown`, `rejoin`.

### Улучшения

* Опции аутентификации по имени пользователя и паролю теперь обрабатываются независимо, что позволяет получать их из разных источников приоритета. Например, имя пользователя можно указать с помощью опции `--user`, а пароль получить из переменной окружения `YDB_PASSWORD`.
* Изменён уровень логирования по умолчанию с `EMERGENCY` на `WARN` для команд, поддерживающих несколько уровней логирования.

### Изменения с потерей обратной совместимости

* Удалена опция `--float-mode` из [команд](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload tpch run` и `{{ ydb-cli }} workload tpcds run`. Режим работы с вещественными числами теперь определяется автоматически из схемы таблицы, созданной во время фазы `init`.

### Исправления ошибок

* Исправлена ошибка, при которой [команда](./reference/ydb-cli/export-import/import-file.md) `{{ ydb-cli }} import file csv` с опцией `--newline-delimited` могла зависать при некорректных входных данных.
* Исправлена ошибка с отображением прогресс-бара в [команде](./reference/ydb-cli/workload-click-bench.md) `{{ ydb-cli }} workload clickbench import files` — неправильное процентное значение и избыточные переносы строк, приводящие к дублированию строк прогресса.
* Исправлена ошибка, при которой [команда](./reference/ydb-cli/topic-write.md) `{{ ydb-cli }} workload topic write` могла завершаться аварийно с ошибкой `Unknown AckedMessageId` из-за внутреннего состояния гонки.
* Исправлено сравнение десятичных типов в [командах](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload * run`.

## Версия 2.24.1 {#2-24-1}

Дата выхода 28 июля 2025. Для обновления до версии **2.24.1** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Исправления ошибок

* Исправлена ошибка, при которой [команда](./reference/ydb-cli/export-import/tools-dump.md#schema-objects) `{{ ydb-cli }} tools dump` без уведомления пропускала объекты схемы неподдерживаемых типов и создавала для них пустые директории в целевой папке на файловой системе.

## Версия 2.24.0 {#2-24-0}

Дата выхода 23 июля 2025. Для обновления до версии **2.24.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена возможность для [команд](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload tpch` и `{{ ydb-cli }} workload tpcds` использовать опцию `--scale` c дробным значением, что позволяет задавать долю от полного объёма данных и нагрузки бенчмарка.
* Добавлена команда `{{ ydb-cli }} workload tpcc check` для проверки целостности данных TPC-C.

### Улучшения

* Тип хранения по умолчанию [в командах](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload * init` изменён на `column` (было `row`), а режим работы с датой и временем по умолчанию — на `datetime32` (было `datetime64`).

### Исправления ошибок

* Исправлена проблема, из-за которой [команда](./reference/ydb-cli/export-import/import-file.md) `{{ ydb-cli }} import file csv` могла зависать.

## Версия 2.23.0 {#2-23-0}

Дата выхода 16 июля 2025. Для обновления до версии **2.23.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена команда `{{ ydb-cli }} workload tpcc` для проведения нагрузочного тестирования TPC-C.
* Добавлена команда `{{ ydb-cli }} workload vector select` для тестирования производительности и полноты векторного индекса.
* Добавлена команда `{{ ydb-cli }} tools infer csv` для генерации SQL-запроса `CREATE TABLE` на основе CSV-файла с данными.

### Улучшения

* Расширена обработка специальных значений (`null`, `/dev/null`, `stdout`, `cout`, `console`, `stderr`, `cerr`) для опции `--output` в [командах](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload * run`.
* [Команды](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload` теперь работают с абсолютными путями до объектов схемы в базе данных.
* Улучшения в [интерактивном режиме выполнения запросов](./reference/ydb-cli/interactive-cli.md) `{{ ydb-cli }}`:
  * Добавлена проверка соединения с сервером и описание горячих клавиш.
  * Улучшены inline-подсказки.
  * Добавлено автодополнение имён столбцов таблицы.
  * Добавлено кеширование схемы таблиц.

### Исправления ошибок

* Исправлена ошибка, из-за которой [команда](./reference/ydb-cli/export-import/tools-restore.md) `{{ ydb-cli }} tools restore` не работала на Windows.

## Версия 2.22.1 {#2-22-1}

Дата выхода 17 июня 2025. Для обновления до версии **2.22.1** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Исправления ошибок

* Исправлена ошибка, из-за которой сертификат не читался из файла, если путь к файлу указан в [профиле](./reference/ydb-cli/profile/index.md) в поле `ca-file`.
* Исправлена ошибка, из-за которой [команды](./reference/ydb-cli/workload-click-bench.md) `{{ ydb-cli }} workload query import` и `{{ ydb-cli }} workload clickbench import files` в состоянии отображали количество строк вместо количества байт.

## Версия 2.22.0 {#2-22-0}

Дата выхода 4 июня 2025. Для обновления до версии **2.22.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлено автодополнение имён схемных объектов в интерактивном режиме.
* Расширены возможности [команды](./reference/ydb-cli/workload-query.md) `{{ ydb-cli }} workload query`: добавлены команды `{{ ydb-cli }} workload query init`, `{{ ydb-cli }} workload query import` и `{{ ydb-cli }} workload query clean` и изменена команда `{{ ydb-cli }} workload query run`. Пользуясь ими можно инициализировать таблицы, заполнить их данными, провести нагрузочное тестирование и очистить данные за собой.
* В [команды](./reference/ydb-cli/workload-click-bench.md) `{{ ydb-cli }} workload clickbench run`, `{{ ydb-cli }} workload tpch run`, `{{ ydb-cli }} workload tpcds run` добавлена опция `--threads`, позволяющая указывать количество потоков для отправки запросов.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Добавлена [команда](./reference/ydb-cli/commands/configuration/cluster/index.md#list) `{{ ydb-cli }} admin cluster config version` для отображения версии конфигурации (V1/V2) на узлах.

### Изменения с потерей обратной совместимости

* Из команд [`{{ ydb-cli }} workload * run`](./reference/ydb-cli/commands/workload/index.md) удалена опция `--executor`. Теперь всегда используется исполнитель `generic`.

### Исправления ошибок

* Исправлена ошибка, из-за которой команды [`{{ ydb-cli }} workload * clean`](./reference/ydb-cli/commands/workload/index.md) удаляли все содержимое целевой директории, а не только таблицы, созданные командой init.

## Версия 2.21.0 {#2-21-0}

Дата выхода 22 мая 2024. Для обновления до версии **2.21.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлен [глобальный параметр](./reference/ydb-cli/commands/global-options.md) `--no-discovery`, позволяющий пропустить процесс discovery и подключиться напрямую к указанному пользователем эндпоинту.
* Добавлены новые опции для команд нагрузочного тестирования:
  * Добавлена опция `--scale` в [команды](./reference/ydb-cli/workload-tpch.md) `{{ ydb-cli }} workload tpch init` и `{{ ydb-cli }} workload tpcds init` для установки процента размера данных и нагрузки относительно максимальной нагрузки.
  * Добавлена опция `--retries` в [команды](./reference/ydb-cli/workload-click-bench.md) `{{ ydb-cli }} workload <clickbench|tpch|tpcds> run` для указания максимального количества повторов каждого запроса.
  * Добавлена опция `--partition-size` в [команды](./reference/ydb-cli/workload-click-bench.md) `{{ ydb-cli }} workload <clickbench|tpcds|tpch> init` для установки максимального размера партиции в мегабайтах для строчных таблиц.
  * Добавлены параметры диапазона дат (`--date-to`, `--date-from`) в операции `{{ ydb-cli }} workload log run` для поддержки равномерного распределения первичных ключей.
* Улучшена функциональность резервного копирования и восстановления:
  * Добавлены опции `--replace` и `--verify-existence` в [команду](./reference/ydb-cli/export-import/tools-restore.md#schema-objects) `{{ ydb-cli }} tools restore` для управления удалением существующих объектов, совпадающих с объектами в резервной копии, перед восстановлением.
  * Улучшена [команда](./reference/ydb-cli/export-import/tools-dump.md#schema-objects) `{{ ydb-cli }} tools dump`: {% if feature_async_replication %}[таблицы-реплики](./concepts/async-replication.md){% else %}таблицы-реплики{% endif %} у ASYNC REPLICATION и их [потоки изменений](./concepts/glossary.md#changefeed) не сохраняются в локальные резервные копии. Это предотвращает дублирование потоков изменений и уменьшает размер резервной копии на диске.
* Изменения, повышающие удобство использования CLI:
  * Вывод подробной справки (`-hh`) теперь показывает всё дерево подкоманд.
  * Добавлена автоматическая вставка парных скобок в интерактивном режиме `{{ ydb-cli }}`.
  * Добавлена поддержка файлов с BOM (Byte Order Mark) в [командах](./reference/ydb-cli/export-import/import-file.md) `{{ ydb-cli }} import file`.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Улучшена команда `{{ ydb-cli }} debug latency`:
  * Добавлен параметр `--min-inflight` для установки минимального количества одновременных запросов (по умолчанию: 1).
  * Добавлена опция `--percentile` для указания пользовательских процентилей задержки.
  * Вывод команды расширен дополнительными измерениями GRPC ping.

### Исправления ошибок

* [Команда](./reference/ydb-cli/operation-get.md) `{{ ydb-cli }} operation get` теперь корректно отображает операции, которые ещё находятся в процессе выполнения.
* Исправлены ошибки в [команде](./reference/ydb-cli/commands/dir.md#rmdir) `{{ ydb-cli }} scheme rmdir`:
  * Исправлена ошибка, из-за которой команда пыталась удалить поддомены.
  * Исправлен порядок удаления: внешние таблицы теперь удаляются перед источниками данных из-за возможных зависимостей между ними.
  * Добавлена поддержка coordination nodes при рекурсивном удалении.
* Исправлен код возврата команды `{{ ydb-cli }} workload * run --check-canonical` при несовпадении результатов с каноническими.
* Исправлена проблема, когда CLI пытался читать параметры из stdin даже при отсутствии данных.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Исправлена ошибка авторизации при выполнении [команды](./reference/ydb-cli/export-import/tools-restore.md#db) `{{ ydb-cli }} admin database restore` при наличии нескольких учетных записей администраторов базы данных в восстанавливаемой резервной копии.

## Версия 2.20.0 {#2-20-0}

Дата выхода 5 марта 2024. Для обновления до версии **2.20.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена поддержка [топиков](./concepts/datamodel/topic.md) при выполнении [команд](./reference/ydb-cli/export-import/tools-dump.md) `{{ ydb-cli }} tools dump` и `{{ ydb-cli }} tools restore`.
* Добавлена поддержка [узлов координации](./concepts/datamodel/coordination-node.md) при выполнении [команд](./reference/ydb-cli/export-import/tools-dump.md) `{{ ydb-cli }} tools dump` и `{{ ydb-cli }} tools restore`.
* Добавлена новая команда `{{ ydb-cli }} workload log import generator`.
* Добавлены новые глобальные опции для пользовательских сертификатов при соединении через SSL/TLS:
  * `--client-cert-file`: файл, содержащий пользовательский сертификат для SSL/TLS соединения, закодированный в PEM или PKCS#12.
  * `--client-cert-key-file`: файл, содержащий приватный ключ к пользовательскому сертификату, закодированный в PEM.
  * `--client-cert-key-password-file`: файл, содержащий пароль для приватного ключа пользовательского сертификата.
* Запросы при выполнении команды `{{ ydb-cli }} workload run` теперь отправляются на сервер в произвольном порядке.
* **_(Требуется сервер v25.1+)_** Добавлена поддержка [внешних источников данных](./concepts/datamodel/external_data_source.md) и [внешних таблиц](./concepts/datamodel/external_table.md) при выполнении [команд](./reference/ydb-cli/export-import/tools-dump.md) `{{ ydb-cli }} tools dump` и `{{ ydb-cli }} tools restore`.
* **_(Экспериментально)_** Добавлена команда `{{ ydb-cli }} admin node config init` для инициализации директории с конфигурационными файлами узла.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Добавлена [команда](./reference/ydb-cli/commands/configuration/cluster/generate.md) `{{ ydb-cli }} admin cluster config generate` для генерации файла [конфигурации V2](./devops/configuration-management/configuration-v2/index.md) из [файла конфигурации V1](./devops/configuration-management/configuration-v2/index.md).
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Добавлены [команда](./reference/ydb-cli/export-import/tools-dump#cluster) `{{ ydb-cli }} admin cluster dump` и [команда](./reference/ydb-cli/export-import/tools-restore#cluster) `{{ ydb-cli }} admin cluster restore` для создания дампа кластера. Дамп кластера содержит список баз данных с метаданными, пользователей и группы, но не содержит схемные объекты.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Добавлены команды `{{ ydb-cli }} admin database dump` и `{{ ydb-cli }} admin database restore` для создания дампа базы данных. Такой дамп содержит метаданные базы данных, схемные объекты, данные в них, пользователей и группы.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Для команды `{{ ydb-cli }} admin cluster config fetch` добавлены новые опции `--dedicated-storage-section` и `--dedicated-cluster-section`, позволяющие получать части конфигурации для кластера и хранилища отдельно.

### Исправления ошибок

* Исправлена ошибка, из-за которой дважды отправлялся запрос аутентификации в команде `{{ ydb-cli }} auth get-token` при получении списка эндпойнтов (Discovery запрос) и при фактическом выполнении запроса на получение токена.
* Исправлена ошибка в команде `{{ ydb-cli }} import file csv`, при которой прогресс импорта сохранялся даже если отправка пакета данных завершилась ошибкой.
* Исправлена ошибка, из-за которой при выполнении команды `{{ ydb-cli }} tools restore` некоторые ошибки игнорировались.
* Исправлена утечка памяти при генерации данных для `{{ ydb-cli }} workload tpcds`.

## Версия 2.19.0 {#2-19-0}

Дата выхода 5 февраля 2025. Для обновления до версии **2.19.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена поддержка [потоков изменений (changefeeds)](./concepts/glossary.md#changefeed) при выполнении [команд](./reference/ydb-cli/export-import/tools-dump.md) `{{ ydb-cli }} tools dump` и `{{ ydb-cli }} tools restore`.
* Добавлена рекомендация с текстом `CREATE TABLE` при схемной ошибке во время выполнения [команды](./reference/ydb-cli/export-import/import-file.md) `{{ ydb-cli }} import file csv`.
* Добавлен вывод статистики для текущего процесса при выполнении [команды](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload`.
* Добавлен текст запроса к сообщению, если запрос завершился ошибкой при выполнении [команды](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload run`.
* Добавлено сообщение в случае ошибки истечения глобального таймаута при выполнении [команды](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload run`.

{% if feature_view %}

* **_(Требуется сервер v25.1+)_** Добавлена поддержка [представлений (VIEW)](./concepts/datamodel/view.md) при выполнении операций `{{ ydb-cli }} export s3` и `{{ ydb-cli }} import s3`. Представления экспортируются как YQL-выражение `CREATE VIEW`, которое выполняется при импорте.

{% endif %}

* **_(Требуется сервер v25.1+)_** Добавлена опция `--skip-checksum-validation` для [команды](./reference/ydb-cli/export-import/import-s3.md) `{{ ydb-cli }} import s3`, позволяющая отключить валидацию контрольной суммы на стороне сервера.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Для команды `{{ ydb-cli }} debug ping` добавлены новые опции: `--chain-length`, `--chain-work-duration`, `--no-tail-chain`.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Для команды `{{ ydb-cli }} admin storage fetch` добавлены новые опции: `--dedicated-storage-section` и `--dedicated-cluster-section`.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Для команды `{{ ydb-cli }} admin storage replace` добавлены новые опции: `--filename`, `--dedicated-cluster-yaml`, `--dedicated-storage-yaml`, `--enable-dedicated-storage-section` и `--disable-dedicated-storage-section`.

### Исправления ошибок

* Исправлена ошибка, из-за которой [команда](./reference/ydb-cli/commands/service.md) `{{ ydb-cli }} update` в arm64-версии исполняемого файла YDB CLI скачивала и заменяла себя исполняемым файлом amd64-версии. Чтобы обновить ранее установленный YDB CLI до последней arm64-версии (а не amd64), его нужно переустановить.
* [Команда](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload run` теперь возвращает корректный код возврата.
* Исправлена ошибка, из-за которой [команды](./reference/ydb-cli/workload-tpch.md) `{{ ydb-cli }} workload tpch import generator` и `{{ ydb-cli }} workload tpcds import generator` завершались с ошибкой из-за отсутствия необходимых таблиц в схеме.
* Исправлена ошибка с обратными слешами при указании путей в [команде]](./reference/ydb-cli/commands/workload/index.md) `{{ ydb-cli }} workload` на Windows.

## Версия 2.18.0 {#2-18-0}

Дата выхода 24 декабря 2024. Для обновления до версии **2.18.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена поддержка [представлений (VIEW)](./concepts/datamodel/view) при выполнении операций резервного копирования `{{ ydb-cli }} tools dump` и восстановления `{{ ydb-cli }} tools restore`. Представления сохраняются в файл "create_view.sql" в виде запросов `CREATE VIEW`, которые будут выполнены для восстановления.
* В [команду](./reference/ydb-cli/workload-topic#run-write) `{{ ydb-cli }} workload topic run` добавлены опции `--tx-commit-interval` и `--tx-commit-messages`, которые задают интервал между коммитами транзакций в миллисекундах и в количестве записанных сообщений соответственно.
* В [команде](./reference/ydb-cli/topic-read) `{{ ydb-cli }} topic read` параметр `--consumer` перестал быть обязательным. В режиме чтения без подписчика обязательно должны быть указаны идентификаторы партиций с помощью параметра `--partition-ids`. Чтение в этом случае выполняется без сохранения коммита оффсетов.
* [Команда](./reference/ydb-cli/export-import/import-file.md) `{{ ydb-cli }} import file csv` теперь сохраняет прогресс выполнения. Повторный запуск команды импорта продолжится с той строки, на которой она была прервана.
* В командах `{{ ydb-cli }} workload kv` и `{{ ydb-cli }} workload stock` значение параметра `--executer` по умолчанию изменено на "generic", благодаря чему они больше не используют устаревшую инфраструктуру выполнения запросов.
* Изменен формат загрузки данных в таблицы для нагрузочных тестов `{{ ydb-cli }} workload` с CSV на Parquet.
* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Добавлена команда `{{ ydb-cli }} admin storage` с подкомандами `fetch` и `replace` для управления конфигурацией хранилища сервера.

### Изменения с потерей обратной совместимости

* В команде `{{ ydb-cli }} workload * run` параметр `--query-settings` заменен на `--query-prefix`.

### Исправления ошибок

* Исправлена ошибка, из-за которой команда `{{ ydb-cli }} workload * run` в режиме `--dry-run` могла приводить к сбою.
* Исправлена ошибка в `{{ ydb-cli }} import file csv`, из-за которой несколько столбцов с экранированными кавычками в одной строке обрабатывались неправильно.

## Версия 2.17.0 {#2-17-0}

Дата выхода 4 декабря 2024. Для обновления до версии **2.17.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* **_(Требуется сервер v25.1+)_** **_(Экспериментально)_** Добавлена команда `{{ ydb-cli }} debug ping` для проверки производительности и связанности.

### Производительность

* Улучшена производительность [параллельного восстановления из файловой системы](./reference/ydb-cli/export-import/tools-restore.md) с помощью команды `{{ ydb-cli }} tools restore`.

### Исправления ошибок

* Исправлена ошибка в схеме таблиц, созданных командой `{{ ydb-cli }} workload tpch`, из-за которой таблица `partsupp` содержала неверный список ключевых столбцов.
* Исправлена ошибка, из-за которой команда `{{ ydb-cli }} tools restore` завершалась с ошибкой `Too much data`, если было установлено максимальное значение параметра `--upload-batchbytes` (16MB).

## Версия 2.16.0 {#2-16-0}

Дата выхода 26 ноября 2024. Для обновления до версии **2.16.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Увеличена пропускная способность команды `{{ ydb-cli }} import file csv` примерно в 3 раза.
* Добавлена поддержка [stock-нагрузки](./reference/ydb-cli/commands/workload/stock.md) для [колоночных таблиц](./concepts/datamodel/table.md#column-oriented-tables).
* Реализована поддержка временных меток в формате [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601) для команд `{{ ydb-cli }} topic`.
* В команду `{{ ydb-cli }} sql` добавлена опция `--explain-ast`, которая выводит AST запроса.
* Добавлена подсветка синтаксиса ANSI SQL в интерактивном режиме.
* В команды `{{ ydb-cli }} workload tpch` и `{{ ydb-cli }} workload tpcds` добавлена поддержка синтаксиса PostgreSQL.
* В команду `{{ ydb-cli }} workload tpcds run` добавлена опция `-c` для сравнения результата с ожидаемым значением и отображения различий.
* В команды `{{ ydb-cli }} tools dump` и `{{ ydb-cli }} tools restore` добавлено логирование событий.
* В команду `{{ ydb-cli }} tools restore` добавлено указание места возникновения ошибки.

### Изменения с потерей обратной совместимости

* В команде `{{ ydb-cli }} topic write` для опции `--codec` значение по умолчанию изменено на `RAW`.

### Исправления ошибок

* Исправлен progress bar для команды `{{ ydb-cli }} workload import`.
* Устранена ошибка восстановления из резервной копии с использованием опции `--import-data`, возникавшая при изменении партиционирования таблицы.

## Версия 2.10.0 {#2-10-0}

Дата выхода 24 июня 2024. Для обновления до версии **2.10.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена команда `{{ ydb-cli }} sql`, работающая поверх QueryService, позволяющая выполнять любые DML/DDL команды.
* Добавлен режим `notx` для опции `--tx-mode` в команде `{{ ydb-cli }} table query execute`.
* Добавлены времена начала и конца в описании длительных операций (export, import).
* Добавлена поддержка описания объектов типа replication в командах `{{ ydb-cli }} scheme describe` и `{{ ydb-cli }} scheme ls`.
* Добавлена поддержка типов big datetime: `Date32`, `Datetime64`, `Timestamp64`, `Interval64`.
* Переработана команда `{{ ydb-cli }} workload`:

  * Добавлена опция `--clear` в подкоманде `init`, позволяющая удалить все существующие таблицы перед созданием новых.
  * Добавлена команда `{{ ydb-cli }} workload * import` для заполнения таблиц начальным контентом перед началом нагрузки.

### Изменения с потерей обратной совместимости

* Переработана команда `{{ ydb-cli }} workload`:

  * Опция `--path` перемещена на уровень конкретного типа нагрузки. Например: `{{ ydb-cli }} workload tpch --path some/tables/path init ...`.
  * Значение опции `--store=s3` переименовано в `--store=external-s3` в подкоманде `init`.

### Исправления ошибок

* Исправлена работа с цветовыми схемами в формате `PrettyTable`

## Версия 2.9.0 {#2-9-0}

Дата выхода 25 апреля 2024. Для обновления до версии **2.9.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Улучшены таблицы с логическими планами запросов: стали информативнее, добавлены цвета, исправлены некоторые ошибки.
* Для команды `{{ ydb-cli }} workload` поддержана опция `-v`, включающая вывод отладочной информации.
* Добавлена возможность запустить `{{ ydb-cli }} workload tpch --store s3` с источником s3 для измерения производительности федеративных запросов.
* Добавлена опция `--rate` для команды `{{ ydb-cli }} workload` для ограничения количества транзакций (запросов) в секунду.
* Добавлена опция `--use-virtual-addressing` для импорта/экспорта s3, позволяющая переключить режим [virtual hosting of buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) для схемы путей s3.
* Улучшена производительность команды `{{ ydb-cli }} scheme ls` параллельным запуском листингов директорий.

### Исправления ошибок

* Исправлено обрезание лишних символов при переносе строк в таблицах CLI
* Исправлена ошибка обращения к памяти в команде `tools restore`.
* Исправлено игнорирование опции `--timeout` в generic и scan запросах.
* Добавлен таймаут 60s на проверку версии и скачивание бинарного файла CLI для избежания бесконечного ожидания.
* Исправлен ряд незначительных ошибок: опечатки, обработка пустых файлов и т.д.

## Версия 2.8.0 {#2-8-0}

Дата выхода 12 января 2024. Для обновления до версии **2.8.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлены команды управления конфигурациями кластера [ydb admin config](reference/ydb-cli/configs.md) и [ydb admin volatile-config](reference/ydb-cli/configs.md).
* Добавлена поддержка загрузки PostgreSQL-совместимых типов командой [ydb import file csv|tsv|json](reference/ydb-cli/export-import/import-file.md). Только для строковых таблиц.
* Добавлена поддержка загрузки директории из S3-совместимого хранилища в команде [ydb import s3](reference/ydb-cli/export-import/import-s3.md). Пока доступна только под Linux и Mac OS.
* Добавлена поддержка вывода результата выполнения команд [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb yql](reference/ydb-cli/yql.md) и [ydb scripting yql](reference/ydb-cli/scripting-yql.md) в формате [Apache Parquet](https://parquet.apache.org/docs/).
* В командах [ydb workload](reference/ydb-cli/commands/workload/index.md) добавлена опция `--executer`, задающая используемый тип запросов.
* Добавлена колонка медианного времени выполнения бенчмарка в таблице статистики в команде [ydb workload clickbench](reference/ydb-cli/workload-click-bench.md).
* **_(Experimental)_** Добавлен тип запросов `generic` в команде [ydb table query execute](reference/ydb-cli/table-query-execute.md), позволяющий выполнять [DDL](https://ru.wikipedia.org/wiki/Data_Definition_Language) и [DML](https://ru.wikipedia.org/wiki/Data_Manipulation_Language) операции, с результатами произвольного размера и c поддержкой [MVCC](concepts/query_execution/mvcc.md). Команда использует экспериментальное API, совместимость не гарантируется.
* **_(Experimental)_** В команде `{{ ydb-cli }} table query explain` добавлена опция `--collect-diagnostics` для сбора диагностики запроса и сохранения её в файл. Команда использует экспериментальное API, совместимость не гарантируется.

### Исправления ошибок

* Исправлена ошибка вывода таблиц в `pretty` формате с [Unicode](https://ru.wikipedia.org/wiki/Юникод) символами.
* Исправлена ошибка подстановки неправильного первичного ключа в команде [ydb tools pg-convert](postgresql/import.md#pg-convert).

## Версия 2.7.0 {#2-7-0}

Дата выхода 23 октября 2023. Для обновления до версии **2.7.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена команда [ydb tools pg-convert](postgresql/import.md#pg-convert), выполняющая подготовку дампа, полученного утилитой [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html), к загрузке в postgres-совместимую прослойку YDB.
* Добавлена команда нагрузочного тестирования `{{ ydb-cli }} workload query`, которая нагружает базу [запросами выполнения скрипта](reference/ydb-cli/yql.md) в несколько потоков.
* Добавлена команда для просмотра списка разрешений `{{ ydb-cli }} scheme permissions list`.
* В командах [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb table query explain](reference/ydb-cli/commands/explain-plan.md), [ydb yql](reference/ydb-cli/yql.md) и [ydb scripting yql](reference/ydb-cli/scripting-yql.md) добавлена опция `--flame-graph`, задающая путь до файла, в котором необходимо сохранить визуализацию статистики выполнения запросов.
* [Специальные команды](reference/ydb-cli/interactive-cli.md#spec-commands) интерактивного режима выполнения запросов теперь не чувствительны к регистру.
* Добавлена валидация [специальных команд](reference/ydb-cli/interactive-cli.md#spec-commands) и их [параметров](reference/ydb-cli/interactive-cli.md#internal-vars).
* Добавлено чтение из таблицы в сценарии с транзакциями в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).
* Добавлена опция `--commit-messages` в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), задающая число сообщений в одной транзакции.
* Добавлены опции `--only-table-in-tx` и `--only-topic-in-tx` в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), задающие ограничения на виды запросов в одной транзакции.
* Добавлены новые колонки `Select time` и `Upsert time` в таблице статистики в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).

### Исправления ошибок

* Исправлена ошибка при загрузке пустого JSON списка командами [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb scripting yql](reference/ydb-cli/scripting-yql.md) и [ydb yql](reference/ydb-cli/yql.md).

## Версия 2.6.0 {#2-6-0}

Дата выхода 7 сентября 2023. Для обновления до версии **2.6.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* В [ydb workload tpch run](reference/ydb-cli/workload-tpch.md#run) добавлена опция `--path`, содержащая путь до директории с таблицами, созданными командой [ydb workload tpch init](reference/ydb-cli/workload-tpch.md#init).
* Добавлена команда [ydb workload transfer topic-to-table](reference/ydb-cli/workload-transfer.md), которая нагружает базу запросами на чтение из топиков и запись в таблицу.
* Добавлена опция `--consumer-prefix` в командax [ydb workload topic init](reference/ydb-cli/workload-topic.md#init), [ydb workload topic run read|full](reference/ydb-cli/workload-topic.md#run-read), задающая префиксы имен читателей.
* Добавлена опция `--partition-ids` в команде [ydb topic read](reference/ydb-cli/topic-read.md), задающая список id партиций топика для чтения, разделенных запятой.
* Добавлена поддержка форматов параметров CSV и TSV в командах исполнения [YQL запросов](reference/ydb-cli/parameterized-queries-cli.md).
* Переработан [интерактивный режим выполнения запросов](reference/ydb-cli/interactive-cli.md). Добавлены [новые специфичные команды интерактивного режима](reference/ydb-cli/interactive-cli.md#spec-commands): `SET stats`, `EXPLAIN`, `EXPLAIN AST`. Добавлены сохранение истории между запусками CLI и автодополнение YQL запросов.
* Добавлена команда [ydb config info](reference/ydb-cli/commands/config-info.md), которая выводит текущие параметры соединения без подключения к базе данных.
* Добавлена команда [ydb workload kv run mixed](reference/ydb-cli/workload-kv.md#mixed-kv), которая нагружает базу запросами на запись и чтение.
* Опция `--percentile` в командах [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write) теперь может принимать вещественные значения.
* Увеличены значения по умолчанию для опций `--seconds` и `--warmup` в командах [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write) до 60 секунд и 5 секунд соответственно.
* Изменено значение по умолчанию для опции `--supported-codecs` на `RAW` в командах [ydb topic create](reference/ydb-cli/topic-create.md) и [ydb topic consumer add](reference/ydb-cli/topic-consumer-add.md).

### Исправления ошибок

* Исправлена потеря строк при загрузке командой [ydb import file json](reference/ydb-cli/export-import/import-file.md).
* Исправлен неучет статистики во время прогрева команд [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write).
* Исправлен неполный вывод статистики в командах [ydb scripting yql](reference/ydb-cli/scripting-yql.md) и [ydb yql](reference/ydb-cli/yql.md).
* Исправлен неправильный вывод progress bar'a в командах [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md) и [ydb tools restore](reference/ydb-cli/export-import/tools-restore.md).
* Исправлена ошибка загрузки больших файлов с заголовком в команде [ydb import file csv|tsv](reference/ydb-cli/export-import/import-file.md).
* Исправлено зависание команды [ydb tools restore --import-data](reference/ydb-cli/export-import/tools-restore.md#optional).
* Исправлена ошибка `Unknown value Rejected` при выполнении команды [ydb operation list buildindex](reference/ydb-cli/operation-list.md).

## Версия 2.5.0 {#2-5-0}

Дата выхода 20 июня 2023. Для обновления до версии **2.5.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Для команды `{{ ydb-cli }} import file` добавлен параметр [--timeout](reference/ydb-cli/export-import/import-file.md#optional), задающий время, в течение которого должна быть выполнена операция на сервере.
* Добавлен индикатор прогресса в командах [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir) и [ydb import file](reference/ydb-cli/export-import/import-file.md).
* Добавлена команда [ydb workload kv run read-rows](reference/ydb-cli/workload-kv.md#read-rows-kv), которая нагружает базу запросами на чтение строк, используя новый экспериментальный API вызов ReadRows (реализован только в ветке [main](https://github.com/ydb-platform/ydb)), выполняющий более быстрое чтение по ключу, чем [select](reference/ydb-cli/workload-kv.md#select-kv).
* В [ydb workload topic](reference/ydb-cli/workload-topic.md) добавлены новые параметры `--warmup-time`, `--percentile`, `--topic`, задающие время прогрева теста, процентиль в выводе статистики и имя топика соответственно.
* Добавлена команда [ydb workload tpch](reference/ydb-cli/workload-tpch.md) для запуска нагрузочного теста TPC-H.
* Добавлен флаг `--ordered` в команде [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md), сохраняющий порядок по первичному ключу в таблицах.

### Производительность

* Увеличена скорость загрузки данных в команде `{{ ydb-cli }} import file` за счет добавления параллельной загрузки. Число потоков задается новым параметром [--threads](reference/ydb-cli/export-import/import-file.md#optional).
* Увеличена производительность команды [ydb import file json](reference/ydb-cli/export-import/import-file.md), за счет уменьшения числа копирований данных.

## Версия 2.4.0 {#2-4-0}

Дата выхода 24 мая 2023. Для обновления до версии **2.4.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена возможность загрузки нескольких файлов командой [ydb import file](reference/ydb-cli/export-import/import-file.md).
* Добавлена поддержка удаления колоночных таблиц для команды [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir).
* Повышена стабильность работы команды [ydb workload topic](reference/ydb-cli/workload-topic.md).

## Версия 2.3.0 {#2-3-0}

Дата выхода 1 мая 2023. Для обновления до версии **2.3.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлен интерактивный режим выполнения запросов. Для перехода в интерактивный режим выполните команду [ydb yql](reference/ydb-cli/yql.md) без аргументов. Режим экспериментальный, обратная совместимость пока не гарантируется.
* Добавлена команда [ydb index rename](reference/ydb-cli/commands/secondary_index.md#rename) для [атомарной замены](dev/secondary-indexes.md#atomic-index-replacement) или переименования вторичного индекса.
* Добавлена команда `{{ ydb-cli }} workload topic` для запуска нагрузки, которая читает и записывает сообщения в топики.
* Для команды `{{ ydb-cli }} scheme rmdir` добавлен параметр [--recursive](reference/ydb-cli/commands/dir.md#rmdir-options), который позволяет рекурсивно удалить директорию вместе со всем содержимым.
* Для команды [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) добавлена поддержка типов `topic` и `coordination node`.
* Для команды `{{ ydb-cli }} topic consumer` добавлен параметр [--commit](reference/ydb-cli/topic-read.md#osnovnye-opcionalnye-parametry) для подтверждения прочитанных сообщений.
* Для команды `{{ ydb-cli }} import file csv|tsv` добавлен параметр [--columns](reference/ydb-cli/export-import/import-file.md#optional), с помощью которого можно указать список колонок вместо заголовка в файле.
* Для команды `{{ ydb-cli }} import file csv|tsv` добавлен параметр [--newline-delimited](reference/ydb-cli/export-import/import-file.md#optional), который подтверждает отсутствие символа переноса строки в данных. Использование этого параметра ускоряет импорт за счет параллельного чтения из нескольких секций файла.

### Исправления ошибок

* Исправлена ошибка, которая приводила к повышенному потреблению памяти и процессора при выполнении команды `{{ ydb-cli }} import file`.

## Версия 2.2.0 {#2-2-0}

Дата выхода 3 марта 2023. Для обновления до версии **2.2.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Исправлена ошибка, когда невозможно было указать поддерживаемые алгоритмы сжатия при добавлении читателя топика.
* Добавлена поддержка потокового выполнения YQL-скриптов и запросов на основе параметров, [передаваемых через `stdin`](reference/ydb-cli/parameterized-queries-cli.md).
* Значения параметров YQL-запросов теперь могут быть [переданы из файла](reference/ydb-cli/parameterized-queries-cli.md).
* Запрос на ввод пароля теперь выводится в `stderr` вместо `stdout`.
* Путь к корневому CA сертификату теперь может быть сохранен в [профиле](reference/ydb-cli/profile/index.md).
* Добавлен глобальный параметр [--profile-file](reference/ydb-cli/commands/global-options.md#service-options) для использования указанного файла в качестве хранилища для настроек профилей.
* Добавлен новый тип нагрузочного тестирования [ydb workload clickbench](reference/ydb-cli/workload-click-bench).

## Версия 2.1.1 {#2-1-1}

Дата выхода 30 декабря 2022. Для обновления до версии **2.1.1** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Улучшения

* Добавлена поддержка параметра `--stats` команды [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) для колоночных таблиц.
* Добавлена поддержка файлов в формате Parquet для импорта командой [ydb import](reference/ydb-cli/export-import/import-file.md).
* Поддержаны дополнительное логирование и ретраи для команды [ydb import](reference/ydb-cli/export-import/import-file.md).

## Версия 2.1.0 {#2-1-0}

Дата выхода 18 ноября 2022. Для обновления до версии **2.1.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена возможность [неинтерактивного создания профиля](reference/ydb-cli/profile/create.md#cmdline).
* Добавлены команды [ydb config profile update](reference/ydb-cli/profile/create.md#update) и [ydb config profile replace](reference/ydb-cli/profile/create.md#replace) для изменения и замены профилей.
* Для команды [ydb scheme ls](reference/ydb-cli/commands/scheme-ls.md) добавлен параметр `-1`, включающая режим вывода по одному объекту на строку.
* URL сервиса IAM теперь можно сохранять в профиле.
* Добавлена возможность использовать аутентификацию по логину и паролю без указания пароля.
* Добавлена поддержка профилей AWS в команде [ydb export s3](reference/ydb-cli/export-import/auth-s3.md#auth).
* Добавлена возможность создания профиля используя `stdin`. Например, можно передать вывод команды [YC CLI](https://cloud.yandex.ru/docs/cli/) `yc ydb database get information` на вход команде `{{ ydb-cli }} config profile create`.

### Исправления ошибок

* Исправлена ошибка, когда некорректно выводился результат запроса в формате JSON-array, если он состоял из нескольких ответов сервера.
* Исправлена ошибка, приводящая к невозможности изменить профиль, используя при этом некорректный профиль.

## Версия 2.0.0 {#2-0-0}

Дата выхода 20 сентября 2022. Для обновления до версии **2.0.0** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена возможность работы с топиками:

  * `{{ ydb-cli }} topic create` — создание топика;
  * `{{ ydb-cli }} topic alter` — изменение топика;
  * `{{ ydb-cli }} topic write` — запись данных в топик;
  * `{{ ydb-cli }} topic read` — чтение данных из топика;
  * `{{ ydb-cli }} topic drop` — удаление топика.

* Добавлен новый тип нагрузочного тестирования:

  * `{{ ydb-cli }} workload kv init` — создание таблицы для тестирования kv нагрузки;
  * `{{ ydb-cli }} workload kv run` — запуск одной из 3 видов нагрузки: запуск нескольких сессий вставки `UPSERT`, запуск нескольких сессий вставки `INSERT` или запуск нескольких сессий с GET-запросами по первичному ключу;
  * `{{ ydb-cli }} workload kv clean` — удаление тестовой таблицы.

* Добавлена возможность деактивировать текущий активный профиль (см. команду `{{ ydb-cli }} config profile deactivate`).
* Добавлена возможность неинтерактивного удаления профиля без подтверждения (см. параметр `--force` команды `{{ ydb-cli }} config profile remove`).
* Добавлена поддержка CDC для команды `{{ ydb-cli }} scheme describe`.
* Добавлена возможность просмотра текущего статуса БД (см. команду `{{ ydb-cli }} monitoring healthcheck`).
* Добавлена возможность просмотра аутентификационной информации (токена), с которой будут отправляться запросы к БД при текущих настройках аутентификации (см. команду `{{ ydb-cli }} auth get-token`).
* Добавлена возможность чтения данных из стандартного потока ввода для команды `{{ ydb-cli }} import`.
* Добавлена возможность импорта данных в формате JSON из файла или стандартного потока ввода (см. команду `{{ ydb-cli }} import file json`).

### Улучшения

* Улучшен процессинг команд. Парсинг и валидация пользовательского ввода теперь более точные.

## Версия 1.9.1 {#1-9-1}

Дата выхода 25 июня 2022. Для обновления до версии **1.9.1** перейдите в раздел [Загрузки](downloads/ydb-cli.md).

### Функциональность

* Добавлена возможность сжатия данных при экспорте в S3-совместимое хранилище (см. параметр `--compression` команды [ydb export s3](reference/ydb-cli/export-import/export-s3.md)).
* Добавлена возможность управления автоматической проверкой доступности новой версии {{ ydb-short-name }} CLI (см. параметры `--disable-checks` и `--enable-checks` команды [ydb version](reference/ydb-cli/version.md)).
