# Ключевые возможности для аналитики: быстрый справочник

Эта страница — карта документации по аналитическим возможностям {{ydb-short-name}}. Текст сгруппирован по этапам жизненного цикла данных, чтобы помочь быстро найти необходимую информацию для проектирования, разработки и эксплуатации аналитических решений.


## Проектирование хранилища данных (Concepts & Design)

Основы организации данных, масштабирования и управления.

### Основные концепции и типы данных

  - [Колоночные таблицы](../datamodel/table.md#column-oriented-tables): архитектура хранения, оптимизированная для OLAP.
  - [Типы данных](../../yql/reference/types/index.md): полный справочник по поддерживаемым типам.

### Масштабирование и производительность

  - [Проектирование ключей для максимальной производительности](../../dev/primary-key/column-oriented.md): как выбирать `PRIMARY KEY` и `PARTITION BY`.
  - [Партиционирование таблиц](../datamodel/table.md#olap-tables-partitioning): механизм распределения данных по узлам.

### Управление жизненным циклом данных

  - [TTL (Time-to-Live)](../ttl.md): автоматическое удаление устаревших данных по истечении срока.

## Загрузка и выгрузка данных (Ingestion & Egress)

Инструменты и API для перемещения данных в {{ydb-short-name}} и из неё.

### Потоковая загрузка (Streaming Ingestion)

  - [Topics (Kafka API)](../datamodel/topic.md): нативная работа с потоками данных через протокол Kafka.
  - [Transfer](../transfer.md): управляемый сервис для переноса данных между топиками и таблицами.
  - [Коннектор Fluent Bit](../../integrations/ingestion/fluent-bit.md): прямая загрузка логов.

### Пакетная загрузка (Batch Ingestion)

  - [Коннектор Apache Spark](../../integrations/ingestion/spark.md): чтение и запись данных для ETL/ELT-задач.
  - [BulkUpsert API](../../recipes/ydb-sdk/bulk-upsert.md): высокопроизводительная вставка больших объемов данных через SDK.

### Взаимодействие с внешними системами

  - [Федеративные запросы](../query_execution/federated_query/index.md): выполнение запросов к данным, находящимся во внешних системах (S3, ClickHouse, Postgres).
  - [Работа с S3 через внешние таблицы](../query_execution/federated_query/s3/external_table.md): чтение и запись данных в формате Parquet/CSV в Object Storage.

## Обработка и трансформация данных (ETL/ELT)

Язык запросов и интеграция с инструментами оркестрации.

### Язык запросов YQL

  - [Полный справочник по YQL](../../yql/reference/index.md): синтаксис, функции и операторы.
  - [Функции для работы с датой и временем](../../yql/reference/udf/list/datetime.md): полный список и типовые сценарии.
  - [Функции для работы с JSON](../../yql/reference/builtins/json.md): извлечение данных из JSON-документов.

### Инструменты для построения пайплайнов

   - [Интеграция с dbt (Data Build Tool)](../../integrations/migration/dbt.md): управление ELT-пайплайнами с помощью SQL.
   - [Интеграция с Apache Airflow](../../integrations/orchestration/airflow.md): оркестрация сложных ETL/ELT-процессов.

## Разработка и интеграция с приложениями (Development & SDKs)

Инструменты для разработчиков приложений.

  - [Обзор {{ydb-short-name}} SDK](../../reference/ydb-sdk/index.md): нативные SDK для Go, Python, Java, C++, Node.js.
  - [JDBC драйвер](../../reference/languages-and-apis/jdbc-driver/index.md): стандартный способ подключения из Java-экосистемы.
  - [{{ydb-short-name}} CLI](../../reference/ydb-cli/index.md): инструмент командной строки для администрирования и выполнения запросов.

## Анализ данных и визуализация (Analytics & BI)

Интеграция с инструментами для конечных пользователей.

### BI-системы

  - [Apache Superset](../../integrations/visualization/superset.md)
  - [Grafana](../../integrations/visualization/grafana.md)
  - [Yandex DataLens](../../integrations/visualization/datalens.md)

### Инструменты Data Science

  - [Jupyter Notebooks](../../integrations/gui/jupyter.md): выполнение YQL-запросов и анализ данных в интерактивном режиме.

## Эксплуатация и управление производительностью (Operations & Performance)

Администрирование, мониторинг, безопасность и оптимизация.

### Управление производительностью

  - [Анализ планов запросов (EXPLAIN)](../../dev/query-plans-optimization.md): как понять план выполнения запроса и найти узкие места.
  - [Управление нагрузкой (Resource Pools)](../../dev/resource-consumption-management.md): изоляция ресурсов CPU для разных команд или нагрузок.
  - [Стоимостной оптимизатор](../query_execution/optimizer.md): обзор принципов работы планировщика запросов.

### Мониторинг и диагностика

  - [Встроенный UI](../../reference/embedded-ui/index.md): веб-интерфейс для мониторинга состояния и диагностики кластера.
  - [Справочник по метрикам](../../reference/observability/metrics/index.md): полный список метрик для систем мониторинга.
  - [Готовые дашборды для Grafana](../../reference/observability/metrics/grafana-dashboards.md): шаблоны для быстрой настройки мониторинга.

### Безопасность и отказоустойчивость

  - [Аутентификация и авторизация](../../security/authentication.md): настройка доступа пользователей, в том числе через LDAP.

### Архитектурные ограничения

  - [Известные ограничения системы](../../analyst/limitations.md): важный раздел для понимания особенностей и компромиссов архитектуры.
