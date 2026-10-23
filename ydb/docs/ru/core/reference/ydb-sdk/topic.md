# Работа с топиками

В этой статье приведены примеры использования {{ ydb-short-name }} SDK для работы с [топиками](../../concepts/datamodel/topic.md).

Фрагменты ниже взяты из полных исполняемых примеров, ссылки на которые приведены в следующем разделе. Приложения создают топики и читателей с уникальными именами и удаляют их после выполнения сценариев. Настройка подключения и вспомогательные функции находятся в этих приложениях.

## Примеры работы с топиками

{% list tabs group=lang %}

- C++

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

- Go

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/8ad2661ef04180780d314fb0706ec3f8a7ccfc8d/examples/ydb_tech/topic)

- Java

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

- Python

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

- C#

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/783cfd6c1803bbd607577c5b59c4ae0cc9a5d9a7/examples/ydb_tech/topic)

- JavaScript

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-js-sdk/tree/e107f68250200a6ddfaa6074ccd1ec48ed1ae839/examples/ydb_tech/topic)

- Rust

  [Исполняемый пример на GitHub](https://github.com/ydb-platform/ydb-rs-sdk/tree/041636b79c6d607c6ce2e594b28bb510db514680/ydb/examples/ydb_tech/topic)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Инициализация соединения с топиками {#init}

{% list tabs group=lang %}

- C++

  Для работы с топиками создаются экземпляры драйвера {{ ydb-short-name }} и клиента.

  Драйвер {{ ydb-short-name }} отвечает за взаимодействие приложения и {{ ydb-short-name }} на транспортном уровне. Драйвер должен существовать на всем протяжении жизненного цикла работы с топиками и должен быть инициализирован перед созданием клиента.

  Клиент сервиса топиков ([исходный код](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1589)) работает поверх драйвера {{ ydb-short-name }} и отвечает за управляющие операции с топиками, а также создание сессий чтения и записи.

  Фрагмент кода приложения для инициализации драйвера {{ ydb-short-name }}:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_init]-[END topic_init]" %}

  В этом примере используется аутентификационный токен, сохранённый в переменной окружения `YDB_TOKEN`. Подробнее про [соединение с БД](../../concepts/connect.md) и [аутентификацию](../../security/authentication.md).

  Фрагмент кода приложения для создания клиента:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_client]-[END topic_client]" %}

- Go

  Для работы с топиками используется экземпляр драйвера {{ ydb-short-name }}, созданный с помощью `ydb.Open`. Клиент топиков доступен через метод `db.Topic()`.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_init]-[END topic_init]" %}

- Java

  Для работы с топиками создаются экземпляры транспорта {{ ydb-short-name }} и клиента.

  Транспорт {{ ydb-short-name }} отвечает за взаимодействие приложения и {{ ydb-short-name }} на транспортном уровне. Он должен существовать на всем протяжении жизненного цикла работы с топиками и должен быть инициализирован перед созданием клиента.

  Фрагмент кода приложения для инициализации транспорта {{ ydb-short-name }}:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_init]-[END topic_init]" %}

  В этом примере используется вспомогательный метод `CloudAuthHelper.getAuthProviderFromEnviron()`, получающий токен из переменных окружения.
  Например, `YDB_ACCESS_TOKEN_CREDENTIALS`.
  Подробнее про [соединение с БД](../../concepts/connect.md) и [аутентификацию](../../security/authentication.md).

  Клиент сервиса топиков ([исходный код](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/TopicClient.java#L34)) работает поверх транспорта {{ ydb-short-name }} и отвечает как за управляющие операции с топиками, так и за создание писателей и читателей.

  Фрагмент кода приложения для создания клиента:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_client]-[END topic_client]" %}

  В обоих примерах кода выше используется блок ([try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html)).
  Это позволяет автоматически закрывать клиент и транспорт при выходе из этого блока, т.к. оба являются наследниками `AutoCloseable`.

- C#

  Для работы с топиками достаточно передать строку подключения напрямую в конструктор нужного клиента.

  В этом примере используется анонимная аутентификация. Подробнее про [соединение с базой данных](../../concepts/connect.md) и [аутентификацию](../../security/authentication.md).

  Фрагмент кода приложения для создания различных клиентов к топикам:

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_init]-[END topic_init]" %}

- Python

  Для работы с топиками создаётся экземпляр драйвера {{ ydb-short-name }}. Клиент топиков доступен через атрибут `topic_client` и используется для управляющих операций с топиками, а также создания писателей и читателей.

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_init]-[END topic_init]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_init]-[END topic_init]" %}

  {% endlist %}

  Подробнее про [соединение с БД](../../concepts/connect.md) и [аутентификацию](../../security/authentication.md).

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_init]-[END topic_init]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_init]-[END topic_init]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Управление топиками {#manage}

### Создание топика {#create-topic}

Единственный обязательный параметр для создания топика - это его путь, остальные параметры опциональны.

{% list tabs group=lang %}

- C++

  Полный список настроек можно посмотреть [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L394).

  Пример создания топика c тремя партициями и поддержкой кодека ZSTD:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_create]-[END topic_create]" %}

- Go

  Полный список поддерживаемых параметров можно посмотреть в [документации SDK](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_create]-[END topic_create]" %}

- Python

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_create]-[END topic_create]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_create]-[END topic_create]" %}

  {% endlist %}

- Java

  Полный список настроек можно посмотреть [в коде SDK](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/CreateTopicSettings.java#L97).

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_create]-[END topic_create]" %}

- C#

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций:

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_create]-[END topic_create]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_create]-[END topic_create]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_create]-[END topic_create]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Изменение топика {#alter-topic}

{% list tabs group=lang %}

- C++

  При изменении топика в параметрах метода `AlterTopic` нужно указать путь топика и параметры, которые будут изменяться. Изменяемые параметры представлены структурой `TAlterTopicSettings`.

  Полный список настроек можно посмотреть [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

  Пример добавления [важного читателя](../../concepts/datamodel/topic#important-consumer) к топику и установки [времени хранения сообщений](../../concepts/datamodel/topic#retention-time) для топика в два дня:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- Go

  При изменении топика в параметрах нужно указать путь топика и те параметры, которые будут изменяться.

  Полный список поддерживаемых параметров можно посмотреть в [документации SDK](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#AlterOption).

  Пример добавления читателя к топику

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- Python

  Пример изменения списка поддерживаемых кодеков и минимального количества партиций у топика

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_alter]-[END topic_alter]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_alter]-[END topic_alter]" %}

  {% endlist %}

- Java

  При изменении топика в параметрах метода `alterTopic` нужно указать путь топика и параметры, которые будут изменяться.

  Полный список настроек можно посмотреть [в коде SDK](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/AlterTopicSettings.java#L23).

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Получение информации о топике {#describe-topic}

{% list tabs group=lang %}

- C++

  Для получения информации о топике используется метод `DescribeTopic`.

  Описание топика представлено структурой `TTopicDescription`.

  Полный список полей описания смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L163).

  Получить доступ к этому описанию можно так:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_describe]-[END topic_describe]" %}

  Существует отдельный метод для получения информации о читателе - `DescribeConsumer`.

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- Python

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_describe]-[END topic_describe]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_describe]-[END topic_describe]" %}

  {% endlist %}

- Java

  Для получения информации о топике используется метод `describeTopic`.

  Полный список полей описания можно посмотреть [в коде SDK](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/description/TopicDescription.java#L19).

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Удаление топика {#drop-topic}

Для удаления топика достаточно указать путь к нему.

{% list tabs group=lang %}

- C++

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- Python

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_drop]-[END topic_drop]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_drop]-[END topic_drop]" %}

  {% endlist %}

- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Запись сообщений {#write}

### Подключение к топику для записи сообщений {#start-writer}

На данный момент поддерживается подключение только с совпадающими идентификаторами [источника и группы сообщений](../../concepts/datamodel/topic#producer-id) (`producer_id` и `message_group_id`), в будущем это ограничение будет снято.

{% list tabs group=lang %}

- C++

  В C++ SDK для записи в топик доступно три варианта API. Базовые настройки (буферизация, кодеки, ретраи) одинаковы у всех трёх и заданы структурой `TWriteSessionSettings`, поэтому ниже подсказки сосредоточены на отличиях и сценариях использования.
    - `IWriteSession` — низкоуровневая сессия записи в одну партицию с полным набором возможностей: цикл событий (`TReadyToAcceptEvent`, `TAcksEvent`, `TSessionClosedEvent`), явное управление потоком отправки через `TContinuationToken`, которые система выдаёт перед приёмом каждого сообщения; независимые подтверждения для каждого сообщения; отправка предварительно сжатых данных через метод `WriteEncoded`, минуя повторное сжатие на сервере. Структура `TWriteSessionSettings` определена именно здесь — два других варианта её переиспользуют. Подходит, когда нужен статус каждого сообщения, кастомная асинхронная логика или ручное управление сжатием.
    - `ISimpleBlockingWriteSession` — синхронный fire-and-forget API для записи в одну партицию топика, самый простой вариант. Метод `Write(message, blockTimeout)` кладёт сообщение во внутренний буфер, отправка на сервер идёт в фоне. В нормальном режиме вызов возвращается мгновенно и блокируется только при переполнении буфера (по `MaxMemoryUsage` / `MaxInflightCount`), не дольше `blockTimeout`. Возврат `false` означает, что сообщение **не попало в буфер** и потеряно. Подтверждений по отдельным сообщениям нет; убедиться, что весь буфер доставлен на сервер, можно только вызвав `Close()` — он ждёт ack от сервера. Подходит, когда достаточно гарантии «всё или ничего» к моменту закрытия сессии и нужен простой синхронный код.
    - `IProducer` — высокоуровневый API поверх нескольких сессий записи: прозрачно шардирует сообщения по партициям топика по ключу. Вдохновлён интерфейсом Producer из Apache Kafka, но учитывает особенности {{ ydb-short-name }} и при работе с топиками с [автопартиционированием](../../concepts/datamodel/topic.md#autopartitioning) даёт полные гарантии порядка и exactly-once. Подтверждения от сервера доступны через обработчик `AcksHandler`; дождаться доставки накопленного буфера — `Flush()`, корректно завершить работу — `Close()`. Подходит, когда нужно писать в **многопартиционный** топик с маршрутизацией по ключу.

  {% list tabs %}

  - IWriteSession

    `IWriteSession` — базовая сессия записи, от которой наследуют настройки другие варианты записи. Настройки сессии записи представлены структурой `TWriteSessionSettings`, для варианта `ISimpleBlockingWriteSession` часть настроек не поддерживается.

    Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h#L56).

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

  - ISimpleBlockingWriteSession

    `ISimpleBlockingWriteSession` — простой синхронный вариант сессии записи `IWriteSession` для записи по одному сообщению без подтверждения по каждому сообщению. Метод `Write` блокируется при превышении числа inflight записей или размера буфера SDK. Настройки сессии записи представлены структурой `TWriteSessionSettings`, как и в случае `IWriteSession`, однако часть настроек не поддерживается.

    Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h#L56).

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_writer_blocking]-[END topic_start_writer_blocking]" %}

  - IProducer

    `IProducer` — высокоуровневый API поверх сессий записи: один объект скрывает управление несколькими сессиями и автоматически выбирает партицию по ключу сообщения. Настройки продюсера представлены структурой `TProducerSettings`, которая наследует `TWriteSessionSettings`, поэтому общие настройки записи совпадают с `IWriteSession`.

    Настройки задаются через `TProducerSettings`:

    - `ProducerIdPrefix` — префикс producer id для подсессий записи;
    - `PartitionChooserStrategy` — стратегия выбора партиции по ключу сообщения:
      - `Bound` — ключ сопоставляется с диапазонами партиций топика (`FromBound`/`ToBound` из описания топика). По умолчанию перед сопоставлением ключ проходит через MurmurHash64. Рекомендуется для топиков с [автопартиционированием](../../concepts/datamodel/topic.md#autopartitioning): при разделении партиции SDK обновляет границы и продолжает направлять сообщения с тем же ключом в корректный диапазон.
      - `KafkaHash` — по аналогии с Kafka: от ключа считается MurmurHash, индекс партиции — остаток от деления хеша на число партиций. Удобно при миграции с Kafka. Не поддерживается при включённом автопартиционировании.
    - `PartitioningKeyHasher` — функция преобразования ключа перед сопоставлением с диапазонами; используется только для стратегии `Bound`. Можно задать свою, например чтобы в сравнении участвовал исходный ключ без хеширования.

    Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/producer.h#L10).

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_producer]-[END topic_start_producer]" %}

  {% endlist %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- Python

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    Инициализация настроек писателя:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_writer_settings]-[END topic_writer_settings]" %}

    Создание синхронного писателя:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_writer]-[END topic_sync_writer]" %}

    После создания писателя его необходимо инициализировать. Для этого есть два метода:

    - `init()`: неблокирующий, запускает процесс инициализации в фоне и не ждёт его завершения.

      {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_writer_init]-[END topic_sync_writer_init]" %}

    - `initAndWait()`: блокирующий, запускает процесс инициализации и ждёт его завершения. Если в процессе инициализации возникла ошибка, будет брошено исключение.

      {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_writer_init_wait]-[END topic_sync_writer_init_wait]" %}

  - Асинхронный API

    Инициализация настроек писателя:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_writer_settings]-[END topic_writer_settings]" %}

    Создание и инициализация асинхронного писателя:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_async_writer]-[END topic_async_writer]" %}

    {% endlist %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Запись сообщений {#writing-messages}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Работа с объектом `IWriteSession` устроена как обработка цикла событий с тремя типами событий: `TReadyToAcceptEvent`, `TAcksEvent` и `TSessionClosedEvent`.

    Для каждого из типов событий можно установить обработчик этого события, а также можно установить общий обработчик. Обработчики устанавливаются в настройках сессии записи перед её созданием.

    Если обработчик для некоторого события не установлен, его необходимо получить и обработать в методах `GetEvent` / `GetEvents`. Для неблокирующего ожидания очередного события есть метод `WaitEvent` с интерфейсом `TFuture<void>()`.

    Для записи каждого сообщения пользователь должен "потратить" move-only объект `TContinuationToken`, который выдаёт SDK с событием `TReadyToAcceptEvent`. При записи сообщения можно установить пользовательские seqNo и временную метку создания, но по умолчанию их проставляет SDK автоматически.

    По умолчанию `Write` выполняется асинхронно — данные из сообщений вычитываются и сохраняются во внутренний буфер, отправка происходит в фоне в соответствии с настройками `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. Сессия сама переподключается к {{ ydb-short-name }} при обрывах связи и повторяет отправку сообщений пока это возможно, в соответствии с настройкой `RetryPolicy`. При получении ошибки, после которой невозможно продолжить работу, сессия записи отправляет пользователю `TSessionClosedEvent` с диагностической информацией.

    Так может выглядеть запись нескольких сообщений в цикле событий без использования обработчиков:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write]-[END topic_write]" %}

  - ISimpleBlockingWriteSession

    Как упрощённый вариант `IWriteSession`, `ISimpleBlockingWriteSession` пишет через ту же внутреннюю буферизацию, но без цикла событий: не использует `ContinuationToken` и не возвращает подтверждения через `TAcksEvent`. Метод `Write` кладёт сообщение во внутренний буфер; если буфер переполнен, вызов блокируется до появления места. Параметр `blockTimeout` ограничивает время ожидания. Метод возвращает `true`, если сообщение принято в буфер, и `false`, если за отведённое время записать не удалось.

    Отправка на сервер, как и у `IWriteSession`, выполняется в фоне. Чтобы дождаться завершения всех записей и закрыть сессию, вызовите `Close()`.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_blocking]-[END topic_write_blocking]" %}

  - IProducer

    `TProducerSettings` наследует `TWriteSessionSettings`, поэтому буферизация, отправка и переподключение устроены так же, как у `IWriteSession`: `Write` кладёт сообщение во внутренний буфер, отправка на сервер идёт в фоне в соответствии с настройками `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. Продюсер переподключается к {{ ydb-short-name }} при обрывах связи и повторяет отправку, пока это возможно, в соответствии с `RetryPolicy`. При неустранимой ошибке продюсер закрывается; статус и причину можно получить из результата `Write` или `Flush`.

    `Flush` дожидается доставки накопленных данных на сервер; `Close` дожидается отправки оставшихся в буфере сообщений и завершает работу продюсера.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_producer]-[END topic_write_producer]" %}

    Подробный пример см. в [репозитории ydb-platform/ydb](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/topic_writer/producer/basic_write).

  {% endlist %}

- Go

  Для отправки сообщения - достаточно в поле Data сохранить Reader, из которого можно будет прочитать данные. Можно рассчитывать на то что данные каждого сообщения читаются один раз (или до первой ошибки), к моменту возврата из Write данные будут уже прочитаны и сохранены во внутренний буфер.

  SeqNo и дата создания сообщений по умолчанию проставляются автоматически.

  По умолчанию Write выполняется асинхронно - данные из сообщений вычитываются и сохраняются во внутренний буфер, отправка происходит в фоне. Writer сам переподключается к {{ ydb-short-name }} при обрывах связи и повторяет отправку сообщений пока это возможно. При получении ошибки, после которой невозможно продолжить работу, Writer останавливается и следующие вызовы Write будут завершаться с ошибкой.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write]-[END topic_write]" %}

  Для записи по ключу в несколько партиций используйте `WithWriteToManyPartitions(...)` при создании писателя и заполняйте поле `Key` в `topicwriter.Message`.

  Стратегии маршрутизации (задаются в `WithWriterPartitionByKey(...)` или `WithWriterPartitionByPartitionID()`):

  - `BoundPartitionChooser` — ключ сопоставляется с диапазонами партиций топика (`FromBound`/`ToBound`). По умолчанию перед сопоставлением ключ проходит через MurmurHash64. Рекомендуется для топиков с [автопартиционированием](../../concepts/datamodel/topic.md#autopartitioning): SDK обновляет границы при разделении партиций.
  - `KafkaHashPartitionChooser` — по аналогии с Kafka: MurmurHash ключа по модулю числа партиций. Удобно при миграции с Kafka. Не поддерживается при включённом автопартиционировании.
  - `WithWriterPartitionByPartitionID` — запись в партицию, указанную в поле `PartitionID` сообщения. Не комбинируется с маршрутизацией по ключу; при split партиции писатель нужно пересоздавать вручную.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_multiwriter]-[END topic_multiwriter]" %}

  Подробный пример с маршрутизацией по ключу, альтернативными стратегиями (`KafkaHash` и `PartitionID`) и транзакционным вариантом см. в репозитории [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/topic/topicwriter/topicwriter_to_many_partitions.go).

- Python

  Для отправки сообщений можно передавать как просто содержимое сообщения (bytes, str), так и вручную задавать некоторые свойства. Объекты можно передавать по одному или сразу в массиве (list). Метод `write` выполняется асинхронно. Возврат из метода происходит сразу после того как сообщения будут положены во внутренний буфер клиента, обычно это происходит быстро. Ожидание может возникнуть, если внутренний буфер уже заполнен и нужно подождать, пока часть данных будет отправлена на сервер.

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write]-[END topic_write]" %}

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_manual]-[END topic_write_manual]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write]-[END topic_write]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    Метод `send` блокирует управление, пока сообщение не будет помещено в очередь отправки.
    Попадание сообщения в эту очередь означает, что писатель сделает всё возможное для доставки сообщения.
    Например, если сессия записи по какой-то причине оборвётся, писатель переустановит соединение и попробует отправить это сообщение на новой сессии.
    Но попадание сообщения в очередь отправки не гарантирует того, что сообщение в итоге будет записано.
    Например, могут возникать ошибки, приводящие к завершению работы писателя до того, как сообщения из очереди будут отправлены.
    Если нужно подтверждение успешной записи для каждого сообщения, используйте асинхронного писателя и проверяйте статус, возвращаемый методом `send`.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_sync]-[END topic_write_sync]" %}

  - Асинхронный API

    Метод `send` в асинхронном клиенте неблокирующий. Помещает сообщение в очередь отправки.
    Метод возвращает `CompletableFuture<WriteAck>`, позволяющую проверить, действительно ли сообщение было записано.
    В случае, если очередь переполнена, будет брошено исключение QueueOverflowException.
    Это способ сигнализировать пользователю о том, что поток записи следует притормозить.
    В таком случае стоит или пропускать сообщения, или выполнять повторные попытки записи через exponential backoff.
    Также можно увеличить размер клиентского буфера (`setMaxSendBufferMemorySize`), чтобы обрабатывать больший объем сообщений перед тем, как он заполнится.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_async]-[END topic_write_async]" %}

  {% endlist %}

- C#

  Асинхронная запись сообщения в топик.

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write]-[END topic_write]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_write]-[END topic_write]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_write]-[END topic_write]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Запись сообщений с подтверждением о сохранении на сервере

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Ответы о записи сообщений на сервере приходят клиенту SDK в виде событий `TAcksEvent`. В одном событии могут содержаться ответы о нескольких отправленных ранее сообщениях. Варианты ответа: запись подтверждена (`EES_WRITTEN`), запись отброшена как дубликат ранее записанного сообщения (`EES_ALREADY_WRITTEN`) или запись отброшена по причине сбоя (`EES_DISCARDED`).

    Пример установки обработчика `TAcksEvent` для сессии записи:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

    В такой сессии записи события `TAcksEvent` не будут приходить пользователю в `GetEvent` / `GetEvents`, вместо этого SDK при получении подтверждений от сервера будет вызывать переданный обработчик. Аналогично можно настраивать обработчики на остальные типы событий.

  - ISimpleBlockingWriteSession

    В отличие от `IWriteSession`, `ISimpleBlockingWriteSession` не возвращает подтверждения по отдельным сообщениям: события `TAcksEvent` и обработчики для них недоступны. Чтобы дождаться, пока все сообщения из буфера будут записаны на сервер, вызовите `Close()` — метод дожидается подтверждения от сервера и закрывает сессию.

  - IProducer

    Подтверждения от сервера приходят так же, как у `IWriteSession`: через обработчик `AcksHandler` в `TProducerSettings::EventHandlers`. Чтобы дождаться доставки накопленного буфера на сервер, вызовите `Flush()`.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_producer_ack]-[END topic_producer_ack]" %}

  {% endlist %}

- Go

  При подключении можно указать опцию синхронной записи сообщений - topicoptions.WithSyncWrite(true). Тогда Write будет возвращаться только после того как получит подтверждение с сервера о сохранении всех, сообщений переданных в вызове. При этом SDK так же как и обычно будет при необходимости переподключаться и повторять отправку сообщений. В этом режиме контекст управляет только временем ожидания ответа из SDK, т.е. даже после отмены контекста SDK продолжит попытки отправить сообщения.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- Python

  Есть два способа получить подтверждение о записи сообщений на сервере:
  - `flush()` — дожидается подтверждения для всех сообщений, записанных ранее во внутренний буфер.
  - `write_with_ack(...)` — отправляет сообщение и ждет подтверждение его доставки от сервера. При отправке нескольких сообщений подряд это способ работает медленно.

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

  {% endlist %}

- Java

  Метод `send` возвращает `CompletableFuture<WriteAck>`. Её успешное завершение означает подтверждение записи сервером.
  В структуре `WriteAck` содержится информация о seqNo, offset и статусе записи:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- C#

  Асинхронная запись сообщения в топик. В случае переполнения внутреннего буфера будет ожидать, когда буфер освободится для повторной отправки.

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

  В случае, если сервер недоступен, сообщения могут накапливаться в очереди в ожидании отправки. Для управления временем ожидания можно использовать токен отмены (`CancellationToken`). Однако, при таком подходе существует вероятность того, что пользователь может отменить отправку уже подтвержденного сообщения.

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write_deadline]-[END topic_write_deadline]" %}

- JavaScript

  Все сообщения записываются во внутренний буфер. Для отправки на сервер есть 3 механизма: два автоматических и один ручной. Ручной - это вызов метода `writer.flush` который возвращает последний seqno записанный на сервере. Автоматическая отправка происходит по условиям:
  - Превышение размера внутреннего буфера `maxBufferBytes` (значение по умолчанию = 256MiB).
  - По тику интервала периодической отправки `flushIntervalMs` (значение по умолчанию = 10ms).

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Выбор кодека для сжатия сообщений {#codec}

Подробнее о [сжатии данных в топиках](../../concepts/datamodel/topic#message-codec).

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Сжатие, которое используется при отправке сообщений методом `Write`, задаётся при [создании сессии записи](#start-writer) настройками `Codec` и `CompressionLevel`. По умолчанию выбирается кодек GZIP.
    Пример создания сессии записи без сжатия сообщений:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_codec]-[END topic_codec]" %}

    Если необходимо в рамках сессии записи отправить сообщение, сжатое другим кодеком, можно использовать метод `WriteEncoded` с указанием кодека и размера расжатого сообщения. Для успешной записи этим способом используемый кодек должен быть разрешён в настройках топика.

  - ISimpleBlockingWriteSession

    Кодек задаётся при [создании сессии записи](#start-writer) в `TWriteSessionSettings` — те же настройки `Codec` и `CompressionLevel`, что и у `IWriteSession`. Метод `WriteEncoded` недоступен.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_codec_blocking]-[END topic_codec_blocking]" %}

  - IProducer

    Кодек задаётся в `TProducerSettings` при [создании продюсера](#start-writer) — те же настройки `Codec` и `CompressionLevel`, что и у `IWriteSession`.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_codec_producer]-[END topic_codec_producer]" %}

  {% endlist %}

- Go

  По умолчанию SDK выбирает кодек автоматически (с учетом настроек топика). В автоматическом режиме SDK сначала отправляет по одной группе сообщений каждым из разрешенных кодеков, затем иногда будет пробовать сжать сообщения всеми доступными кодеками и выбирать кодек, дающий наименьший размер сообщения. Если для топика список разрешенных кодеков пуст, то автовыбор производится между Raw и Gzip-кодеками.

  При необходимости можно задать фиксированный кодек в опциях подключения. Тогда будет использоваться именно он и замеры проводиться не будут.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_codec]-[END topic_codec]" %}

- Python

  По умолчанию SDK выбирает кодек автоматически (с учетом настроек топика). В автоматическом режиме SDK сначала отправляет по одной группе сообщений каждым из разрешенных кодеков, затем иногда будет пробовать сжать сообщения всеми доступными кодеками и выбирать кодек, дающий наименьший размер сообщения. Если для топика список разрешенных кодеков пуст, то автовыбор производится между Raw и Gzip-кодеками.

  При необходимости можно задать фиксированный кодек в опциях подключения. Тогда будет использоваться именно он и замеры проводиться не будут.

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_codec]-[END topic_codec]" %}

- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_codec]-[END topic_codec]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  JavaScript SDK принимает объект `CompressionCodec` с функциями `compress` и `decompress`. `RAW_CODEC` и `GZIP_CODEC` импортируются из `@ydbjs/topic/codec`. Для LZOP и пользовательских кодеков нужно передать реализацию; в полном примере определены `lzopCodec` и `customCodec`.

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_raw]-[END topic_codec_raw]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_gzip]-[END topic_codec_gzip]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_lzop]-[END topic_codec_lzop]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_custom]-[END topic_codec_custom]" %}

- Rust

  Выбор кодека сжатия при записи в Rust SDK пока недоступен; сообщения отправляются с кодеком `Raw`.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Запись сообщений без дедупликации {#nodedup}

Подробнее о записи без дедупликации — в [соответствующем разделе концепций](../../concepts/datamodel/topic#no-dedup).

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Если в настройках сессии записи не указывается опция `ProducerId`, будет создана сессия записи без дедупликации.
    Пример создания такой сессии записи:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_no_dedup]-[END topic_no_dedup]" %}

    Для включения дедупликации нужно в настройках сессии записи указать опцию `ProducerId` или явно включить дедупликацию, вызвав метод `DeduplicationEnabled()`, например, как в секции ["Подключение к топику"](#start-writer).

  - ISimpleBlockingWriteSession

    Поведение такое же, как у `IWriteSession`: если не указывать `ProducerId`, сессия создаётся без дедупликации.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_no_dedup_blocking]-[END topic_no_dedup_blocking]" %}

  - IProducer

    `IProducer` всегда записывает с дедупликацией: идентификатор продюсера формируется из `ProducerIdPrefix` и id партиции. Для записи без дедупликации используйте `IWriteSession` или `ISimpleBlockingWriteSession`.

  {% endlist %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Go

  В **ydb-go-sdk** при создании писателя, если не передавать `topicoptions.WithWriterProducerID`, SDK всё равно подставляет идентификатор производителя (генерирует его автоматически). Режим записи без дедупликации, эквивалентный отсутствию `ProducerId` в примере для C++ выше, в текущей версии SDK недоступен.

- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Запись метаданных на уровне сообщения {#messagemeta}

При записи сообщения можно дополнительно указать метаданные как список пар "ключ-значение". Эти данные будут доступны при вычитывании сообщения.
Ограничение на размер метаданных — не более 1000 ключей.

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Метаданные задаются в объекте `TWriteMessage` и передаются в `Write()`:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  - ISimpleBlockingWriteSession

    Используется тот же `TWriteMessage`, что и у `IWriteSession`: метаданные задаются через `MessageMeta()` и передаются в `Write()`:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_metadata_blocking]-[END topic_write_metadata_blocking]" %}

  - IProducer

    Как и у сессий записи, метаданные задаются в `TWriteMessage` через `MessageMeta()`. Отличие `IProducer` в том, что сообщение также содержит ключ партиционирования, по которому продюсер выбирает партицию:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_metadata_producer]-[END topic_write_metadata_producer]" %}

  {% endlist %}

- Go

  Метаданные задаются в поле `Metadata` структуры `topicwriter.Message`:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  При чтении метаданные доступны в поле `Metadata` у сообщения:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_metadata]-[END topic_read_metadata]" %}

- Java

  При конструировании сообщения для записи с помощью Builder'а, ему можно передать объекты типа `MetadataItem` с парой ключ типа `String` + значение типа `byte[]`.

  Можно передать сразу `List` таких объектов:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  Или добавлять каждый `MetadataItem` отдельно:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_metadata_add]-[END topic_write_metadata_add]" %}

  При чтении эти метаданные сообщения получить, вызвав на нём метод `getMetadataItems()`:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_metadata]-[END topic_read_metadata]" %}

- Python

  Для использования функции передачи метаданных создайте объект `TopicWriterMessage` с аргументом `metadata_items`, как показано ниже:

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  {% endlist %}

  Во время чтения метаданные можно получить из поля `metadata_items` объекта `PublicMessage`:

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_metadata]-[END topic_read_metadata]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Запись в транзакции {#write-tx}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Для записи в топик в транзакции необходимо передать ссылку на объект транзакции в метод `Write` сессии записи.

    [Пример на GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

    Функция `NextToken` из полного примера ожидает `TReadyToAcceptEvent` и возвращает его `ContinuationToken`, как показано в цикле записи выше. Асинхронному writer этот токен нужен и при отправке сообщения в транзакции.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

  - ISimpleBlockingWriteSession

    Как и `IWriteSession`, `ISimpleBlockingWriteSession` поддерживает запись в транзакции. Так как у простого варианта нет `ContinuationToken`, объект транзакции передаётся вторым аргументом в `Write()`.

    [Пример на GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_tx_blocking]-[END topic_write_tx_blocking]" %}

  - IProducer

    У `IProducer` транзакция задаётся не аргументом `Write`, а в `TWriteMessage` через `Tx()`. После этого продюсер записывает сообщение так же, как обычное сообщение с ключом партиционирования:

  {% endlist %}

- Go

  Для записи в топик в транзакции необходимо создать транзакционного писателя через вызов [TopicClient.StartTransactionalWriter](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic#Client.StartTransactionalWriter). После этого можно отправлять сообщения, как обычно. Закрывать транзакционного писателя не требуется — это происходит автоматически при завершении транзакции.

  [Пример на GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/8ad2661ef04180780d314fb0706ec3f8a7ccfc8d/examples/ydb_tech/topic)

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

- Python

  Для записи в топик в транзакции необходимо создать транзакционного писателя через вызов `topic_client.tx_writer`. После этого можно отправлять сообщения, как обычно. Закрывать транзакционного писателя не требуется — это происходит автоматически при завершении транзакции.

  В примере ниже нет явного вызова `tx.commit()` — он происходит неявно при успешном завершении лямбды `callee`.

  [Пример на GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

  - Native SDK (Asyncio)

    [Пример на GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    В настройках `SendSettings` метода `send` можно указать транзакцию.
    Тогда сообщение будет записано вместе с коммитом этой транзакцией.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_tx_sync]-[END topic_write_tx_sync]" %}

  - Асинхронный API

    [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    В настройках `SendSettings` метода `send` можно указать транзакцию.
    Тогда сообщение будет записано вместе с коммитом этой транзакцией.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_tx_async]-[END topic_write_tx_async]" %}

  {% endlist %}

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Чтение сообщений {#reading}

### Подключение к топику для чтения сообщений {#start-reader}

Чтение сообщений из топика может выполнятся с указанием Consumer'а, связанного с этим топиком, а также без Consumer'а. Если Consumer не указан, то клиентское приложение должно самостоятельно рассчитывать offset для чтения сообщений. Более подробно пример чтения без Consumer'а рассмотрен в [соответствующей секции](#no-consumer).

Создать Consumer можно при [создании](#create-topic) или [изменении](#alter-topic) топика.
У топика может быть несколько Consumer'ов и для каждого из них сервер хранит свой прогресс чтения.

{% list tabs group=lang %}

- C++

  Подключение для чтения из одного или нескольких топиков представлено объектом сессии чтения с интерфейсом `IReadSession`. Настройки сессии чтения представлены структурой `TReadSessionSettings`.

  Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1344).

  Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- Go

  Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- Python

  Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    Инициализация настроек читателя

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_reader_settings]-[END topic_reader_settings]" %}

    Создание синхронного читателя

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_reader]-[END topic_sync_reader]" %}

    После создания синхронного читателя необходимо инициализировать. Для этого следует воспользоваться одним их двух методов:
    - `init()`: неблокирующий, запускает процесс инициализации в фоне и не ждёт его завершения.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_reader_background]-[END topic_sync_reader_background]" %}

    - `initAndWait()`: блокирующий, запускает процесс инициализации и ждёт его завершения. Если в процессе инициализации возникла ошибка, будет брошено исключение.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_reader_init]-[END topic_sync_reader_init]" %}

  - Асинхронный API

    Инициализация настроек читателя

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_reader_settings]-[END topic_reader_settings]" %}

    Для асинхронного читателя, помимо общих настроек чтения `ReaderSettings`, понадобятся настройки обработчика событий `ReadEventHandlersSettings`, в которых необходимо передать экземпляр наследника `ReadEventHandler`.
    Он будет описывать, как должна происходить обработка различных событий, происходящих во время чтения.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_handler_settings]-[END topic_handler_settings]" %}

    Опционально, в `ReadEventHandlersSettings` можно указать executor'а, на котором будет происходить обработка сообщений; по умолчанию используется внутренний поток SDK.

    Для реализации обработчика событий можно унаследоваться от `AbstractReadEventHandler` и переопределить метод `onMessages`.
    Метод `onMessages` вызывается каждый раз, когда SDK получает очередной пакет сообщений от сервера. В рамках одного вызова приходит один или несколько сообщений, которые можно подтвердить (`commit`) как по отдельности, так и после обработки всего пакета. Пример реализации:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_batch_individual_commit]-[END topic_read_batch_individual_commit]" %}

    Создание и инициализация асинхронного читателя:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_async_reader]-[END topic_async_reader]" %}

  {% endlist %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

Вы также можете использовать расширенный вариант создания подключения, чтобы указать несколько топиков и задать параметры чтения. Следующий код создаст подключение к топикам `my-topic` и `my-specific-topic` через читателя `my-consumer`:

{% list tabs group=lang %}

- C++

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

  Также в примере выше задаётся время, с которого следует начинать читать сообщения.

- Python

  Функциональность находится в разработке.

- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_partitions]-[END topic_reader_selectors_partitions]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_lag]-[END topic_reader_selectors_lag]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_from]-[END topic_reader_selectors_from]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_multiple]-[END topic_reader_selectors_multiple]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Чтение сообщений {#reading-messages}

Сервер хранит [позицию чтения сообщений](../../concepts/datamodel/topic.md#consumer-offset). После вычитывания очередного сообщения клиент может [отправить на сервер подтверждение обработки](#commit). Позиция чтения изменится, а при новом подключении будут вычитаны только неподтвержденные сообщения.

Читать сообщения можно и [без подтверждения обработки](#no-commit). В этом случае при новом подключении будут прочитаны все неподтвержденные сообщения, в том числе и уже обработанные.

Информацию о том, какие сообщения уже обработаны, можно [сохранять на клиентской стороне](#client-commit), передавая на сервер стартовую позицию чтения при создании подключения. При этом позиция чтения сообщений на сервере не изменяется.

Можно использовать [транзакции](#read-tx). В этом случае позиция чтения изменится при подтверждении транзакции. При новом подключении будут прочитаны все неподтверждённые сообщения.

{% list tabs group=lang %}

- C++

  Работа пользователя с объектом `IReadSession` в общем устроена как обработка цикла событий со следующими типами событий: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TEndPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent` и `TSessionClosedEvent`.

  Для каждого из типов событий можно установить обработчик этого события, а также можно установить общий обработчик. Обработчики устанавливаются в настройках сессии записи перед её созданием.

  Если обработчик для некоторого события не установлен, его необходимо получить и обработать в методах `GetEvent` / `GetEvents`. Для неблокирующего ожидания очередного события есть метод `WaitEvent` с сигнатурой `TFuture<void>()`.

- Go

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}

- Python

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}

- Java

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}

- C#

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  Полный пример чтения топика в транзакции с записью в таблицу: [`topic-read-in-transaction-example.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/topic-read-in-transaction-example.rs).

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Чтение без подтверждения обработки сообщений {#no-commit}

#### Чтение сообщений по одному

{% list tabs group=lang %}

- C++

  Чтение сообщений по одному в C++ SDK не предусмотрено. Событие `TDataReceivedEvent` содержит пакет сообщений.

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    Чтобы читать сообщения без подтверждения обработки, по одному, используйте следующий код:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

  - Асинхронный API

    В асинхронном клиенте нет возможности читать сообщения по одному.

  {% endlist %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Чтение сообщений пакетом

{% list tabs group=lang %}

- C++

  При установке сессии чтения с настройкой `SimpleDataHandlers` достаточно передать обработчик для сообщений с данными. SDK будет вызывать этот обработчик на каждый принятый от сервера пакет сообщений. Подтверждения чтения по умолчанию отправляться не будут.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_session]-[END topic_read_batch_session]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_wait]-[END topic_read_batch_wait]" %}

  В этом примере после создания сессии основной поток дожидается завершения сессии со стороны сервера в методе `GetEvent`, другие типы событий приходить не будут.

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    В синхронном клиенте нет возможности прочитать сразу пакет сообщений.

  - Асинхронный API

    Чтобы прочитать пакет сообщений без подтверждения обработки, используйте следующий код:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  {% endlist %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Чтение с подтверждением обработки сообщений {#commit}

Подтверждение обработки сообщения (коммит) - сообщает серверу, что сообщение из топика обработано получателем и больше его отправлять не нужно. При использовании чтения с подтверждением нужно подтверждать все полученные сообщения без пропуска. Коммит сообщений на сервере происходит после подтверждения очередного интервала сообщений «без дырок», сами подтверждения при этом можно отправлять в любом порядке.

Например, с сервера пришли сообщения 1, 2, 3. Программа обрабатывает их параллельно и отправляет подтверждения в таком порядке: 1, 3, 2. В этом случае сначала будет закоммичено сообщение 1, а сообщения 2 и 3 будут закоммичены только после того как сервер получит подтверждение об обработке сообщения 2.

В случае ошибки на коммите сообщения можно написать эту ошибку в лог и продолжить работу. Состояние сообщения в этой точке неизвестно. Сообщение могло закоммититься, а потом возникла сетевая ошибка и клиент не получил подтверждения. Если сообщение не закоммитилось, то оно будет прочитано ещё раз и снова поступит в обработку (может быть на другом читателе). Ретраить именно коммит смысла нет, т.к. сессия чтения этого сообщения уже потеряна.

#### Чтение сообщений по одному с подтверждением

{% list tabs group=lang %}

- C++

  Чтение сообщений по одному в C++ SDK не предусмотрено. Событие `TDataReceivedEvent` содержит пакет сообщений.

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  По умолчанию `Commit` — это быстрый вызов: сохраняет данные во внутреннем буфере и сразу возвращает управление, а реальная отправка происходит позже. Поэтому, чтобы не терять последние коммиты перед выходом из программы, читателя нужно закрывать явно с помощью вызова `Reader.Close()`.

- Python

  `commit` - это быстрый вызов: сохраняет данные во внутреннем буфере и сразу возвращает управление, а реальная отправка происходит позже. Поэтому, чтобы не терять последние коммиты перед выходом из программы, читателя нужно закрывать явно.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  {% endlist %}

- Java

  Для подтверждения обработки сообщения достаточно вызвать у сообщения метод `commit`.
  Актуально как для синхронного, так и для асинхронного читателя.
  В асинхронном читателе, при обработке пакета сообщений, можно вызвать `commit` или у всего пакета сразу, или у каждого сообщения отдельно.
  Этот метод возвращает `CompletableFuture<Void>`, успешное выполнение которой означает подтверждение обработки сервером.
  В случае ошибки коммита не следует пытаться его ретраить. Скорее всего, ошибка вызвана закрытием сессии.
  Читатель (необязательно этот же) сам создаст новую сессию для этой партиции и сообщение будет прочитано снова.

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_commit_ack]-[END topic_read_commit_ack]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Чтение сообщений пакетом с подтверждением

{% list tabs group=lang %}

- C++

  Аналогично [примеру выше](#no-commit), при установке сессии чтения с настройкой `SimpleDataHandlers` достаточно передать обработчик для сообщений с данными. SDK будет вызывать этот обработчик на каждый принятый от сервера пакет сообщений. Передача параметра `commitDataAfterProcessing = true` означает, что SDK будет отправлять на сервер подтверждения чтения всех сообщений после выполнения обработчика.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_commit_session]-[END topic_read_batch_commit_session]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_commit_wait]-[END topic_read_batch_commit_wait]" %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  По умолчанию `Commit` - это быстрый вызов: сохраняет данные во внутреннем буфере и сразу возвращает управление, а реальная отправка происходит позже. Поэтому, чтобы не терять последние коммиты перед выходом из программы, читателя нужно закрывать явно.

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  {% endlist %}

  `commit` - это быстрый вызов: сохраняет данные во внутреннем буфере и сразу возвращает управление, а реальная отправка происходит позже. Поэтому, чтобы не терять последние коммиты перед выходом из программы, читателя нужно закрывать явно.

- Java

  {% list tabs %}

  - Синхронный API

    Неактуально, т.к. в синхронном читателе нет возможности читать сообщения пакетами.

  - Асинхронный API

    В обработчике `onMessages` можно закоммитить весь пакет сообщений, вызвав `commit` на событии.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  {% endlist %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Чтение с хранением позиции на клиентской стороне {#client-commit}

Вместо коммитов сообщений на сервер можно хранить прогресс чтения самостоятельно. В этом случае нужно передать в SDK обработчик, который будет вызываться при старте чтения каждой партиции. В этом обработчике нужно будет
указать позицию, с которой нужно начинать чтение этой партиции.

{% list tabs group=lang %}

- C++

  При обработке событий `TStartPartitionSessionEvent` можно при ответе серверу задать позицию, с которой следует начинать чтение.
  Для этого в метод `Confirm` следует передать параметр `readOffset`.
  Допольнительно можно передать параметр `commitOffset`, который укажет позицию, сообщения до которой следует считать [закоммиченными](#commit).

  Пример установки обработчика:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

  Здесь `GetOffsetToReadFrom` - это часть примера, а не SDK. Используйте свой способ определить требуемую стартовую позицию чтения для партиции с данным partition id.

  Также в `TReadSessionSettings` поддерживается настройка `ReadFromTimestamp` для чтения событий с отметками времени записи не меньше данной. Эта настройка предполагается не для точного позиционирования старта, а для пропуска объёма данных за большой интервал времени. Несколько первых полученных сообщений могут иметь отметки времени записи меньше указанной.

- Go

  {% note tip %}

  В режиме читателя по умолчанию оффсеты до позиции, указанной через `res.StartFrom`, подтверждаются на сервере. После этого повторное чтение тех же сообщений путём сдвига позиции назад становится невозможным. Чтобы отключить автоматическое подтверждение, используйте режим без коммитов при создании читателя.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

  {% endnote %}

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_client_offset_storage]-[END topic_client_offset_storage]" %}

- Python

  Функциональность находится в разработке.

- Java

  Чтение с заданного оффсета в Java возможно только в асинхронном читателе.
  В обработчике событий `StartPartitionSessionEvent` можно при ответе серверу задать позицию, с которой следует начинать чтение.
  Для этого в метод `confirm` следует передать настройки `StartPartitionSessionSettings` с указанным оффсетом через `setReadOffset`.
  Также вызовом `setCommitOffset` можно указать оффсет, который следует считать закоммиченным.

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

  Также поддерживается настройка читателя `setReadFrom` для чтения событий с отметками времени записи не меньше данной.

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Чтение без указания Consumer'а {#no-consumer}

Обычно прогресс чтения топика сохраняется на сервере в каждом `Consumer`е. Но можно не хранить такой прогресс на сервере и при создании читателя явно указать, что чтение будет происходить без `Consumer`а.

{% list tabs group=lang %}

- C++

  В `NYdb::NTopic::TReadSessionSettings` вызовите `WithoutConsumer()`:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

  При переподключении прогресс чтения на сервере не сохраняется. Чтобы не начинать с начала, при каждом старте сессии чтения партиции передавайте смещение в `TStartPartitionSessionEvent::Confirm` — см. [хранение позиции на клиенте](#client-commit).

- Go

  Нужно передать пустую строку в качестве имени consumer и опцию `topicoptions.WithReaderWithoutConsumer(false)` (режим **экспериментальный**, см. [VERSIONING](https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md) в репозитории SDK). В селекторе чтения укажите путь топика и список партиций. Коммиты сообщений в этом режиме недоступны (`CommitModeNone`); при переподключениях прогресс нужно восстанавливать на стороне клиента — см. [хранение позиции на клиенте](#client-commit).

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

- Java

  Для чтения без Consumer'а следует в настройках читателя `ReaderSettings` это явно указать, вызвав `withoutConsumer()`:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

  В таком случае нужно учитывать, что при переустановке соединения прогресс на сервере будет сброшен. Поэтому, чтобы не начинать чтение сначала, в SDK следует передавать offset начала чтения при каждом старте сессии чтения партиции:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_no_consumer_offset]-[END topic_no_consumer_offset]" %}

- Python

  Для чтения без Consumer'а следует создать читателя с помощью метода `reader` с указанием следующих аргументов:
  - `topic` - объект `ydb.TopicReaderSelector` с указанными `path` и списком `partitions`;
  - `consumer` - должен быть `None`;
  - `event_handler` - наследник `ydb.TopicReaderEvents.EventHandler`, который реализует функцию `on_partition_get_start_offset`. Эта функция отвечает за возвращение начального смещения (offset) для чтения сообщений при старте читателя, а также во время переподключений. Клиентское приложение должно указать это смещение в параметре `ydb.TopicReaderEvents.OnPartitionGetStartOffsetResponse.start_offset`. Также функция может быть реализована как асинхронная.

  Пример:

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_no_consumer_handler]-[END topic_no_consumer_handler]" %}

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Чтение в транзакции {#read-tx}

{% list tabs group=lang %}

- C++

  Перед чтением из топика клиентский код должен передать в настройки получения событий из сессии ссылку на объект транзакции.

  [Пример на GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

  {% note warning %}

  При обработке событий `events` не нужно явно подтверждать обработку для событий типа `TDataReceivedEvent`.

  {% endnote %}

  Подтверждение обработки события `TStopPartitionSessionEvent` надо делать после вызова `Commit`.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_tx_deferred_stop]-[END topic_read_tx_deferred_stop]" %}

- Go

  Для чтения сообщений в рамках транзакции следует использовать метод [`Reader.PopMessagesBatchTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader#Reader.PopMessagesBatchTx). Он прочитает пакет сообщений и добавит их коммит в транзакцию, при этом отдельно коммитить эти сообщения не требуется. Читателя сообщений можно использовать повторно в разных транзакциях. При этом важно, чтобы порядок коммита транзакций соответствовал порядку получения сообщений от читателя, так как коммиты сообщений в топике должны выполняться строго по порядку. Проще всего это сделать если использовать читателя в цикле.

  [Пример на GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/8ad2661ef04180780d314fb0706ec3f8a7ccfc8d/examples/ydb_tech/topic)

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

- Python

  Для чтения сообщений в рамках транзакции следует использовать метод `reader.receive_batch_with_tx`. Он прочитает пакет сообщений и добавит их коммит в транзакцию, при этом отдельно коммитить эти сообщения не требуется. Читателя сообщений можно использовать повторно в разных транзакциях. При этом важно, чтобы порядок коммита транзакций соответствовал порядку получения сообщений от читателя, так как коммиты сообщений в топике должны выполняться строго по порядку - в противном случае транзакция получит ошибку на попытке сделать коммит. Проще всего это сделать, если использовать читателя в цикле.

  {% list tabs %}

  - Native SDK

    [Пример на GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

  - Native SDK (Asyncio)

    [Пример на GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    В настройках `ReceiveSettings` метода `receive` можно указать транзакцию:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_tx_sync]-[END topic_read_tx_sync]" %}

    Тогда полученное сообщение будет закоммичено вместе с транзакцией. Коммитить его отдельно не нужно.
    Метод `receive` свяжет на сервере оффсеты сообщения с транзакцией вызовом `sendUpdateOffsetsInTransaction` и вернёт управление, когда получит ответ на него.

  - Асинхронный API

    [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    После получения сообщения в обработчике `onMessages` можно связать одно или несколько сообщений с транзакцией.
    Для этого нужно вызвать отдельный метод `reader.updateOffsetsInTransaction` и дождаться его выполнения на сервере.
    Этот метод принимает параметром список оффсетов. Для удобства у `Message` и `DataReceivedEvent` есть метод `getPartitionOffsets()`, возвращающий такой список.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_tx_async]-[END topic_read_tx_async]" %}

  {% endlist %}

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  Полный пример чтения топика в транзакции с записью в таблицу: [`topic-read-in-transaction-example.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/topic-read-in-transaction-example.rs).

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Обработка серверного прерывания чтения {#stop}

В {{ ydb-short-name }} используется серверная балансировка партиций между клиентами. Это означает, что сервер может прерывать чтение сообщений из произвольных партиций.

При *мягком прерывании* клиент получает уведомление, что сервер уже закончил отправку сообщений из партиции и больше сообщения читаться не будут. Клиент может завершить обработку сообщений и отправить подтверждение на сервер.

В случае *жесткого прерывания* клиент получает уведомление, что работать с сообщениями партиции больше нельзя. Клиент должен прекратить обработку прочитанных сообщений. Неподтвержденные сообщения будут переданы другому читателю.

#### Мягкое прерывание чтения {#soft-stop}

{% list tabs group=lang %}

- C++

  Мягкое прерывание приходит в виде события `TStopPartitionSessionEvent` с методом `Confirm`. Клиент может завершить обработку сообщений и отправить подтверждение на сервер.

  Фрагмент цикла событий может выглядеть так:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

- Go

  Клиентский код сразу получает все имеющиеся в буфере (на стороне SDK) сообщения, даже если их не достаточно для формирования пакета при групповой обработке.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

- Python

  Специальной обработки не требуется.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    Неактуально, т.к. в синхронном читателе нет возможности настраивать обработку подобных событий.
    Клиент сразу ответит серверу подтверждением остановки.

  - Асинхронный API

    Для возможности реагировать на такое событие следует переопределить метод `onStopPartitionSession(StopPartitionSessionEvent event)` в объекте-наследнике `ReadEventHandler` (см [Подключение к топику для чтения сообщений](#start-reader)).
    `event.confirm()` обязательно должен быть вызван, т.к. сервер ожидает этого ответа для продолжения остановки.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

  {% endlist %}

- C#

  Специальной обработки не требуется.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  Rust SDK обрабатывает события остановки и закрытия partition session внутренне; публичного API для настройки мягкого или жёсткого прерывания чтения пока нет.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Жесткое прерывание чтения {#hard-stop}

{% list tabs group=lang %}

- C++

  Жёсткое прерывание приходит в виде события `TPartitionSessionClosedEvent` либо в ответ на подтверждение мягкого прерывания, либо при потере соединения с партицией. Узнать причину можно, вызвав метод `GetReason`.

  Фрагмент цикла событий может выглядеть так:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

- Go

  При прерывании чтения контекст сообщения или пакета сообщений будет отменен.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

- Python

  В этом примере обработка сообщений в батче остановится, если в процессе работы партиция будет отобрана. Такая оптимизация требует дополнительного кода на клиенте. В простых случаях, когда обработка отобранных партиций не является проблемой, ее можно не применять.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

  {% endlist %}

- Java

  {% list tabs %}

  - Синхронный API

    Неактуально, т.к. в синхронном читателе нет возможности настраивать обработку подобных событий.

  - Асинхронный API

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  Rust SDK обрабатывает события остановки и закрытия partition session внутренне; публичного API для настройки мягкого или жёсткого прерывания чтения пока нет.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Поддержка автомасштабирования топиков {#autoscaling}

{% note warning %}

Параметры `min_active_partitions` и `max_active_partitions` сами по себе не
включают автомасштабирование топика. Если не передать
`auto_partitioning_settings`, останется стратегия `DISABLED`, а число активных
партиций будет фиксированным. В YDB 26.2 метод `describe_topic()` в этом случае
возвращает эффективное значение `max_active_partitions`, равное
`min_active_partitions`.

Чтобы настроить разные минимальную и максимальную границы, дополнительно
задайте стратегию `TopicAutoPartitioningStrategy.SCALE_UP`, как в примере ниже.

{% endnote %}

{% list tabs group=lang %}

- C++

  SDK поддерживает два режима чтения топиков с включенным автомасштабированием: режим полной поддержки и режим совместимости. Режим чтения задаётся в параметрах создания сессии чтения. По умолчанию используется режим совместимости.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_autoscale_reader_full]-[END topic_autoscale_reader_full]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_autoscale_reader_compat]-[END topic_autoscale_reader_compat]" %}

  В режиме полной поддержки, когда все сообщения из партиции будут прочитаны, придёт событие `TEndPartitionSessionEvent`. После получения этого события в партиции больше не появится новых сообщений для чтения. Чтобы продолжить чтение из дочерних партиций, необходимо вызвать `Confirm()`, тем самым подтвердив, что приложение готово принимать сообщения из дочерних партиций. Если сообщения из всех партиций обрабатываются в одном потоке, то `Confirm()` можно вызвать сразу после получения `TEndPartitionSessionEvent`. Если обработка сообщений из разных партиций осуществляется в разных потоках, то следует завершить обработку сообщений, например, выполнить накопившийся батч, подтвердить их обработку (коммит) или сохранить позицию чтения в своей базе, и только после этого вызвать `Confirm()`.

  После получения `TEndPartitionSessionEvent` и обработки всех сообщений рекомендуется всегда сразу подтверждать их обработку (коммит). Это позволит сбалансировать чтение дочерних партиций между разными сессиями чтения, что приведёт к равномерному распределению нагрузки по всем читателям.

  Фрагмент цикла событий может выглядеть так:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_autoscale_end]-[END topic_autoscale_end]" %}

  В режиме совместимости отсутствует явный сигнал о завершении чтения из партиции, и сервер будет пытаться эвристически определить, что клиент обработал партицию до конца. Это может привести к задержке между завершением чтения из исходной партиции и началом чтения из её дочерних партиций.

  Если клиент подтверждает обработку сообщений (коммит), то сигналом завершения обработки сообщений из партиции будет подтверждение обработки последнего сообщения этой партиции. В случае, если клиент не подтверждает обработку сообщений, сервер будет периодически прерывать чтение из партиции и переключаться на чтение в другой сессии (если существуют другие сессии, готовые обрабатывать партицию). Это будет продолжаться до тех пор, пока чтение не [начнётся](#client-commit) с конца партиции.

  Рекомендуется проверять корректность обработки мягкого прерывания чтения: клиент должен обработать полученные сообщения, подтвердить их обработку (коммит) или сохранить позицию чтения в своей базе, и только после этого вызывать `Confirm()` для события `TStopPartitionSessionEvent`.

- Go

  Включение автомасштабирования топика во время его создания производится с помощью опции `topicoptions.CreateWithAutoPartitioningSettings`:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_create_basic]-[END topic_autoscale_create_basic]" %}

  При необходимости в AutoPartitioningSettings можно задать и другие параметры:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_create]-[END topic_autoscale_create]" %}

  Включение автомасштабирования у существующего топика производится с помощью опции `topicoptions.AlterWithAutoPartitioningStrategy` у `.Topic().Alter`:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_alter_basic]-[END topic_autoscale_alter_basic]" %}

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_alter]-[END topic_autoscale_alter]" %}

  SDK поддерживает два режима чтения топиков с включенным автомасштабированием: режим полной поддержки и режим совместимости. Режим чтения задаётся опцией `topicoptions.WithReaderSupportSplitMergePartitions` во время создания читателя. По умолчанию используется режим полной поддержки (`true`).

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_reader_full]-[END topic_autoscale_reader_full]" %}

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_reader_compat]-[END topic_autoscale_reader_compat]" %}

- Python

  Включение автомасштабирования топика во время его создания производится с помощью аргумента `auto_partitioning_settings` у `create_topic`:

  {% list tabs %}
  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_create]-[END topic_autoscale_create]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_autoscale_create]-[END topic_autoscale_create]" %}

  {% endlist %}

  Внесение изменений в существующий топик производится с помощью аргумента `alter_auto_partitioning_settings` у `alter_topic`:

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_alter]-[END topic_autoscale_alter]" %}

  SDK поддерживает два режима чтения топиков с включенным автомасштабированием: режим полной поддержки и режим совместимости. Режим чтения задаётся аргументом `auto_partitioning_support` во время создания читателя. По умолчанию используется режим полной поддержки.

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_reader_full]-[END topic_autoscale_reader_full]" %}

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_reader_compat]-[END topic_autoscale_reader_compat]" %}

  С практической точки зрения для конечного пользователя режимы не отличаются. Режим полной поддержки отличается от режима совместимости тем, кто гарантирует порядок чтения — клиент или сервер. Режим совместимости достигается серверной обработкой и, как правило, работает медленнее.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#311](https://github.com/ydb-platform/ydb-rs-sdk/issues/311)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Подтверждение обработки вне читателя {#commit-outside-the-reader}

Чаще всего подтверждение обработки удобно выполнять в рамках читателя, получающего сообщения. Однако существуют сценарии, при которых подтверждение обработки должно производиться процессом, отличным от процесса чтения. В таком случае необходим способ подтверждения, находящийся вне читателя.

{% list tabs group=lang %}

- C++

  Подтверждение обработки вне сессии чтения производится с помощью метода `NYdb::NTopic::TTopicClient::CommitOffset`:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  Если в момент подтверждения существует активная сессия чтения (например через `CreateReadSession`), рекомендуется передать её идентификатор с помощью опции `ReadSessionId` в `NYdb::NTopic::TCommitOffsetSettings`. Это позволяет серверу не прерывать текущую сессию чтения:

  В этой версии SDK `IReadSession::GetSessionId()` идентифицирует локальный объект SDK. Идентификатор активной серверной сессии берётся из статистики партиции, возвращаемой `DescribeConsumer`.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_commit_outside_session]-[END topic_commit_outside_session]" %}

- Go

  Подтверждение обработки вне читателя производится с помощью метода `db.Topic().CommitOffset`:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  Если в момент подтверждения существует активная сессия чтения (через `StartReader` или `StartListener`), рекомендуется передать её идентификатор с помощью опции `WithCommitOffsetReadSessionID`. Это позволяет серверу не прерывать текущую сессию чтения:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_commit_outside_session]-[END topic_commit_outside_session]" %}

- Python

  Подтверждение обработки вне читателя производится с помощью метода `topic_client.commit_offset`:

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  {% endlist %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Отслеживать прогресс или проголосовать за поддержку в Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
