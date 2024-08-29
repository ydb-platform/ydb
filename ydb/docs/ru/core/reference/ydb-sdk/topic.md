<!-- Этот файл не отслеживается автоматической системой перевода. Правки в EN-версию необходимо внести самостоятельно. -->
# Работа с топиками

В этой статье приведены примеры использования {{ ydb-short-name }} SDK для работы с [топиками](../../concepts/topic.md).

Перед выполнением примеров [создайте топик](../ydb-cli/topic-create.md) и [добавьте читателя](../ydb-cli/topic-consumer-add.md).

## Примеры работы с топиками

{% list tabs %}

- C++

  [Пример читателя на GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/topic_reader)

- Go

  [Примеры на GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/master/examples/topic)

- Java

  [Примеры на GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/master/ydb-cookbook/src/main/java/tech/ydb/examples/topic)

- Python

  [Примеры на GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/main/examples/topic)


{% endlist %}

## Инициализация соединения с топиками

{% list tabs %}

- C++

  Для работы с топиками создаются экземпляры драйвера YDB и клиента.

  Драйвер YDB отвечает за взаимодействие приложения и YDB на транспортном уровне. Драйвер должен существовать на всем протяжении жизненного цикла работы с топиками и должен быть инициализирован перед созданием клиента.

  Клиент сервиса топиков ([исходный код](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1589)) работает поверх драйвера YDB и отвечает за управляющие операции с топиками, а также создание сессий чтения и записи.

  Фрагмент кода приложения для инициализации драйвера YDB:
  ```cpp
  // Create driver instance.
  auto driverConfig = TDriverConfig()
      .SetEndpoint(opts.Endpoint)
      .SetDatabase(opts.Database)
      .SetAuthToken(GetEnv("YDB_TOKEN"));

  TDriver driver(driverConfig);
  ```
  В этом примере используется аутентификационный токен, сохранённый в переменной окружения `YDB_TOKEN`. Подробнее про [соединение с БД](../../concepts/connect.md) и [аутентификацию](../../concepts/auth.md).

  Фрагмент кода приложения для создания клиента:
  ```cpp
  TTopicClient topicClient(driver);
  ```

- Java

  Для работы с топиками создаются экземпляры транспорта YDB и клиента.

  Транспорт YDB отвечает за взаимодействие приложения и YDB на транспортном уровне. Он должен существовать на всем протяжении жизненного цикла работы с топиками и должен быть инициализирован перед созданием клиента.

  Фрагмент кода приложения для инициализации транспорта YDB:
  ```java
  try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
          .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
          .build()) {
      // Use YDB transport
  }
  ```
  В этом примере используется вспомогательный метод `CloudAuthHelper.getAuthProviderFromEnviron()`, получающий токен из переменных окружения.
  Например, `YDB_ACCESS_TOKEN_CREDENTIALS`.
  Подробнее про [соединение с БД](../../concepts/connect.md) и [аутентификацию](../../concepts/auth.md).

  Клиент сервиса топиков ([исходный код](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/TopicClient.java#L34)) работает поверх транспорта YDB и отвечает как за управляющие операции с топиками, так и за создание писателей и читателей.

  Фрагмент кода приложения для создания клиента:
  ```java
  try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {
    // Use topic client
  }
  ```
  В обоих примерах кода выше используется блок ([try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html)).
  Это позволяет автоматически закрывать клиент и транспорт при выходе из этого блока, т.к. оба являются наследниками `AutoCloseable`.

{% endlist %}

## Управление топиками {#manage}

### Создание топика {#create-topic}

Единственный обязательный параметр для создания топика - это его путь, остальные параметры опциональны.

{% list tabs %}

- C++

  Полный список настроек можно посмотреть [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L394).

  Пример создания топика c тремя партициями и поддержкой кодека ZSTD:

  ```cpp
  auto settings = NYdb::NTopic::TCreateTopicSettings()
      .PartitioningSettings(3, 3)
      .AppendSupportedCodecs(NYdb::NTopic::ECodec::ZSTD);

  auto status = topicClient
      .CreateTopic("my-topic", settings)  // returns TFuture<TStatus>
      .GetValueSync();
  ```

- Go

  Полный список поддерживаемых параметров можно посмотреть в [документации SDK](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций

  ```go
  err := db.Topic().Create(ctx, "topic-path",
    // optional
    topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

    // optional
    topicoptions.CreateWithMinActivePartitions(3),
  )
  ```

- Python

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций

  ```python
  driver.topic_client.create_topic(topic_path,
      supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
      min_active_partitions=3,                                    # optional
  )
  ```

- Java

  Полный список настроек можно посмотреть [в коде SDK](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/CreateTopicSettings.java#L97).

  Пример создания топика со списком поддерживаемых кодеков и минимальным количеством партиций

  ```java
  topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                  // Optional
                  .setSupportedCodecs(SupportedCodecs.newBuilder()
                          .addCodec(Codec.RAW)
                          .addCodec(Codec.GZIP)
                          .build())
                  // Optional
                  .setPartitioningSettings(PartitioningSettings.newBuilder()
                          .setMinActivePartitions(3)
                          .build())
                  .build());
  ```

{% endlist %}

### Изменение топика {#alter-topic}

{% list tabs %}

- C++

  При изменении топика в параметрах метода `AlterTopic` нужно указать путь топика и параметры, которые будут изменяться. Изменяемые параметры представлены структурой `TAlterTopicSettings`.

  Полный список настроек можно посмотреть [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

  Пример добавления [важного читателя](../../concepts/topic#important-consumer) к топику и установки [времени хранения сообщений](../../concepts/topic#retention-time) для топика в два дня:

  ```cpp
  auto alterSettings = NYdb::NTopic::TAlterTopicSettings()
      .BeginAddConsumer("my-consumer")
          .Important(true)
      .EndAddConsumer()
      .SetRetentionPeriod(TDuration::Days(2));

  auto status = topicClient
      .AlterTopic("my-topic", alterSettings)  // returns TFuture<TStatus>
      .GetValueSync();
  ```

- Go

  При изменении топика в параметрах нужно указать путь топика и те параметры, которые будут изменяться.

  Полный список поддерживаемых параметров можно посмотреть в [документации SDK](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#AlterOption).

  Пример добавления читателя к топику

  ```go
  err := db.Topic().Alter(ctx, "topic-path",
    topicoptions.AlterWithAddConsumers(topictypes.Consumer{
      Name:            "new-consumer",
      SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}, // optional
    }),
  )
  ```

- Python

  Пример изменения списка поддерживаемых кодеков и минимального количества партиций у топика 

  ```python
  driver.topic_client.alter_topic(topic_path,
      set_supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
      set_min_active_partitions=3,                                    # optional
  )
  ```

- Java

  При изменении топика в параметрах метода `alterTopic` нужно указать путь топика и параметры, которые будут изменяться.

  Полный список настроек можно посмотреть [в коде SDK](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/AlterTopicSettings.java#L23).


  ```java
  topicClient.alterTopic(topicPath, AlterTopicSettings.newBuilder()
                  .addAddConsumer(Consumer.newBuilder()
                          .setName("new-consumer")
                          .setSupportedCodecs(SupportedCodecs.newBuilder()
                                  .addCodec(Codec.RAW)
                                  .addCodec(Codec.GZIP)
                                  .build())
                          .build())
                  .build());

{% endlist %}

### Получение информации о топике {#describe-topic}

{% list tabs %}

- C++

  Для получения информации о топике используется метод `DescribeTopic`.

  Описание топика представлено структурой `TTopicDescription`.

  Полный список полей описания смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L163).

  Получить доступ к этому описанию можно так:

  ```cpp
  auto result = topicClient.DescribeTopic("my-topic").GetValueSync();
  if (result.IsSuccess()) {
      const auto& description = result.GetTopicDescription();
      std::cout << "Topic description: " << GetProto(description) << std::endl;
  }
  ```

  Существует отдельный метод для получения информации о читателе - `DescribeConsumer`.

- Go

  ```go
    descResult, err := db.Topic().Describe(ctx, "topic-path")
  if err != nil {
    log.Fatalf("failed describe topic: %v", err)
    return
  }
  fmt.Printf("describe: %#v\n", descResult)
  ```

- Python

  ```python
  info = driver.topic_client.describe_topic(topic_path)
  print(info)
  ```

- Java

  Для получения информации о топике используется метод `describeTopic`.

  Полный список полей описания можно посмотреть [в коде SDK](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/description/TopicDescription.java#L19).


  ```java
  Result<TopicDescription> topicDescriptionResult = topicClient.describeTopic(topicPath)
          .join();
  TopicDescription description = topicDescriptionResult.getValue();
  ```

{% endlist %}

### Удаление топика {#drop-topic}

Для удаления топика достаточно указать путь к нему.

{% list tabs %}

- C++

  ```cpp
  auto status = topicClient.DropTopic("my-topic").GetValueSync();
  ```

- Go

  ```go
    err := db.Topic().Drop(ctx, "topic-path")
  ```

- Python

  ```python
  driver.topic_client.drop_topic(topic_path)
  ```

- Java

  ```java
  topicClient.dropTopic(topicPath);
  ```

{% endlist %}

## Запись сообщений {#write}

### Подключение к топику для записи сообщений {#start-writer}

На данный момент поддерживается подключение только с совпадающими идентификаторами [источника и группы сообщений](../../concepts/topic#producer-id) (`producer_id` и `message_group_id`), в будущем это ограничение будет снято.

{% list tabs %}

- C++

  Подключение к топику на запись представлено объектом сессии записи с интерфейсом `IWriteSession` или `ISimpleBlockingWriteSession` (вариант для простой записи по одному сообщению без подтверждения, блокирующейся при превышении числа inflight записей или размера буфера SDK). Настройки сессии записи представлены структурой `TWriteSessionSettings`, для варианта `ISimpleBlockingWriteSession` часть настроек не поддерживается.

  Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1199).

  Пример создания сессии записи с интерфейсом `IWriteSession`.

  ```cpp
  TString producerAndGroupID = "group-id";
  auto settings = TWriteSessionSettings()
      .Path("my-topic")
      .ProducerId(producerAndGroupID)
      .MessageGroupId(producerAndGroupID);

  auto session = topicClient.CreateWriteSession(settings);
  ```

- Go

  ```go
  producerAndGroupID := "group-id"
  writer, err := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
  )
  if err != nil {
      return err
  }
  ```

- Python

  ```python
  writer = driver.topic_client.writer(topic_path)
  ```

- Java (sync)

  Инициализация настроек писателя:
  ```java
  String producerAndGroupID = "group-id";
  WriterSettings settings = WriterSettings.newBuilder()
        .setTopicPath(topicPath)
        .setProducerId(producerAndGroupID)
        .setMessageGroupId(producerAndGroupID)
        .build();
  ```

  Создание синхронного писателя:
  ```java
  SyncWriter writer = topicClient.createSyncWriter(settings);
  ```

  После создания писателя его необходимо инициализировать. Для этого есть два метода:

  - `init()`: неблокирующий, запускает процесс инициализации в фоне и не ждёт его завершения.
    ```java
    writer.init();
    ```
  - `initAndWait()`: блокирующий, запускает процесс инициализации и ждёт его завершения. Если в процессе инициализации возникла ошибка, будет брошено исключение.
    ```java
    try {
        writer.initAndWait();
        logger.info("Init finished succsessfully");
    } catch (Exception exception) {
        logger.error("Exception while initializing writer: ", exception);
        return;
    }
    ```

- Java (async)

  Инициализация настроек писателя:
  ```java
  String producerAndGroupID = "group-id";
  WriterSettings settings = WriterSettings.newBuilder()
        .setTopicPath(topicPath)
        .setProducerId(producerAndGroupID)
        .setMessageGroupId(producerAndGroupID)
        .build();
  ```

  Создание и инициализация асинхронного писателя:
  ```java
  AsyncWriter writer = topicClient.createAsyncWriter(settings);

  // Init in background
  writer.init()
          .thenRun(() -> logger.info("Init finished successfully"))
          .exceptionally(ex -> {
              logger.error("Init failed with ex: ", ex);
              return null;
          });
  ```

{% endlist %}

### Запись сообщений {#writing-messages}

{% list tabs %}

- C++

  Асинхронная запись возможна через интерфейс `IWriteSession`.

  Работа пользователя с объектом `IWriteSession` в общем устроена как обработка цикла событий с тремя типами событий: `TReadyToAcceptEvent`, `TAcksEvent` и `TSessionClosedEvent`.

  Для каждого из типов событий можно установить обработчик этого события, а также можно установить общий обработчик. Обработчики устанавливаются в настройках сессии записи перед её созданием.

  Если обработчик для некоторого события не установлен, его необходимо получить и обработать в методах `GetEvent` / `GetEvents`. Для неблокирующего ожидания очередного события есть метод `WaitEvent` с интерфейсом `TFuture<void>()`.

  Для записи каждого сообщения пользователь должен "потратить" move-only объект `TContinuationToken`, который выдаёт SDK с событием `TReadyToAcceptEvent`. При записи сообщения можно установить пользовательские seqNo и временную метку создания, но по умолчанию их проставляет SDK автоматически.

  По умолчанию `Write` выполняется асинхронно - данные из сообщений вычитываются и сохраняются во внутренний буфер, отправка происходит в фоне в соответствии с настройками `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. Сессия сама переподключается к YDB при обрывах связи и повторяет отправку сообщений пока это возможно, в соответствии с настройкой `RetryPolicy`. При получении ошибки, которую невозможно повторить, сессия чтения отправляет пользователю `TSessionClosedEvent` с диагностической информацией.

  Так может выглядеть запись нескольких сообщений в цикле событий без использования обработчиков:
  ```cpp
  // Event loop
  while (true) {
      // Get event
      // May block for a while if write session is busy
      TMaybe<TWriteSessionEvent::TEvent> event = session->GetEvent(/*block=*/true);

      if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
          session->Write(std::move(event.ContinuationToken), "This is yet another message.");

      } else if (auto* ackEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
          std::cout << ackEvent->DebugString() << std::endl;

      } else if (auto* closeSessionEvent = std::get_if<TSessionClosedEvent>(&*event)) {
          break;
      }
  }
  ```

- Go

  Для отправки сообщения - достаточно в поле Data сохранить Reader, из которого можно будет прочитать данные. Можно рассчитывать на то что данные каждого сообщения читаются один раз (или до первой ошибки), к моменту возврата из Write данные будут уже прочитаны и сохранены во внутренний буфер.

  SeqNo и дата создания сообщений по умолчанию проставляются автоматически.

  По умолчанию Write выполняется асинхронно - данные из сообщений вычитываются и сохраняются во внутренний буфер, отправка происходит в фоне. Writer сам переподключается к YDB при обрывах связи и повторяет отправку сообщений пока это возможно. При получении ошибки, которую невозможно повторить Writer останавливается и следующие вызовы Write будут завершаться с ошибкой.

  ```go
  err := writer.Write(ctx,
    topicwriter.Message{Data: strings.NewReader("1")},
    topicwriter.Message{Data: bytes.NewReader([]byte{1,2,3})},
    topicwriter.Message{Data: strings.NewReader("3")},
  )
  if err == nil {
    return err
  }
  ```

- Python

  Для отправки сообщений можно передавать как просто содержимое сообщения (bytes, str), так и вручную задавать некоторые свойства. Объекты можно передавать по одному или сразу в массиве (list). Метод `write` выполняется асинхронно. Возврат из метода происходит сразу после того как сообщения будут положены во внутренний буфер клиента, обычно это происходит быстро. Ожидание может возникнуть, если внутренний буфер уже заполнен и нужно подождать, пока часть данных будет отправлена на сервер.

  ```python
  # Простая отправка сообщений, без явного указания метаданных.
  # Удобно начинать, удобно использовать пока важно только содержимое сообщения.
  writer = driver.topic_client.writer(topic_path)
  writer.write("mess")  # Строки будут переданы в кодировке utf-8, так удобно отправлять
                        # текстовые сообщения.
  writer.write(bytes([1, 2, 3]))  # Эти байты будут отправлены "как есть", так удобно отправлять
                                  # бинарные данные.
  writer.write(["mess-1", "mess-2"])  # Здесь за один вызов отправляется несколько сообщений —
                                      # так снижаются накладные расходы на внутренние процессы SDK,
                                      # имеет смысл при большом потоке сообщений.

  # Полная форма, используется, когда кроме содержимого сообщения нужно вручную задать и его свойства.
  writer = driver.topic_client.writer(topic="topic-path", auto_seqno=False, auto_created_at=False)

  writer.write(ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()))
  writer.write(ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now()))

  # В полной форме так же можно отправлять несколько сообщений за один вызов функции.
  # Это имеет смысл при большом потоке отправляемых сообщений — для снижения
  # накладных расходов на внутренние вызовы SDK.
  writer.write([
    ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()),
    ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now(),
    ])

  ```

- Java (sync)

  Метод `send` блокирует управление, пока сообщение не будет помещено в очередь отправки.
  Попадание сообщения в эту очередь означает, что писатель сделает всё возможное для доставки сообщения.
  Например, если сессия записи по какой-то причине оборвётся, писатель переустановит соединение и попробует отправить это сообщение на новой сессии.
  Но попадание сообщения в очередь отправки не гарантирует того, что сообщение в итоге будет записано.
  Например, могут возникать ошибки, приводящие к завершению работы писателя до того, как сообщения из очереди будут отправлены.
  Если нужно подтверждение успешной записи для каждого сообщения, используйте асинхронного писателя и проверяйте статус, возвращаемый методом `send`.
  ```java
  writer.send(Message.of("11".getBytes()));

  long timeoutSeconds = 5; // How long should we wait for a message to be put into sending buffer
  try {
      writer.send(
              Message.newBuilder()
                      .setData("22".getBytes())
                      .setCreateTimestamp(Instant.now().minusSeconds(5))
                      .build(),
              timeoutSeconds,
              TimeUnit.SECONDS
      );
  } catch (TimeoutException exception) {
      logger.error("Send queue is full. Couldn't put message into sending queue within {} seconds", timeoutSeconds);
  } catch (InterruptedException | ExecutionException exception) {
      logger.error("Couldn't put the message into sending queue due to exception: ", exception);
  }
  ```

- Java (async)

  Метод `send` в асинхронном клиенте неблокирующий. Помещает сообщение в очередь отправки.
  Метод возвращает `CompletableFuture<WriteAck>`, позволяющую проверить, действительно ли сообщение было записано.
  В случае, если очередь переполнена, будет брошено исключение QueueOverflowException.
  Это способ сигнализировать пользователю о том, что поток записи следует притормозить.
  В таком случае стоит или пропускать сообщения, или выполнять повторные попытки записи через exponential backoff.
  Также можно увеличить размер клиентского буфера (`setMaxSendBufferMemorySize`), чтобы обрабатывать больший объем сообщений перед тем, как он заполнится.
  ```java
  try {
      // Non-blocking. Throws QueueOverflowException if send queue is full
      writer.send(Message.of("33".getBytes()));
  } catch (QueueOverflowException exception) {
      // Send queue is full. Need to retry with backoff or skip
  }
  ```

{% endlist %}

### Запись сообщений с подтверждением о сохранении на сервере

{% list tabs %}

- C++

  Получение подтверждений от сервера возможно через интерфейс `IWriteSession`.

  Ответы о записи сообщений на сервере приходят клиенту SDK в виде событий `TAcksEvent`. В одном событии могут содержаться ответы о нескольких отправленных ранее сообщениях. Варианты ответа: запись подтверждена (`EES_WRITTEN`), запись отброшена как дубликат ранее записанного сообщения (`EES_ALREADY_WRITTEN`) или запись отброшена по причине сбоя (`EES_DISCARDED`).

  Пример установки обработчика TAcksEvent для сессии записи:
  ```cpp
  auto settings = TWriteSessionSettings()
    // other settings are set here
    .EventHandlers(
      TWriteSessionSettings::TEventHandlers()
        .AcksHandler(
          [&](TWriteSessionEvent::TAcksEvent& event) {
            for (const auto& ack : event.Acks) {
              if (ack.State == TWriteAck::EEventState::EES_WRITTEN) {
                ackedSeqNo.insert(ack.SeqNo);
                std::cout << "Acknowledged message with seqNo " << ack.SeqNo << std::endl;
              }
            }
          }
        )
    );

  auto session = topicClient.CreateWriteSession(settings);
  ```

  В такой сессии записи события `TAcksEvent` не будут приходить пользователю в `GetEvent` / `GetEvents`, вместо этого SDK при получении подтверждений от сервера будет вызывать переданный обработчик. Аналогично можно настраивать обработчики на остальные типы событий.

- Go

  При подключении можно указать опцию синхронной записи сообщений - topicoptions.WithSyncWrite(true). Тогда Write будет возвращаться только после того как получит подтверждение с сервера о сохранении всех, сообщений переданных в вызове. При этом SDK так же как и обычно будет при необходимости переподключаться и повторять отправку сообщений. В этом режиме контекст управляет только временем ожидания ответа из SDK, т.е. даже после отмены контекста SDK продолжит попытки отправить сообщения.

  ```go

  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithSyncWrite(true),
  )

  err = writer.Write(ctx,
    topicwriter.Message{Data: strings.NewReader("1")},
    topicwriter.Message{Data: bytes.NewReader([]byte{1,2,3})},
    topicwriter.Message{Data: strings.NewReader("3")},
  )
  if err == nil {
    return err
  }
  ```

- Python

  Есть два способа получить подтверждение о записи сообщений на сервере:

  * `flush()` — дожидается подтверждения для всех сообщений, записанных ранее во внутренний буфер.
  * `write_with_ack(...)` — отправляет сообщение и ждет подтверждение его доставки от сервера. При отправке нескольких сообщений подряд это способ работает медленно.

  ```python
  # Положить несколько сообщений во внутренний буфер, затем дождаться,
  # пока все они будут доставлены до сервера.
  for mess in messages:
      writer.write(mess)

  writer.flush()

  # Можно отправить несколько сообщений и дождаться подтверждения на всю группу.
  writer.write_with_ack(["mess-1", "mess-2"])

  # Ожидание при отправке каждого сообщения — этот метод вернет результат только после получения
  # подтверждения от сервера.
  # Это самый медленный вариант отправки сообщений, используйте его только если такой режим
  # действительно нужен.
  writer.write_with_ack("message")

  ```

- Java (async)

  Метод `send` возвращает `CompletableFuture<WriteAck>`. Её успешное завершение означает подтверждение записи сервером.
  В структуре `WriteAck` содержится информация о seqNo, offset и статусе записи:

  ```java
  writer.send(Message.of(message))
          .whenComplete((result, ex) -> {
              if (ex != null) {
                  logger.error("Exception on writing message message: ", ex);
              } else {
                  switch (result.getState()) {
                      case WRITTEN:
                          WriteAck.Details details = result.getDetails();
                          StringBuilder str = new StringBuilder("Message was written successfully");
                          if (details != null) {
                              str.append(", offset: ").append(details.getOffset());
                          }
                          logger.debug(str.toString());
                          break;
                      case ALREADY_WRITTEN:
                          logger.warn("Message has already been written");
                          break;
                      default:
                          break;
                  }
              }
          });
  ```

{% endlist %}

### Выбор кодека для сжатия сообщений {#codec}

Подробнее о [сжатии данных в топиках](../../concepts/topic#message-codec).

{% list tabs %}

- C++

  Сжатие, которое используется при отправке сообщений методом `Write`, задаётся при [создании сессии записи](#start-writer) настройками `Codec` и `CompressionLevel`. По умолчанию выбирается кодек GZIP.
  Пример создания сессии записи без сжатия сообщений:

  ```cpp
  auto settings = TWriteSessionSettings()
    // other settings are set here
    .Codec(ECodec::RAW);

  auto session = topicClient.CreateWriteSession(settings);
  ```

  Если необходимо в рамках сессии записи отправить сообщение, сжатое другим кодеком, можно использовать метод `WriteEncoded` с указанием кодека и размера расжатого сообщения. Для успешной записи этим способом используемый кодек должен быть разрешён в настройках топика.


- Go

  По умолчанию SDK выбирает кодек автоматически (с учетом настроек топика). В автоматическом режиме SDK сначала отправляет по одной группе сообщений каждым из разрешенных кодеков, затем иногда будет пробовать сжать сообщения всеми доступными кодеками и выбирать кодек, дающий наименьший размер сообщения. Если для топика список разрешенных кодеков пуст, то автовыбор производится между Raw и Gzip-кодеками.

  При необходимости можно задать фиксированный кодек в опциях подключения. Тогда будет использоваться именно он и замеры проводиться не будут.

  ```go
  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithCodec(topictypes.CodecGzip),
  )
  ```

- Python

  По умолчанию SDK выбирает кодек автоматически (с учетом настроек топика). В автоматическом режиме SDK сначала отправляет по одной группе сообщений каждым из разрешенных кодеков, затем иногда будет пробовать сжать сообщения всеми доступными кодеками и выбирать кодек, дающий наименьший размер сообщения. Если для топика список разрешенных кодеков пуст, то автовыбор производится между Raw и Gzip-кодеками.

  При необходимости можно задать фиксированный кодек в опциях подключения. Тогда будет использоваться именно он и замеры проводиться не будут.

  ```python
  writer = driver.topic_client.writer(topic_path,
      codec=ydb.TopicCodec.GZIP,
  )
  ```

- Java

  ```java
  String producerAndGroupID = "group-id";
  WriterSettings settings = WriterSettings.newBuilder()
          .setTopicPath(topicPath)
          .setProducerId(producerAndGroupID)
          .setMessageGroupId(producerAndGroupID)
          .setCodec(Codec.ZSTD)
          .build();
  ```

{% endlist %}

### Запись сообщений без дедупликации {#nodedup}

Подробнее о записи без дедупликации — в [соответствующем разделе концепций](../../concepts/topic#no-dedup).

{% list tabs %}

- C++

  Если в настройках сессии записи не указывается опция `ProducerId`, будет создана сессия записи без дедупликации.
  Пример создания такой сессии записи:

  ```cpp
  auto settings = TWriteSessionSettings()
      .Path(myTopicPath);

  auto session = topicClient.CreateWriteSession(settings);
  ```

  Для включения дедупликации нужно в настройках сессии записи указать опцию `ProducerId` или явно включить дедупликацию, вызвав метод `EnableDeduplication()`, например, как в секции ["Подключение к топику"](#start-writer).

{% endlist %}

### Запись метаданных на уровне сообщения {#messagemeta}

При записи сообщения можно дополнительно указать метаданные как список пар "ключ-значение". Эти данные будут доступны при вычитывании сообщения.
Ограничение на размер метаданных — не более 1000 ключей.

{% list tabs %}

- C++

  Воспользоваться функцией записи метаданных можно с помощью метода `Write()`, принимающего `TWriteMessage` объект:

  ```cpp
  auto settings = TWriteSessionSettings()
      .Path(myTopicPath)
  //set all oter settings;
  ;

  auto session = topicClient.CreateWriteSession(settings);

  TMaybe<TWriteSessionEvent::TEvent> event = session->GetEvent(/*block=*/true);
  TWriteMessage message("This is yet another message").MessageMeta({
      {"meta-key", "meta-value"},
      {"another-key", "value"}
  });

  if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
      session->Write(std::move(event.ContinuationToken), std::move(message));
  }

  ```

- Java

  При конструировании сообщения для записи с помощью Builder'а, ему можно передать объекты типа `MetadataItem` с парой ключ типа `String` + значение типа `byte[]`.

  Можно передать сразу `List` таких объектов:

  ```java
  List<MetadataItem> metadataItems = Arrays.asList(
          new MetadataItem("meta-key", "meta-value".getBytes()),
          new MetadataItem("another-key", "value".getBytes())
  );
  writer.send(
          Message.newBuilder()
                  .setMetadataItems(metadataItems)
                  .build()
  );
  ```

  Или добавлять каждый `MetadataItem` отдельно:

  ```java
  writer.send(
          Message.newBuilder()
                  .addMetadataItem(new MetadataItem("meta-key", "meta-value".getBytes()))
                  .addMetadataItem(new MetadataItem("another-key", "value".getBytes()))
                  .build()
  );
  ```

  При чтении эти метаданные сообщения получить, вызвав на нём метод `getMetadataItems()`:

  ```java
  Message message = reader.receive();
  List<MetadataItem> metadata = message.getMetadataItems();
  ```

{% endlist %}

### Запись в транзакции {#write-tx}

{% list tabs %}

- Java (sync)

  [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionWriteSync.java)

  В настройках `SendSettings` метода `send` можно указать транзакцию.
  Тогда сообщение будет записано вместе с коммитом этой транзакцией.

  ```java
  // creating a session in the table service
  Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
  if (!sessionResult.isSuccess()) {
      logger.error("Couldn't get a session from the pool: {}", sessionResult);
      return; // retry or shutdown
  }
  Session session = sessionResult.getValue();
  // creating a transaction in the table service
  // this transaction is not yet active and has no id
  TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

  // get message text within the transaction
  Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
          .join();
  if (!dataQueryResult.isSuccess()) {
      logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
      return; // retry or shutdown
  }
  // now the transaction is active and has an id

  ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
  byte[] message;
  if (rsReader.next()) {
      message = rsReader.getColumn(0).getBytes();
  } else {
      return; // retry or shutdown
  }

  writer.send(
          Message.of(message),
          SendSettings.newBuilder()
                  .setTransaction(transaction)
                  .build()
  );

  // flush to wait until all messages reach server before commit
  writer.flush();

  Status commitStatus = transaction.commit().join();
  analyzeCommitStatus(commitStatus);
  ```

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

- Java (async)

  [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionWriteAsync.java)

  В настройках `SendSettings` метода `send` можно указать транзакцию.
  Тогда сообщение будет записано вместе с коммитом этой транзакцией.

  ```java
  // creating a session in the table service
  Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
  if (!sessionResult.isSuccess()) {
      logger.error("Couldn't get a session from the pool: {}", sessionResult);
      return; // retry or shutdown
  }
  Session session = sessionResult.getValue();
  // creating a transaction in the table service
  // this transaction is not yet active and has no id
  TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

  // get message text within the transaction
  Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
          .join();
  if (!dataQueryResult.isSuccess()) {
      logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
      return; // retry or shutdown
  }
  // now the transaction is active and has an id

  ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
  byte[] message;
  if (rsReader.next()) {
      message = rsReader.getColumn(0).getBytes();
  } else {
      return; // retry or shutdown
  }

  try {
      writer.send(Message.newBuilder()
                              .setData(message)
                              .build(),
                      SendSettings.newBuilder()
                              .setTransaction(transaction)
                              .build())
              .whenComplete((result, ex) -> {
                  if (ex != null) {
                      logger.error("Exception while sending a message: ", ex);
                  } else {
                      switch (result.getState()) {
                          case WRITTEN:
                              WriteAck.Details details = result.getDetails();
                              logger.info("Message was written successfully, offset: " + details.getOffset());
                              break;
                          case ALREADY_WRITTEN:
                              logger.info("Message has already been written");
                              break;
                          default:
                              break;
                      }
                  }
              })
              // Waiting for the message to reach the server before committing the transaction
              .join();

      Status commitStatus = transaction.commit().join();
      analyzeCommitStatus(commitStatus);
  } catch (QueueOverflowException exception) {
      logger.error("Queue overflow exception while sending a message{}: ", index, exception);
      // Send queue is full. Need to retry with backoff or skip
  }
  ```

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

{% endlist %}


## Чтение сообщений {#reading}

### Подключение к топику для чтения сообщений {#start-reader}

Для чтения сообщений из топика необходимо наличие заранее созданного Consumer, связанного с этим топиком.
Создать Consumer можно при [создании](#create-topic) или [изменении](#alter-topic) топика.
У топика может быть несколько Consumer'ов и для каждого из них сервер хранит свой прогресс чтения.

{% list tabs %}

- C++

  Подключение для чтения из одного или нескольких топиков представлено объектом сессии чтения с интерфейсом `IReadSession`. Настройки сессии чтения представлены структурой `TReadSessionSettings`.

  Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1344).

  Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

  ```cpp
  auto settings = TReadSessionSettings()
      .ConsumerName("my-consumer")
      .AppendTopics("my-topic");

  auto session = topicClient.CreateReadSession(settings);
  ```

- Go

  Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

  ```go
  reader, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"))
  if err != nil {
      return err
  }
  ```

- Python

  Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

  ```python
  reader = driver.topic_client.reader(topic="topic-path", consumer="consumer_name")
  ```

- Java (sync)

  Инициализация настроек читателя
  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                  .setMaxLag(Duration.ofMinutes(30)) // Optional
                  .build())
          .build();
  ```

  Создание синхронного читателя
  ```java
  SyncReader reader = topicClient.createSyncReader(settings);
  ```

  После создания синхронного читателя необходимо инициализировать. Для этого следует воспользоваться одним их двух методов:
  - `init()`: неблокирующий, запускает процесс инициализации в фоне и не ждёт его завершения.
    ```java
    reader.init();
    ```
  - `initAndWait()`: блокирующий, запускает процесс инициализации и ждёт его завершения. Если в процессе инициализации возникла ошибка, будет брошено исключение.
    ```java
    try {
        reader.initAndWait();
        logger.info("Init finished succsessfully");
    } catch (Exception exception) {
        logger.error("Exception while initializing reader: ", exception);
        return;
    }
    ```

- Java (async)

  Инициализация настроек читателя
  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                  .setMaxLag(Duration.ofMinutes(30)) // Optional
                  .build())
          .build();
  ```

  Для асинхронного читателя, помимо общих настроек чтения `ReaderSettings`, понадобятся настройки обработчика событий `ReadEventHandlersSettings`, в которых необходимо передать экземпляр наследника `ReadEventHandler`.
  Он будет описывать, как должна происходить обработка различных событий, происходящих во время чтения.

  ```java
  ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
          .setEventHandler(new Handler())
          .build();
  ```

  Опционально, в `ReadEventHandlersSettings` можно указать executor'а, на котором будет происходить обработка сообщений.
  Для реализации объекта-наследника ReadEventHandler можно воспользоваться дефолтным абстрактным классом `AbstractReadEventHandler`.
  Достаточно переопределить метод onMessages, отвечающий за обработку самих сообщений. Пример реализации:

  ```java
  private class Handler extends AbstractReadEventHandler {
      @Override
      public void onMessages(DataReceivedEvent event) {
          for (Message message : event.getMessages()) {
              StringBuilder str = new StringBuilder();
              logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());

              process(message);

              message.commit().thenRun(() -> {
                  logger.info("Message committed");
              });
          }
      }
  }
  ```

  Создание и инициализация асинхронного читателя:
  ```java
  AsyncReader reader = topicClient.createAsyncReader(readerSettings, handlerSettings);
  // Init in background
  reader.init()
          .thenRun(() -> logger.info("Init finished successfully"))
          .exceptionally(ex -> {
              logger.error("Init failed with ex: ", ex);
              return null;
          });
  ```

{% endlist %}

Вы также можете использовать расширенный вариант создания подключения, чтобы указать несколько топиков и задать параметры чтения. Следующий код создаст подключение к топикам `my-topic` и `my-specific-topic` через читателя `my-consumer`:

{% list tabs %}

- C++

  ```cpp
  auto settings = TReadSessionSettings()
      .ConsumerName("my-consumer")
      .AppendTopics("my-topic")
      .AppendTopics(
          TTopicReadSettings("my-specific-topic")
              .ReadFromTimestamp(someTimestamp)
      );

  auto session = topicClient.CreateReadSession(settings);
  ```

- Go

  ```go
  reader, err := db.Topic().StartReader("my-consumer", []topicoptions.ReadSelector{
      {
          Path: "my-topic",
      },
      {
          Path:       "my-specific-topic",
          ReadFrom:   time.Date(2022, 7, 1, 10, 15, 0, 0, time.UTC),
      },
      },
  )
  if err != nil {
      return err
  }
  ```

  Также в примере выше задаётся время, с которого следует начинать читать сообщения.

- Python

  Функциональность находится в разработке.

- Java

  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath("my-topic")
                  .build())
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath("my-specific-topic")
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                  .setMaxLag(Duration.ofMinutes(30)) // Optional
                  .build())
          .build();
  ```

{% endlist %}

### Чтение сообщений {#reading-messages}

Сервер хранит [позицию чтения сообщений](../../concepts/topic.md#consumer-offset). После вычитывания очередного сообщения клиент может [отправить на сервер подтверждение обработки](#commit). Позиция чтения изменится, а при новом подключении будут вычитаны только неподтвержденные сообщения.

Читать сообщения можно и [без подтверждения обработки](#no-commit). В этом случае при новом подключении будут прочитаны все неподтвержденные сообщения, в том числе и уже обработанные.

Информацию о том, какие сообщения уже обработаны, можно [сохранять на клиентской стороне](#client-commit), передавая на сервер стартовую позицию чтения при создании подключения. При этом позиция чтения сообщений на сервере не изменяется.

Можно использовать [транзакции](#read-tx). В этом случае позиция чтения изменится при подтверждении транзакции. При новом подключении будут прочитаны все неподтверждённые сообщения.

{% list tabs %}

- C++

  Работа пользователя с объектом `IReadSession` в общем устроена как обработка цикла событий со следующими типами событий: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent` и `TSessionClosedEvent`.

  Для каждого из типов событий можно установить обработчик этого события, а также можно установить общий обработчик. Обработчики устанавливаются в настройках сессии записи перед её созданием.

  Если обработчик для некоторого события не установлен, его необходимо получить и обработать в методах `GetEvent` / `GetEvents`. Для неблокирующего ожидания очередного события есть метод `WaitEvent` с сигнатурой `TFuture<void>()`.

- Go

  SDK получает данные с сервера партиями и буферизирует их. В зависимости от задач клиентский код может читать сообщения из буфера по одному или пакетами.

- Python

  SDK получает данные с сервера партиями и буферизирует их. В зависимости от задач клиентский код может читать сообщения из буфера по одному или пакетами.

- Java

  SDK получает данные с сервера партиями и буферизирует их. В зависимости от задач клиентский код может читать сообщения из буфера по одному или пакетами.

{% endlist %}


### Чтение без подтверждения обработки сообщений {#no-commit}

#### Чтение сообщений по одному

{% list tabs %}

- C++

  Чтение сообщений по одному в C++ SDK не предусмотрено. Событие `TDataReceivedEvent` содержит пакет сообщений.

- Go

  ```go
  func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
      for {
          mess, err := r.ReadMessage(ctx)
          if err != nil {
              return err
          }
          processMessage(mess)
      }
  }
  ```

- Python

  ```python
  while True:
      message = reader.receive_message()
      process(message)
  ```

- Java (sync)

  Чтобы читать сообщения без подтверждения обработки, по одному, используйте следующий код:

  ```java
  while(true) {
      Message message = reader.receive();
      process(message);
  }
  ```

- Java (async)

  В асинхронном клиенте нет возможности читать сообщения по одному.

{% endlist %}

#### Чтение сообщений пакетом

{% list tabs %}

- C++

  При установке сессии чтения с настройкой `SimpleDataHandlers` достаточно передать обработчик для сообщений с данными. SDK будет вызывать этот обработчик на каждый принятый от сервера пакет сообщений.  Подтверждения чтения по умолчанию отправляться не будут.

  ```cpp
  auto settings = TReadSessionSettings()
      .EventHandlers_.SimpleDataHandlers(
          [](TReadSessionEvent::TDataReceivedEvent& event) {
              std::cout << "Get data event " << DebugString(event);
          }
      );

  auto session = topicClient.CreateReadSession(settings);

  // Wait SessionClosed event.
  ReadSession->GetEvent(/* block = */true);
  ```

  В этом примере после создания сессии основной поток дожидается завершения сессии со стороны сервера в методе `GetEvent`, другие типы событий приходить не будут.


- Go

  ```go
  func SimpleReadBatches(ctx context.Context, r *topicreader.Reader) error {
      for {
          batch, err := r.ReadMessageBatch(ctx)
          if err != nil {
              return err
          }
          processBatch(batch)
      }
  }
  ```

- Python

  ```python
  while True:
    batch = reader.receive_batch()
    process(batch)
  ```

- Java (sync)

  В синхронном клиенте нет возможности прочитать сразу пакет сообщений.

- Java (async)

  Чтобы прочитать пакет сообщений без подтверждения обработки, используйте следующий код:

  ```java
  private class Handler extends AbstractReadEventHandler {
      @Override
      public void onMessages(DataReceivedEvent event) {
          for (Message message : event.getMessages()) {
              process(message);
          }
      }
  }
  ```

{% endlist %}

### Чтение с подтверждением обработки сообщений {#commit}

Подтверждение обработки сообщения (коммит) - сообщает серверу, что сообщение из топика обработано получателем и больше его отправлять не нужно. При использовании чтения с подтверждением нужно подтверждать все полученные сообщения без пропуска. Коммит сообщений на сервере происходит после подтверждения очередного интервала сообщений «без дырок», сами подтверждения при этом можно отправлять в любом порядке.

Например с сервера пришли сообщения 1, 2, 3. Программа обрабатывает их параллельно и отправляет подтверждения в таком порядке: 1, 3, 2. В этом случае сначала будет закоммичено сообщение 1, а сообщения 2 и 3 будут закоммичены только после того как сервер получит подтверждение об обработке сообщения 2.

В случае ошибки на коммите сообщения можно написать эту ошибку в лог и продолжить работу. Состояние сообщения в этой точке неизвестно. Сообщение могло закоммититься, а потом возникла сетевая ошибка и клиент не получил подтверждения. Если сообщение не закоммитилось, то оно будет прочитано ещё раз и снова поступит в обработку (может быть на другом читателе). Ретраить именно коммит смысла нет, т.к. сессия чтения этого сообщения уже потеряна.

#### Чтение сообщений по одному с подтверждением

{% list tabs %}

- C++

  Чтение сообщений по одному в C++ SDK не предусмотрено. Событие `TDataReceivedEvent` содержит пакет сообщений.

- Go

  ```go
  func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
      for {
        mess, err := r.ReadMessage(ctx)
        if err != nil {
            return err
        }
        processMessage(mess)
        r.Commit(mess.Context(), mess)
      }
  }
  ```

- Python

  ```python
  while True:
      message = reader.receive_message()
      process(message)
      reader.commit(message)
  ```

- Java

  Для подтверждения обработки сообщения достаточно вызвать у сообщения метод `commit`.
  Актуально как для синхронного, так и для асинхронного читателя.
  В асинхронном читателе, при обработке пакета сообщений, можно вызвать `commit` или у всего пакета сразу, или у каждого сообщения отдельно.
  Этот метод возвращает `CompletableFuture<Void>`, успешное выполнение которой означает подтверждение обработки сервером.
  В случае ошибки коммита не следует пытаться его ретраить. Скорее всего, ошибка вызвана закрытием сессии.
  Читатель (необязательно этот же) сам создаст новую сессию для этой партиции и сообщение будет прочитано снова.

  ```java
  message.commit()
         .whenComplete((result, ex) -> {
             if (ex != null) {
                 // Read session was probably closed, there is nothing we can do here.
                 // Do not retry this commit on the same event.
                 logger.error("exception while committing message: ", ex);
             } else {
                 logger.info("message committed successfully");
             }
         });
  ```

{% endlist %}

#### Чтение сообщений пакетом с подтверждением

{% list tabs %}

- C++

  Аналогично [примеру выше](#no-commit), при установке сессии чтения с настройкой `SimpleDataHandlers` достаточно передать обработчик для сообщений с данными. SDK будет вызывать этот обработчик на каждый принятый от сервера пакет сообщений. Передача параметра `commitDataAfterProcessing = true` означает, что SDK будет отправлять на сервер подтверждения чтения всех сообщений после выполнения обработчика.

  ```cpp
  auto settings = TReadSessionSettings()
      .EventHandlers_.SimpleDataHandlers(
          [](TReadSessionEvent::TDataReceivedEvent& event) {
              std::cout << "Get data event " << DebugString(event);
          }
          , /* commitDataAfterProcessing = */true
      );

  auto session = topicClient.CreateReadSession(settings);

  // Wait SessionClosed event.
  ReadSession->GetEvent(/* block = */true);
  ```

- Go

  ```go
  func SimpleReadMessageBatch(ctx context.Context, r *topicreader.Reader) error {
      for {
        batch, err := r.ReadMessageBatch(ctx)
        if err != nil {
            return err
        }
        processBatch(batch)
        r.Commit(batch.Context(), batch)
      }
  }
  ```

- Python

  ```python
  while True:
    batch = reader.receive_batch()
    process(batch)
    reader.commit(batch)
  ```

- Java (sync)

  Неактуально, т.к. в синхронном читателе нет возможности читать сообщения пакетами.

- Java (async)

  В обработчике `onMessage` можно закоммитить весь пакет сообщений, вызвав `commit` на событии.
  ```java
  @Override
  public void onMessages(DataReceivedEvent event) {
      for (Message message : event.getMessages()) {
          process(message);
      }
      event.commit()
             .whenComplete((result, ex) -> {
                 if (ex != null) {
                     // Read session was probably closed, there is nothing we can do here.
                     // Do not retry this commit on the same message.
                     logger.error("exception while committing message batch: ", ex);
                 } else {
                     logger.info("message batch committed successfully");
                 }
             });
  }
  ```

{% endlist %}

### Чтение с хранением позиции на клиентской стороне {#client-commit}

Вместо коммитов сообщений на сервер можно хранить прогресс чтения самостоятельно. В этом случае нужно передать в SDK обработчик, который будет вызываться при старте чтения каждой партиции. В этом обработчике нужно будет
указать позицию, с которой нужно начинать чтение этой партиции.

{% list tabs %}

- C++

  При обработке событий `TStartPartitionSessionEvent` можно при ответе серверу задать позицию, с которой следует начинать чтение.
  Для этого в метод `Confirm` следует передать параметр `readOffset`.
  Допольнительно можно передать параметр `commitOffset`, который укажет позицию, сообщения до которой следует считать [закоммиченными](#commit).

  Пример установки обработчика:
  ```cpp
  settings.EventHandlers_.StartPartitionSessionHandler(
      [](TReadSessionEvent::TStartPartitionSessionEvent& event) {
          auto readFromOffset = GetOffsetToReadFrom(event.GetPartitionId());
          event.Confirm(readFromOffset);
      }
  );
  ```
  Здесь `GetOffsetToReadFrom` - это часть примера, а не SDK. Используйте свой способ определить требуемую стартовую позицию чтения для партиции с данным partition id.

  Также в `TReadSessionSettings` поддерживается настройка `ReadFromTimestamp` для чтения событий с отметками времени записи не меньше данной. Эта настройка предполагается не для точного позиционирования старта, а для пропуска объёма данных за большой интервал времени. Несколько первых полученных сообщений могут иметь отметки времени записи меньше указанной.

- Go

  ```go
  func ReadWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage(ctx context.Context, db ydb.Connection) error {
      readContext, stopReader := context.WithCancel(context.Background())
      defer stopReader()

      readStartPosition := func(
          ctx context.Context,
          req topicoptions.GetPartitionStartOffsetRequest,
      ) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
          offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
          res.StartFrom(offset)

          // Reader will stop if return err != nil
          return res, err
      }

      r, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"),
          topicoptions.WithGetPartitionStartOffset(readStartPosition),
      )
      if err != nil {
          return err
      }

      go func() {
          <-readContext.Done()
          _ = r.Close(ctx)
      }()

      for {
          batch, err := r.ReadMessageBatch(readContext)
          if err != nil {
              return err
          }

          processBatch(batch)
          _ = externalSystemCommit(batch.Context(), batch.Topic(), batch.PartitionID(), batch.EndOffset())
      }
  }
  ```

- Python

  Функциональность находится в разработке.

- Java

  Чтение с заданного оффсета в Java возможно только в асинхронном читателе.
  В обработчике событий `StartPartitionSessionEvent` можно при ответе серверу задать позицию, с которой следует начинать чтение.
  Для этого в метод `confirm` следует передать настройки `StartPartitionSessionSettings` с указанным оффсетом через `setReadOffset`.
  Также вызовом `setCommitOffset` можно указать оффсет, который следует считать закоммиченным.

  ```java
  @Override
  public void onStartPartitionSession(StartPartitionSessionEvent event) {
      event.confirm(StartPartitionSessionSettings.newBuilder()
              .setReadOffset(lastReadOffset) // Long
              .setCommitOffset(lastCommitOffset) // Long
              .build());
  }
  ```

  Также поддерживается настройка читателя `setReadFrom` для чтения событий с отметками времени записи не меньше данной.

{% endlist %}

### Чтение без указания Consumer'а {#no-consumer}

Обычно прогресс чтения топика сохраняется на сервере в каждом `Consumer`е. Но можно не хранить такой прогресс на сервере и при создании читателя явно указать, что чтение будет происходить без `Consumer`а.

{% list tabs %}

- Java

  Для чтения без `Consumer`а следует в настройках читателя `ReaderSettings` это явно указать, вызвав `withoutConsumer()`:

  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .withoutConsumer()
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(TOPIC_NAME)
                  .build())
          .build();
  ```

  В таком случае нужно учитывать, что при переустановке соединения прогресс на сервере будет сброшен. Поэтому, чтобы не начинать чтение сначала, в SDK следует передавать offset начала чтения при каждом старте сессии чтения партиции:

  ```java
  @Override
  public void onStartPartitionSession(StartPartitionSessionEvent event) {
      event.confirm(StartPartitionSessionSettings.newBuilder()
              .setReadOffset(lastReadOffset) // the last offset read by this client, Long
              .build());
  }
  ```

{% endlist %}

### Чтение в транзакции {#read-tx}

{% list tabs %}

- C++

  Перед чтением из топика клиентский код должен передать в настройки получения событий из сессии ссылку на объект транзакции.

  ```cpp
      ReadSession->WaitEvent().Wait(TDuration::Seconds(1));

      auto tableSettings = NYdb::NTable::TTxSettings::SerializableRW();
      auto transactionResult = TableSession->BeginTransaction(tableSettings).GetValueSync();
      auto Transaction = transactionResult.GetTransaction();

      NYdb::NTopic::TReadSessionGetEventSettings topicSettings;
      topicSettings.Block(false);
      topicSettings.Tx(Transaction);

      auto events = ReadSession->GetEvents(topicSettings);

      for (auto& event : events) {
          // обработать событие и записать результаты в таблицу
      }

      NYdb::NTable::TCommitTxSettings commitSettings;
      auto commitResult = Transaction.Commit(commitSettings).GetValueSync();
  ```

  {% note warning %}

  При обработке событий `events` не нужно явно подтверждать обработку для событий типа `TDataReceivedEvent`.

  {% endnote %}

  Подтверждение обработки события `TStopPartitionSessionEvent` надо делать после вызова `Commit`.

  ```cpp
      std::optional<TStopPartitionSessionEvent> stopPartitionSession;

      auto events = ReadSession->GetEvents(topicSettings);

      for (auto& event : events) {
          if (auto* e = std::get_if<TStopPartitionSessionEvent>(&event) {
              stopPartitionSessionEvent = std::move(*e);
          } else {
              // обработать событие и записать результаты в таблицу
          }
      }

      NYdb::NTable::TCommitTxSettings commitSettings;
      auto commitResult = Transaction.Commit(commitSettings).GetValueSync();

      if (stopPartitionSessionEvent) {
          stopPartitionSessionEvent->Commit();
      }
  ```

- Java (sync)

  [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionReadSync.java)

  В настройках `ReceiveSettings` метода `receive` можно указать транзакцию:

  ```java
  Message message = reader.receive(ReceiveSettings.newBuilder()
          .setTransaction(transaction)
          .build());
  ```
  Тогда полученное сообщение будет закоммичено вместе с транзакцией. Коммитить его отдельно не нужно.
  Метод `receive` свяжет на сервере оффсеты сообщения с транзакцией вызовом `sendUpdateOffsetsInTransaction` и вернёт управление, когда получит ответ на него.

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

- Java (async)

  [Пример на GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionReadAsync.java)

  После получения сообщения в обработчике `onMessages` можно связать одно или несколько сообщений с транзакцией.
  Для этого нужно вызвать отдельный метод `reader.updateOffsetsInTransaction` и дождаться его выполнения на сервере.
  Этот метод принимает параметром список оффсетов. Для удобства у `Message` и `DataReceivedEvent` есть метод `getPartitionOffsets()`, возвращающий такой список.

  ```java
  @Override
  public void onMessages(DataReceivedEvent event) {
      for (Message message : event.getMessages()) {
          // creating a session in the table service
          Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
          if (!sessionResult.isSuccess()) {
              logger.error("Couldn't get a session from the pool: {}", sessionResult);
              return; // retry or shutdown
          }
          Session session = sessionResult.getValue();
          // creating a transaction in the table service
          // this transaction is not yet active and has no id
          TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

          // do something else in the transaction
          transaction.executeDataQuery("SELECT 1").join();
          // now the transaction is active and has an id
          // analyzeQueryResultIfNeeded();

          Status updateStatus = reader.updateOffsetsInTransaction(transaction,
                          message.getPartitionOffsets(), new UpdateOffsetsInTransactionSettings.Builder().build())
                  // Do not commit a transaction without waiting for updateOffsetsInTransaction result to avoid a race condition
                  .join();
          if (!updateStatus.isSuccess()) {
              logger.error("Couldn't update offsets in a transaction: {}", updateStatus);
              return; // retry or shutdown
          }

          Status commitStatus = transaction.commit().join();
          analyzeCommitStatus(commitStatus);
      }
  }
  ```

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

{% endlist %}

### Обработка серверного прерывания чтения {#stop}

В {{ ydb-short-name }} используется серверная балансировка партиций между клиентами. Это означает, что сервер может прерывать чтение сообщений из произвольных партиций.

При _мягком прерывании_ клиент получает уведомление, что сервер уже закончил отправку сообщений из партиции и больше сообщения читаться не будут. Клиент может завершить обработку сообщений и отправить подтверждение на сервер.

В случае _жесткого прерывания_ клиент получает уведомление, что работать с сообщениями партиции больше нельзя. Клиент должен прекратить обработку прочитанных сообщений. Неподтвержденные сообщения будут переданы другому читателю.

#### Мягкое прерывание чтения {#soft-stop}

{% list tabs %}

- C++

  Мягкое прерывание приходит в виде события `TStopPartitionSessionEvent` с методом `Confirm`. Клиент может завершить обработку сообщений и отправить подтверждение на сервер.

  Фрагмент цикла событий может выглядеть так:

  ```cpp
  auto event = ReadSession->GetEvent(/*block=*/true);
  if (auto* stopPartitionSessionEvent = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
      stopPartitionSessionEvent->Confirm();
  } else {
    // other event types
  }
  ```

- Go

  Клиентский код сразу получает все имеющиеся в буфере (на стороне SDK) сообщения, даже если их не достаточно для формирования пакета при групповой обработке.

  ```go
  r, _ := db.Topic().StartReader("my-consumer", nil,
      topicoptions.WithBatchReadMinCount(1000),
  )

  for {
      batch, _ := r.ReadMessageBatch(ctx) // <- if partition soft stop batch can be less, then 1000
      processBatch(batch)
      _ = r.Commit(batch.Context(), batch)
  }

  ```

- Python

  Специальной обработки не требуется.

  ```python
  while True:
    batch = reader.receive_batch()
    process(batch)
    reader.commit(batch)
  ```

- Java (sync)

  Неактуально, т.к. в синхронном читателе нет возможности настраивать обработку подобных событий.
  Клиент сразу ответит серверу подтверждением остановки.

- Java (async)

  Для возможности реагировать на такое событие следует переопределить метод `onStopPartitionSession(StopPartitionSessionEvent event)` в объекте-наследнике `ReadEventHandler` (см [Подключение к топику для чтения сообщений](#start-reader)).
  `event.confirm()` обязательно должен быть вызван, т.к. сервер ожидает этого ответа для продолжения остановки.

  ```java
  @Override
  public void onStopPartitionSession(StopPartitionSessionEvent event) {
      logger.info("Partition session {} stopped. Committed offset: {}", event.getPartitionSessionId(),
              event.getCommittedOffset());
      // This event means that no more messages will be received by server
      // Received messages still can be read from ReaderBuffer
      // Messages still can be committed, until confirm() method is called

      // Confirm that session can be closed
      event.confirm();
  }
  ```

{% endlist %}

#### Жесткое прерывание чтения {#hard-stop}

{% list tabs %}

- C++

  Жёсткое прерывание приходит в виде события `TPartitionSessionClosedEvent` либо в ответ на подтверждение мягкого прерывания, либо при потере соединения с партицией. Узнать причину можно, вызвав метод `GetReason`.

  Фрагмент цикла событий может выглядеть так:

  ```cpp
  auto event = ReadSession->GetEvent(/*block=*/true);
  if (auto* partitionSessionClosedEvent = std::get_if<TReadSessionEvent::TPartitionSessionClosedEvent>(&*event)) {
      if (partitionSessionClosedEvent->GetReason() == TPartitionSessionClosedEvent::EReason::ConnectionLost) {
          std::cout << "Connection with partition was lost" << std::endl;
      }
  } else {
    // other event types
  }
  ```

- Go

  При прерывании чтения контекст сообщения или пакета сообщений будет отменен.

  ```go
  ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
  if len(batch.Messages) == 0 {
      return
  }

  buf := &bytes.Buffer{}
  for _, mess := range batch.Messages {
      buf.Reset()
      _, _ = buf.ReadFrom(mess)
      _, _ = io.Copy(buf, mess)
      writeMessagesToDB(ctx, buf.Bytes())
  }
  ```

- Python

  В этом примере обработка сообщений в батче остановится, если в процессе работы партиция будет отобрана. Такая оптимизация требует дополнительного кода на клиенте. В простых случаях, когда обработка отобранных партиций не является проблемой, ее можно не применять.

  ```python
  def process_batch(batch):
      for message in batch.messages:
          if not batch.alive:
              return False
          process(message)
      return True

  batch = reader.receive_batch()
  if process_batch(batch):
      reader.commit(batch)
  ```

- Java (sync)

  Неактуально, т.к. в синхронном читателе нет возможности настраивать обработку подобных событий.

- Java (async)

  ```java
  @Override
  public void onPartitionSessionClosed(PartitionSessionClosedEvent event) {
      logger.info("Partition session {} is closed.", event.getPartitionSession().getPartitionId());
  }
  ```

{% endlist %}
