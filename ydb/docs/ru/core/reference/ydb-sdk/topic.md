<!-- Этот файл не отслеживается автоматической системой перевода. Правки в EN-версию необходимо внести самостоятельно. -->
# Работа с топиками

В этой статье приведены примеры использования {{ ydb-short-name }} SDK для работы с [топиками](../../concepts/topic.md).

Перед выполнением примеров [создайте топик](../ydb-cli/topic-create.md) и [добавьте читателя](../ydb-cli/topic-consumer-add.md).

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


{% endlist %}

## Управление топиками {#manage}

### Создание топика {#create-topic}

{% list tabs %}

- C++

  Единственный обязательный параметр для создания топика - это его путь. Остальные настройки опциональны и представлены структурой `TCreateTopicSettings`.

  Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L394).

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

  Единственный обязательный параметр для создания топика - это его путь, остальные параметры опциональны.

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

{% endlist %}

### Изменение топика {#alter-topic}

{% list tabs %}

- C++

  При изменении топика в параметрах метода `AlterTopic` нужно указать путь топика и параметры, которые будут изменяться. Изменяемые параметры представлены структурой `TAlterTopicSettings`.

  Полный список настроек смотри [в заголовочном файле](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

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

  Функциональность находится в разработке.

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
    log.Fatalf("failed drop topic: %v", err)
    return
  }
  fmt.Printf("describe: %#v\n", descResult)
  ```

- Python

  ```python
  info = driver.topic_client.describe_topic(topic_path)
  print(info)
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

{% endlist %}

### Асинхронная запись сообщений {#async-write}

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

{% endlist %}

## Чтение сообщений {#reading}

### Подключение к топику для чтения сообщений {#start-reader}

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

{% endlist %}

Вы также можете использовать расширенный вариант создания подключения, чтобы указать несколько топиков и задать параметры чтения. Следующий код создаст подключение к топикам `my-topic` и `my-specific-topic` через читателя `my-consumer`, а также задаст время, с которого начинать читать сообщения:

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

- Python

  Функциональность находится в разработке.

{% endlist %}

### Чтение сообщений {#reading-messages}

Сервер хранит [позицию чтения сообщений](../../concepts/topic.md#consumer-offset). После вычитывания очередного сообщения клиент может [отправить на сервер подтверждение обработки](#commit). Позиция чтения изменится, а при новом подключении будут вычитаны только неподтвержденные сообщения.

Читать сообщения можно и [без подтверждения обработки](#no-commit). В этом случае при новом подключении будут прочитаны все неподтвержденные сообщения, в том числе и уже обработанные.

Информацию о том, какие сообщения уже обработаны, можно [сохранять на клиентской стороне](#client-commit), передавая на сервер стартовую позицию чтения при создании подключения. При этом позиция чтения сообщений на сервере не изменяется.

{% list tabs %}

- C++

  Работа пользователя с объектом `IReadSession` в общем устроена как обработка цикла событий со следующими типами событий: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent` и `TSessionClosedEvent`.

  Для каждого из типов событий можно установить обработчик этого события, а также можно установить общий обработчик. Обработчики устанавливаются в настройках сессии записи перед её созданием.

  Если обработчик для некоторого события не установлен, его необходимо получить и обработать в методах `GetEvent` / `GetEvents`. Для неблокирующего ожидания очередного события есть метод `WaitEvent` с сигнатурой `TFuture<void>()`.

- Go

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

{% endlist %}

### Чтение с подтверждением обработки сообщений {#commit}

Подтверждение обработки сообщения (коммит) - сообщает серверу, что сообщение из топика обработано получателем и больше его отправлять не нужно. При использовании чтения с подтверждением нужно подтверждать все полученные сообщения без пропуска. Коммит сообщений на сервере происходит после подтверждения очередного интервала сообщений «без дырок», сами подтверждения при этом можно отправлять в любом порядке.

Например с сервера пришли сообщения 1, 2, 3. Программа обрабатывает их параллельно и отправляет подтверждения в таком порядке: 1, 3, 2. В этом случае сначала будет закоммичено сообщение 1, а сообщения 2 и 3 будут закоммичены только после того как сервер получит подтверждение об обработке сообщения 2.

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

{% endlist %}

### Чтение с хранением позиции на клиентской стороне {#client-commit}

При начале чтения клиентский код должен сообщить серверу стартовую позицию чтения:

{% list tabs %}

- C++

  Чтение с заданной позиции в текущей версии SDK отсутствует.

  Поддерживается настройка `ReadFromTimestamp` для чтения событий с отметками времени записи не меньше данной.

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

{% endlist %}
