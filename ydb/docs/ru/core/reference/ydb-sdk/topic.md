<!-- Этот файл не отслеживается автоматической системой перевода. Правки в EN-версию необходимо внести самостоятельно. -->
# Работа с топиками

В этой статье приведены примеры использования {{ ydb-short-name }} SDK для работы с [топиками](../../concepts/topic.md).

Перед выполнением примеров [создайте топик](../ydb-cli/topic-create.md) и [добавьте читателя](../ydb-cli/topic-consumer-add.md).

## Управление топиками {#manage}

### Создание топика {#create-topic}

{% list tabs %}

Единственный обязательный параметр для создания топика - это его путь, остальные параметры - опциональны.

- Go

  Полный список поддерживаемых параметров можно посмотреть в [документации SDK](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

  Пример создания топика со списком поддерживаемых кодеков и минимальным количество партиций

  ```go
  err := db.Topic().Create(ctx, "topic-path",
      // optional
    topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

    // optional
    topicoptions.CreateWithMinActivePartitions(3),
  )
  ```

- Python

  ```python
  driver.topic_client.create_topic(topic_path,
      supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
      min_active_partitions=3,                                    # optional
  )
  ```

{% endlist %}

### Изменение топика {#alter-topic}

При изменении топика в параметрах нужно указать путь топика и те параметры, которые будут изменяться.

{% list tabs %}

- Go

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

На данный момент поддерживается подключение только с совпадающими producer_id и message_group_id, в будущем это ограничение будет снято.

{% list tabs %}

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

По умолчанию SDK выбирает кодек автоматически (с учетом настроек топика). В автоматическом режиме SDK сначала отправляет по одной группе сообщений каждым из разрешенных кодеков, затем иногда будет пробовать сжать сообщения всеми доступными кодеками и выбирать кодек, дающий наименьший размер сообщения. Если для топика список разрешенных кодеков пуст, то автовыбор производится между Raw и Gzip-кодеками.

При необходимости можно задать фиксированный кодек в опциях подключения. Тогда будет использоваться именно он и замеры проводиться не будут.

{% list tabs %}

- Go

  ```go
  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithCodec(topictypes.CodecGzip),
  )
  ```

- Python

  ```python
  writer = driver.topic_client.writer(topic_path,
      codec=ydb.TopicCodec.GZIP,
  )
  ```

{% endlist %}

## Чтение сообщений {#reading}

### Подключение к топику для чтения сообщений {#start-reader}

Чтобы создать подключение к существующему топику `my-topic` через добавленного ранее читателя `my-consumer`, используйте следующий код:

{% list tabs %}

- Go

  ```go
  reader, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"))
  if err != nil {
      return err
  }
  ```

- Python

  ```python
  reader = driver.topic_client.reader(topic="topic-path", consumer="consumer_name")
  ```

{% endlist %}

Вы также можете использовать расширенный вариант создания подключения, чтобы указать несколько топиков и задать параметры чтения. Следующий код создаст подключение к топикам `my-topic` и `my-specific-topic` через читателя `my-consumer`, а также задаст время, с которого начинать читать сообщения:

{% list tabs %}

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

SDK получает данные с сервера партиями и буферизирует их. В зависимости от задач клиентский код может читать сообщения из буфера по одному или пакетами.

### Чтение без подтверждения обработки сообщений {#no-commit}

Чтобы читать сообщения по одному, используйте следующий код:

{% list tabs %}

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

Чтобы прочитать пакет сообщений, используйте следующий код:

{% list tabs %}

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

Чтобы подтверждать обработку сообщений по одному, используйте следующий код:

{% list tabs %}

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

Для подтверждения обработки пакета сообщений используйте следующий код:

{% list tabs %}

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

#### Чтение с хранением позиции на клиентской стороне {#client-commit}

При начале чтения клиентский код должен сообщить серверу стартовую позицию чтения:

{% list tabs %}

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
