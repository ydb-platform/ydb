# Работа с топиками

В этой статье приведены примеры использования {{ ydb-short-name }} SDK для работы с [топиками](../../concepts/topic.md).

Перед выполнением примеров [создайте топик](../ydb-cli/topic-create.md) и [добавьте читателя](../ydb-cli/topic-consumer-add.md).

## Запись сообщений
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

{% endlist %}


### Асинхронная запись сообщений

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

{% endlist %}

### Выбор кодека для сжатия сообщений

{% list tabs %}

- Go

  По умолчанию SDK выбирает кодек автоматически (с учётом настроек топика). В автоматическом режиме SDK сначала отправляет по одной группе сообщений каждым из разрешённых кодеков, затем иногда будет пробовать сжать сообщения всеми доступными кодеками и выбирать кодек, дающий наименьший размер сообщения. Если для топика список разрешённых кодеков пуст, то автовыбор производится между Raw и Gzip-кодеками.
  При необходимости можно задать фиксированный кодек в опциях подключения. Тогда будет использоваться именно он и замеры проводиться не будут.

  ```go
  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithCodec(topictypes.CodecGzip),
  )
  ```

{% endlist %}



## Чтение сообщений
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

{% endlist %}

#### Чтение с подтверждением обработки сообщений {#commit}

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

{% endlist %}
