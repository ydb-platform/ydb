# Amazon SQS API

{{ ydb-short-name }} поддерживает работу с [топиками](../../concepts/datamodel/topic.md) по протоколу [Amazon SQS](https://en.wikipedia.org/wiki/Amazon_Simple_Queue_Service).

С одним топиком работа может вестись одновременно по нескольким протоколам. Например, запись может осуществляться с использованием Topic API, а чтение — с использованием Amazon SQS API, и наоборот.

При создании топика командой `CreateQueue` Amazon SQS API топик создаётся с включённым [автопартиционированием](../../concepts/datamodel/topic.md#autopartitioning_modes): с одной партицией и автоматической возможностью увеличения до 10 активных партиций. Параметры автопартиционирования можно изменить через [YQL](../../yql/reference/syntax/alter-topic.md) или [YDB CLI](../ydb-cli/topic-alter.md).

## Чтение по Amazon SQS API

Для чтения по Amazon SQS API используется [разделяемый (общий) читатель](../../concepts/datamodel/topic.md#shared-consumer), который должен быть создан на топике до начала чтения по Amazon SQS-протоколу. Если топик создаётся командой `CreateQueue` Amazon SQS API, автоматически создаётся разделяемый (общий) читатель с именем `ydb-sqs-consumer`.

Сообщения, записанные через Topic API, могут быть сжаты алгоритмами [gzip](https://en.wikipedia.org/wiki/Gzip), [lzop](https://en.wikipedia.org/wiki/Lzop) или [zstd](https://en.wikipedia.org/wiki/Zstd). При чтении по Amazon SQS-протоколу сервер не разжимает их, а передаёт в виде base64: читатель должен выполнить base64-декодирование, а затем разжатие.

Сообщения, записанные через Topic API без сжатия, могут содержать бинарные данные. При чтении по Amazon SQS-протоколу сервер также кодирует их в base64 — читатель должен выполнить base64-декодирование.

Чтобы читатель мог определить алгоритм сжатия, вместе с каждым сообщением отдаётся атрибут `BodyEncoding`. Если атрибут `BodyEncoding` отсутствует, разжимать сообщение не требуется. Возможные значения атрибута: `gzip`, `lzop`, `zstd` или `base64`.

Пример сообщения, содержимое которого сжато:

```json
{
    "Messages": [
        {
            "MessageId": "D523647F-5E89-560B-B1D0-152201831603",
            "ReceiptHandle": "CAAQAA==",
            "MD5OfBody": "d620a162c499920254054f78eac4feed",
            "Body": "KLUv/QBYaQAAeWRiIHdyaXRlZCAyCg==",
            "Attributes": {
                "SentTimestamp": "1780660726888",
                "BodyEncoding": "zstd"
            }
        }
    ]
}
```

## Запись по Amazon SQS API

При записи в топик по Amazon SQS-протоколу сообщения равномерно распределяются по партициям. При этом гарантируется, что все сообщения с одинаковым `MessageGroupId` попадут в одну партицию.

Для [очередей с обработкой в порядке First-In-First-Out (FIFO)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html) каждому сообщению нужен идентификатор дедупликации. Передайте его в параметре [`MessageDeduplicationId`](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html). Если для очереди включён параметр [`ContentBasedDeduplication`](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html), `MessageDeduplicationId` можно не передавать: {{ ydb-short-name }} вычислит идентификатор как SHA-256-хеш тела сообщения без учёта атрибутов. Если не передан явный идентификатор и выключена дедубликация по содержимому, запрос завершится ошибкой.

В течение пятиминутного окна {{ ydb-short-name }} принимает повторный запрос с тем же идентификатором, но не добавляет в очередь новое сообщение. После окончания окна идентификатор можно использовать для нового сообщения.

Есть ограничение на количество сообщений, которые можно записать в партицию FIFO-очереди, — 1000 сообщений в секунду. Если требуется записывать больше сообщений, следует увеличить количество партиций. Ограничение на очередь вычисляется как 1000 сообщений/сек/партиция × количество партиций. Например, чтобы записывать 10 тыс. сообщений в секунду, создайте FIFO-очередь с 10 партициями.

## Разделы документации

- [{#T}](auth.md)
- [{#T}](examples.md)
- [Создание очереди с помощью YQL](../../yql/reference/syntax/alter-topic.md#add-consumer)
- [{#T}](../../dev/shared-consumer-internals.md)
