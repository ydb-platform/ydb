# Примеры чтения и записи по Kafka API

В этой статье приведены примеры чтения и записи в [топики](../../concepts/topic.md) с использованием Kafka API.

Перед выполнением примеров:

1. [Создайте топик](../ydb-cli/topic-create.md).
1. [Добавьте читателя](../ydb-cli/topic-consumer-add.md).
1. Если у вас включена аутентификация, [создайте пользователя](../../yql/reference/syntax/create-user.md).

## Начало работы {#how-to-try-kafka-api}

### В Docker {#how-to-try-kafka-api-in-docker}

Запустите Docker по [этой](../../quickstart#install) инструкции. Kafka API будет доступен на 9092 порте.

### В Yandex Cloud {#how-to-try-kafka-api-in-cloud}

Попробовать работу с топиками YDB по Kafka API можно без тарификации ([в небольших месячных объемах](https://yandex.cloud/ru/docs/data-streams/pricing?from=int-console-help-center-or-nav#prices)) в Yandex Cloud.
Для этого в своей [консоли Yandex Cloud](https://console.yandex.cloud):

1. Создайте [базу данных YDB](https://yandex.cloud/ru/docs/ydb/quickstart), если у вас ее еще нет.
1. Создайте [очередь Yandex Data Streams](https://yandex.cloud/ru/docs/data-streams/quickstart).
1. Создайте [сервисный аккаунт](https://yandex.cloud/ru/docs/iam/operations/sa/create), если у вас его еще нет.
1. Добавьте этому сервисному аккаунту следующие роли:

    - ydb.viewer (для чтения данных из потока);
    - ydb.editor (для записи данных в поток);
    - ydb.kafkaApi.client (для доступа к потоку данных по Kafka API).

1. Создайте [API ключ](https://yandex.cloud/ru/docs/iam/operations/sa/create-access-key) для этого сервисного аккаунта.

   - В поле **Описание** введите описание ключа.
   - В поле **Область действия** выберите `yc.ydb.topics.manage`.
   - В поле **Срок действия** укажите срок действия ключа.

Для работы с Yandex Cloud требуется аутентификация, см примеры аутентификации [ниже](#authentication-in-cloud-examples).

## Примеры работы с Kafka API

### Чтение

При чтении отличительной особенностью Kafka API являются:

- отсутствие поддержки опции [check.crcs](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs);
- только одна стратегия назначения партиция - roundrobin;
- отсутствие возможности читать без предварительно созданной группы читателей.

Поэтому в конфигурации читателя всегда нужно указывать **имя группы читателей** и параметры:

- `check.crc=false`
- `partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor`

Ниже даны примеры чтения по Kafka протоколу для разных приложений, языков программирования и фреймворков подключения без аутентификации.
Примеры того, как настроить аутентификацию, смотри в разделе [Примеры с аутентификацией](#authentication-examples)

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  ```bash
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic my-topic  \
    --group my-group \
    --from-beginning \
    --consumer-property check.crcs=false \
    --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor
  ```

- kcat

  ```bash
  kcat -C \
      -b <ydb-endpoint> \
      -X check.crcs=false \
      -X partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor \
      -G <consumer-name> <topic-name>
  ```

- Java

  ```java
  String HOST = "<ydb-endpoint>";
  String TOPIC = "<topic-name>";
  String CONSUMER = "<consumer-name>";

  Properties props = new Properties();

  props.put("bootstrap.servers", HOST);

  props.put("key.deserializer", StringDeserializer.class.getName());
  props.put("value.deserializer", StringDeserializer.class.getName());

  props.put("check.crcs", false);
  props.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());

  props.put("group.id", CONSUMER);
  Consumer<String, String> consumer = new KafkaConsumer<>(props);
  consumer.subscribe(Arrays.asList(new String[] {TOPIC}));

  while (true) {
      ConsumerRecords<String, String> records = consumer.poll(10000); // timeout 10 sec
      for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.key() + ":" + record.value());
      }
  }
  ```

- Spark

  {% include [index.md](_includes/spark-constraints.md) %}

  ```java
  public class ExampleReadApp {
      public static void main(String[] args) {
        var conf = new SparkConf().setAppName("my-app").setMaster("local");
        var context = new SparkContext(conf);

        context.setCheckpointDir("checkpoints");
        SparkSession spark = SparkSession.builder()
                .sparkContext(context)
                .config(conf)
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "flink-demo-input-topic")
                .option("kafka.group.id", "spark-example-app")
                .option("startingOffsets", "earliest")
                .option("kafka." + ConsumerConfig.CHECK_CRCS_CONFIG, "false")
                .load();

        df.foreach((ForeachFunction<Row>) row -> {
            System.out.println(row);
        });
      }
  }
  ```

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/flink-constraints.md) %}

  ```java
    public class YdbKafkaApiReadExample {
   
        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                    .enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
   
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///path/to/your/checkpoints");
            env.configure(config);
   
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setProperty(ConsumerConfig.CHECK_CRCS_CONFIG, "false")
                    .setGroupId("flink-demo-consumer")
                    .setTopics("my-topic")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setBounded(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();
   
            env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source").print();
   
            env.execute("YDB Kafka API example read app");
        }
    }
  ```

  {% include [index.md](_includes/flink-version-notice.md) %}

{% endlist %}

#### Частые проблемы и их решение

##### Ошибка Unexpected error in join group response

Полный текст ошибки:

```txt
Unexpected error in join group response: This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
```

Скорее всего проблема в том, что не указано имя читателя или указанное имя читателя не существует в кластере YDB.

Решение: создайте читателя с помощью [CLI](../ydb-cli/topic-consumer-add) или [SDK](../ydb-sdk/topic#alter-topic)

### Запись

{% note info %}

Сейчас не поддержана запись по Kafka API с использованием Kafka транзакций. Транзакции доступны только при использовании
[YDB Topic API](https://ydb.tech/docs/ru/reference/ydb-sdk/topic#write-tx).

В остальном запись в Apache Kafka и в YDB Topics через Kafka API ничем не отличается.

{% endnote %}

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  ```bash
  kafka-console-producer --broker-list localhost:9092 --topic my-topic
  ```

- kcat

  ```bash
  echo "test message" | kcat -P \
    -b <ydb-endpoint> \
    -t <topic-name> \
    -k key
  ```

- Java

  ```java
  String HOST = "<ydb-endpoint>";
  String TOPIC = "<topic-name>";

  Properties props = new Properties();
  props.put("bootstrap.servers", HOST);
  props.put("acks", "all");

  props.put("key.serializer", StringSerializer.class.getName());
  props.put("key.deserializer", StringDeserializer.class.getName());
  props.put("value.serializer", StringSerializer.class.getName());
  props.put("value.deserializer", StringDeserializer.class.getName());

  props.put("compression.type", "none");

  Producer<String, String> producer = new KafkaProducer<>(props);
  producer.send(new ProducerRecord<String, String>(TOPIC, "msg-key", "msg-body"));
  producer.flush();
  producer.close();
  ```

- Spark

  {% include [index.md](_includes/spark-constraints.md) %}

  ```java
  public class ExampleWriteApp {
    public static void main(String[] args) {
        var conf = new SparkConf().setAppName("my-app").setMaster("local");
        var context = new SparkContext(conf);
        context.setCheckpointDir("path/to/dir/with/checkpoints");
        SparkSession spark = SparkSession.builder()
            .sparkContext(context)
              .config(conf)
              .appName("Simple Application")
              .getOrCreate();
 
        spark
              .createDataset(List.of("spark-1", "spark-2", "spark-3", "spark-4"), Encoders.STRING())
              .write()
              .format("kafka")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("topic", "flink-demo-output-topic")
              .option("kafka.group.id", "spark-example-app")
              .option("startingOffsets", "earliest")
              .save();
      }
  }
  ```

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/flink-constraints.md) %}

  ```java
  public class YdbKafkaApiProduceExample {
      private static final String TOPIC = "my-topic";
 
      public static void main(String[] args) throws Exception {
          final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 
          Sink<String> kafkaSink = KafkaSink.<String>builder()
                  .setBootstrapServers("localhost:9092") // assuming ydb is running locally with kafka proxy on 9092 port
                  .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                          .setTopic(TOPIC)
                          .setValueSerializationSchema(new SimpleStringSchema())
                          .setKeySerializationSchema(new SimpleStringSchema())
                          .build())
                  .setRecordSerializer((el, ctx, ts) -> new ProducerRecord<>(TOPIC, el.getBytes()))
                  .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                          .build();
 
          env.setParallelism(1)
                  .fromSequence(0, 10)
                  .map(i -> i + "")
                  .sinkTo(kafkaSink);
 
          // Execute program, beginning computation.
          env.execute("ydb_kafka_api_write_example");
      }
  }
  ```

  {% include [index.md](_includes/flink-version-notice.md) %}

- Logstash

  ```ruby
  output {
    kafka {
      codec => json
      topic_id => "<topic-name>"
      bootstrap_servers => "<ydb-endpoint>"
      compression_type => none
    }
  }
  ```

- Fluent Bit

  ```ini
  [OUTPUT]
    name                          kafka
    match                         *
    Brokers                       <ydb-endpoint>
    Topics                        <topic-name>
    rdkafka.client.id             Fluent-bit
    rdkafka.request.required.acks 1
    rdkafka.log_level             7
    rdkafka.sasl.mechanism        PLAIN
  ```

{% endlist %}

### Примеры с аутентификацией {#authentication-examples}

Подробнее про аутентификацию смотри в разделе [Аутентификация](./auth.md). Ниже есть примеры аутентификации в облачной базе
и в локальной базе.

{% note info %}

Сейчас единственным доступным механизмом аутентификации с Kafka API в YDB Topics является `SASL_PLAIN`.

{% endnote %}

#### Примеры с аутентификацией в Yandex Cloud {#authentication-in-cloud-examples}

Инструкцию, как попробовать работу с Kafka API поверх YDB Topics в Яндекс облаке, смотри [выше](#how-to-try-kafka-api-in-cloud).

Для аутентификации добавьте в параметры подключения Apache Kafka следующие значения:

- `security.protocol` со значением `SASL_SSL`;
- `sasl.mechanism` со значением `PLAIN`;
- `sasl.jaas.config` со значением `org.apache.kafka.common.security.plain.PlainLoginModule required username="@<path_to_database>" password="<API Key сервисного аккаунта>";`.

Ниже приведены примеры чтения из топика облака, в которых:

- <path_to_database> - это путь к базе данных со страницы топика в YDS Yandex Cloud;
  ![path_to_database_example](./_assets/path_to_db_in_yds_cloud_ui.png)
- <kafka_api_endpoint> - это Kafka API Endpoint со страницы описания YDS Yandex Cloud. Его нужно использовать в качестве `bootstrap.servers`;
  ![kafka_endpoint_example](./_assets/kafka_api_endpoint_in_cloud_ui.png)
- <api_key> - API Key сервисного аккаунта, у которого есть доступ к YDS.

{% note warning %}

Username не указывается. Указывается только `@`, а затем путь до вашей базы данных.

{% endnote %}

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  ```bash
  kafka-console-consumer --bootstrap-server <kafka_api_endpoint> \
    --topic <topic-name>  \
    --group <consumer-name> \
    --from-beginning \
    --consumer-property check.crcs=false \
    --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor \
    --consumer-property security.protocol=SASL_SSL \
    --consumer-property sasl.mechanism=PLAIN \
    --consumer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"@<path_to_database>\" password=\"<api_key>\";"
  ```

- kcat

  ```bash
  kcat -C \
      -b <kafka_api_endpoint> \
      -X security.protocol=SASL_SSL \
      -X sasl.mechanism=PLAIN \
      -X sasl.username="@<path_to_database>" \
      -X sasl.password="<api_key>" \
      -X check.crcs=false \
      -X partition.assignment.strategy=roundrobin \
      -G <consumer-name> <topic-name>
  ```

- Java

  ```java
  String TOPIC = "<topic-name>";
  String CONSUMER = "<consumer-name>";

  Properties props = new Properties();

  props.put("bootstrap.servers", <kafka_api_endpoint>);

  props.put("key.deserializer", StringDeserializer.class.getName());
  props.put("value.deserializer", StringDeserializer.class.getName());

  props.put("check.crcs", false);
  props.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());

  props.put("security.protocol", "SASL_SSL");
  props.put("sasl.mechanism", "PLAIN");
  props.put("sasl.jaas.config", "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"@<path_to_database>\" password=\"<api_key>\";");

  props.put("group.id", CONSUMER);
  Consumer<String, String> consumer = new KafkaConsumer<>(props);
  consumer.subscribe(Arrays.asList(new String[] {TOPIC}));

  while (true) {
      ConsumerRecords<String, String> records = consumer.poll(10000); // timeout 10 sec
      for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.key() + ":" + record.value());
      }
  }
  ```

{% endlist %}

#### Примеры аутентификации в самостоятельно развернутом YDB

Для того, чтобы проверить работу с аутентификацией в локальной базе:

1. Создайте пользователя. [Как это сделать в YQL](../../yql/reference/syntax/create-user.md). [Как выполнить YQL из CLI](../ydb-cli/yql.md).
2. Подключитесь к Kafka API, как в примерах ниже. Во всех примерах предполагается, что:

  - YDB запущен локально с переменной окружения YDB_KAFKA_PROXY_PORT=9092 - то есть Kafka API доступен по адресу localhost:9092. Например можно поднять YDB в докере, как указано [здесь](../../quickstart.md#install).
  - <username> - это имя пользователя, которое вы указали при создании пользователя.
  - <password> - это пароль пользователя, который вы указали при создании пользователя.

Примеры показаны для чтения, но те же самые параметры конфигурации работают и для записи в топик.

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  ```bash
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic <topic-name>  \
    --group <consumer-name> \
    --from-beginning \
    --consumer-property check.crcs=false \
    --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor \
    --consumer-property security.protocol=SASL_PLAINTEXT \
    --consumer-property sasl.mechanism=PLAIN \
    --consumer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<username>\" password=\"<password>\";"
  ```

- kcat

  ```bash
  kcat -C \
      -b localhost:9092 \
      -X security.protocol=SASL_PLAINTEXT \
      -X sasl.mechanism=PLAIN \
      -X sasl.username="<username>" \
      -X sasl.password="<password>" \
      -X check.crcs=false \
      -X partition.assignment.strategy=roundrobin \
      -G <consumer-name> <topic-name>
  ```

- Java

  ```java
  String TOPIC = "<topic-name>";
  String CONSUMER = "<consumer-name>";

  Properties props = new Properties();

  props.put("bootstrap.servers", "localhost:9092");

  props.put("key.deserializer", StringDeserializer.class.getName());
  props.put("value.deserializer", StringDeserializer.class.getName());

  props.put("check.crcs", false);
  props.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());
 
  props.put("security.protocol", "SASL_PLAINTEXT");
  props.put("sasl.mechanism", "PLAIN");
  props.put("sasl.jaas.config", "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<username>\" password=\"<password>\";");

  props.put("group.id", CONSUMER);
  Consumer<String, String> consumer = new KafkaConsumer<>(props);
  consumer.subscribe(Arrays.asList(new String[] {TOPIC}));

  while (true) {
      ConsumerRecords<String, String> records = consumer.poll(10000); // timeout 10 sec
      for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.key() + ":" + record.value());
      }
  }
  ```

{% endlist %}