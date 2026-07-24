# kafka_proxy_config

The `kafka_proxy_config` section of the {{ ydb-short-name }} configuration file enables and configures Kafka Proxy, which provides access to work with [{{ ydb-short-name }} Topics](../../concepts/datamodel/topic.md) via [Kafka API](../../reference/kafka-api/index.md).

## Description of parameters

| Parameter | Type | Default value | Description |
| --- | --- | --- | --- |
| `enable_kafka_proxy` | bool | `false` | Enables or disables Kafka Proxy. |
| `listening_port` | int32 | `9092` | The port on which the Kafka API will be available. |
| `listening_address` | string | `[::]` | The network address on which Kafka Proxy listens for incoming connections. Use `[::]` to listen on all interfaces (dual-stack, requires IPv6 to be enabled), `127.0.0.1` or `[::1]` to restrict access to localhost. |
| `transaction_timeout_ms` | uint32 | `300000` (5 minutes) | The maximum timeout for Kafka transactions, after which the transaction will be cancelled. |
| `auto_create_topics_enable` | bool | `false` | Enables automatic creation of topics when they are accessed. Analogous to [the same option](https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable) in Apache Kafka. |
| `auto_create_consumers_enable` | bool | `true` | Enables automatic registration of consumers when they are accessed. |
| `topic_creation_default_partitions` | uint32 | `1` | The number of partitions that will be created if the number of partitions is not specified when adding a topic via the Kafka protocol. Analogous to [num.partitions](https://kafka.apache.org/documentation/#brokerconfigs_num.partitions) option in Apache Kafka. |
| `ssl_cerificate` | string | — | The path to the SSL certificate file, which includes both the certificate file and the key file. When this parameter is specified, Kafka Proxy automatically starts processing requests using the specified SSL certificate. |
| `cert` | string | — | The path to the SSL certificate file. When this parameter is specified, Kafka Proxy automatically starts processing requests using the specified SSL certificate. |
| `key` | string | — | The path to the SSL key file. |

## Example of a completed config

```yaml
kafka_proxy_config:
  enable_kafka_proxy: true
  listening_port: 9092
  listening_address: "[::]"
  transaction_timeout_ms: 300000 # 5 minutes
  auto_create_topics_enable: true
  auto_create_consumers_enable: true
  topic_creation_default_partitions: 1
  cert: /path/to/cert.pem
  key: /path/to/key.pem
```
