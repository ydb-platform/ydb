# kafka_proxy_config

In the `kafka_proxy_config` section of the {{ ydb-short-name }} configuration file, Kafka Proxy is enabled and configured, which provides access to work with [{{ ydb-short-name }} Topics](../../concepts/datamodel/topic.md) via [Kafka API](../../reference/kafka-api/index.md).

## Description of parameters

#|
|| Parameter | Type | Default value | Description ||
|| `enable_kafka_proxy` | bool | `false` | Enables or disables Kafka Proxy. ||
|| `listening_port` | int32 | `9092` | Port on which the Kafka API will be available. ||
|| `transaction_timeout_ms` | uint32 | `300000` (5 minutes) | Maximum timeout for Kafka transactions, after which the transaction will be canceled. ||
|| `auto_create_topics_enable` | bool | `false` | Enables automatic creation of topics when accessed. Analogous to the [same option](https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable) in Apache Kafka. ||
|| `auto_create_consumers_enable` | bool | `true` | Enables automatic creation of consumers when accessed. ||
|| `topic_creation_default_partitions` | uint32 | `1` | Number of partitions that will be created if the number of partitions was not specified when adding a topic via the Kafka protocol. Analogous to the [num.partitions](https://kafka.apache.org/documentation/#brokerconfigs_num.partitions) option in Apache Kafka. ||
|| `ssl_cerificate` | string | - | Path to the certificate file for SSL access, including both the certificate file and the key file. When this parameter is specified, Kafka Proxy automatically starts processing requests using the specified SSL certificate. ||
|| `cert` | string | - | Path to the certificate file for SSL access. When this parameter is specified, Kafka Proxy automatically starts processing requests using the specified SSL certificate. ||
|| `key` | string | - | Path to the key file for SSL access. ||
|| `ca` | string | - | Path to the certificate authority file for mTLS. ||
|| `enable_self_signed_certs` | bool | `false` | Allows the use of self-signed certificates for mTLS. ||
|| `mtls_enable` | bool | `false` | Enables the use of mTLS authentication. ||
|#

## Example of a filled config


```yaml
kafka_proxy_config:
  enable_kafka_proxy: true
  listening_port: 9092
  transaction_timeout_ms: 300000 # 5 минут
  auto_create_topics_enable: true
  auto_create_consumers_enable: true
  topic_creation_default_partitions: 1
  cert: /path/to/cert.pem
  key: /path/to/key.pem
```
