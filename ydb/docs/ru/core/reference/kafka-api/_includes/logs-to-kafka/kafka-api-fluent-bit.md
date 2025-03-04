
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
