# Передача внешнего trace-id в {{ ydb-short-name }}

{{ ydb-short-name }} поддерживает передачу внешних trace-id для построения цельной трассы операции.
Передача trace-id производится согласно [спецификации trace context](https://w3c.github.io/trace-context/#traceparent-header) —
через заголовок `traceparent` gRPC запроса должна передаваться строка вида `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`.
Только версия (version) 00 поддерживается, флаги (trace-flags) игнорируются. Если заголовок не соответствует спецификации, он игнорируется.

## Ограничение на количество трассируемых запросов

{% note warning %}

В {{ ydb-short-name }} действует глобальное ограничение в 10 спанов в секунду, отправляемых с одного узла кластера. При его превышении спаны могут удаляться, что может приводить к неполным трассам.

{% endnote %}

Для контроля количества трассируемых извне запросов, в {{ ydb-short-name }} предусмотрен rate limiting аналогичный механизму в [сэмплировании запросов](./sampling.md). Для включения rate limiting нужно добавить секцию `external_throttling` в `tracing_config`:

```yaml
tracing_config:
  # ...
  external_throttling:
    - max_rate_per_minute: 60
      max_burst: 3
    - scope:
        request_type: KeyValue.ReadRange
      max_rate_per_minute: 10
```

Семантика всех настроек, а так же допустимые значения аналогичны соответствующим настройкам в [сэмплировании](./sampling.md#semantics).
Если секция `external_throttling` пустая, либо отсутствует, все trace-id, приходящие извне, будут игнорироваться.
