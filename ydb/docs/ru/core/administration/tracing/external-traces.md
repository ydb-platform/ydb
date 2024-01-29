# Передача внешнего trace-id в {{ ydb-short-name }}

{{ ydb-short-name }} поддерживает передачу внешних trace-id для построения цельной трассы операции.
Передача trace-id производится согласно [спецификации trace context](https://w3c.github.io/trace-context/#traceparent-header) ---
через заголовок `traceparent` gRPC запроса должна передаваться строка вида `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`.
Только версия (version) 00 поддерживается, флаги (trace-flags) игнорируются. Если заголовок не соответствует спецификации, он игнорируется.

## Ограничение на количество трассируемых запросов

В большинстве случаев пропускная способность коллектора спанов не позволяет трассировать все запросы. Для избежания перегрузки коллектора спанов в {{ ydb-short-name }} предусмотрено два механизма:

1. Ограничение количества спанов, отправляемых с одной ноды кластера

    На момент написания документации ограничение составляет 10 шт./сек.

1. Ограничение количества трассируемых запросов на входе в систему, в gRPC

    Для каждого _типа_ запросов (напр. `KeyValue.ExecuteTransaction`) действует ограничение на максимальное количество трассируемых извне запросов. Для отдельной ноды эти ограничения регулируются через Immediate Control Board. Соответствующие контроллеры называются:
    - `MaxRatePerMinute` --- `TracingControls.<RequestType>.ExternalThrottling.MaxRatePerMinute` (напр. `TracingControls.KeyValue.ExecuteTransaction.ExternalThrottling.MaxBurst`)
    - `MaxBurst` --- `TracingControls.<RequestType>.ExternalThrottling.MaxBurst`
    
    Используется вариация [leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket) с размером бакета равным `MaxBurst + 1`. Например, если `MaxRatePerMinute = 60` и `MaxBurst = 0`, то при потоке в 1000 запросов в минуту будет трассироваться один запрос каждую секунду. Если же `MaxBurst = 20`, то при поступлении аналогичного потока запросов первые 21 будут оттрассированы, далее аналогично будет трассироваться один запрос в секунду.

    Для массового обновления настроек на кластере следует использовать [{#T}](../../maintenance/manual/dynamic-config.md#dynamic-kinds), соответствующие настройки называются `immediate_controls_config.tracing_controls.<request_type>.max_rate_per_minute` и `immediate_controls_config.tracing_controls.<request_type>.max_burst` (напр. `immediate_controls_config.tracing_controls.key_value.execute_transaction.max_rate_per_minute` и `immediate_controls_config.tracing_controls.key_value.execute_transaction.max_burst`)

    {% note warning %}
    Для каждого типа запросов соответствующий контроллер в Immediate Control Board ноды появляется только после того, как к ноде был сделан запрос этого типа через gRPC.
    {% endnote %}
