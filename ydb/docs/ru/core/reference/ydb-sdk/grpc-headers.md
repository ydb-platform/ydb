## Заголовки метаданных gRPC

{{ ydb-short-name }} использует следующие gRPC metadata заголовки:

* gRPC заголовки, отправляемые клиентом в {{ ydb-short-name }} :
  * `x-ydb-database` - база данных
  * `x-ydb-auth-ticket` - токен авторизации, полученный от провайдера авторизации
  * `x-ydb-sdk-build-info` - информация о {{ ydb-short-name }} SDK
  * `x-ydb-trace-id` - идентификатор запроса, устанавливаемый пользователем. Если не задано пользователем - {{ ydb-short-name }} SDK генерирует автоматически в формате [UUID](https://ru.wikipedia.org/wiki/UUID)
  * `x-ydb-application-name` - опциональное имя приложения, устанавливаемое пользователем
  * `x-ydb-client-capabilities` - поддерживаемые клиентским SDK возможности (`session-balancer` и другие)
  * `x-ydb-client-pid` - идентификатор процесса клиентского приложения
  * `traceparent` - заголовок для передачи родительского идентификатора трассы OpenTelemetry ([спецификация](https://w3c.github.io/trace-context/#header-name))

* gRPC заголовки, отправляемые клиенту {{ ydb-short-name }} вместе с ответом на текущий запрос:
  * `x-ydb-server-hints` - уведомления {{ ydb-short-name }} (`session-close` и другие)
  * `x-ydb-consumed-units` - потребленные ресурсов YDB на текущем запросе
