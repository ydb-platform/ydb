|Feature|C\+\+|Python|Go|Java|NodeJS|C#|Rust|PHP|
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
|{% if lang == 'en' %} SSL/TLS support (system certificates) {% else %} {% if lang == 'ru' %} Поддержка SSL/TLS (системные сертификаты) {% else %} Unsupported language {% endif %} {% endif %} |\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} SSL/TLS support (custom certificates) {% else %} {% if lang == 'ru' %} Поддержка SSL/TLS (кастомные сертификаты) {% else %} Unsupported language {% endif %}
{% endif %}|\+|\+|\+|\+|\+|\-|\-|\+|
|{% if lang == 'en' %} Configure/enable GRPC KeepAlive (keeping the connection alive in the background) {% else %} {% if lang == 'ru' %} Возможность настроить/включить GRPC KeepAlive (фоновое поддержание живости соединения) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|?||\-|\-|\-|
|{% if lang == 'en' %} Regular SLO testing on the latest code version {% else %} {% if lang == 'ru' %} Регулярный прогон тестов SLO на последней версии кода {% else %} Unsupported language {% endif %} {% endif %}|new|new|new|new|new|new|\-|\-|
|{% if lang == 'en' %} Issue templates on GitHub {% else %} {% if lang == 'ru' %} Шаблоны Issue в GitHub {% else %} Unsupported language {% endif %} {% endif %}|\-|?|\+|\-|\+|\+|\-|\+|
|{% if lang == 'en' %} **Client-side balancing** {% else %} {% if lang == 'ru' %} **Клиентская балансировка** {% else %} **Unsupported language** {% endif %} {% endif %}|||||||||
|{% if lang == 'en' %} Load balancer initialization through Discovery/ListEndpoints {% else %} {% if lang == 'ru' %} Инициализация балансировщика через Discovery/ListEndpoints {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} Disable client-side load balancing (all requests to the initial Endpoint) {% else %} {% if lang == 'ru' %} Отключение клиентской балансировки (все запросы в начальный Endpoint) {% else %} Unsupported language {% endif %} {% endif %}|\+/-|\-|\+|\-|\-|\+|\+|\+|
|{% if lang == 'en' %} Background Discovery/ListEndpoints (by default, once a minute) {% else %} {% if lang == 'ru' %} Фоновый Discovery/ListEndpoints (раз в минуту по умолчанию) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|\-|
|{% if lang == 'en' %} Support for multiple IP addresses in the initial Endpoint DNS record, some of which may not be available (DNS load balancing) {% else %} {% if lang == 'ru' %} Поддержка множества IP адресов в DNS-записи начального Endpoint, часть из которых может быть недоступна (DNS балансировка) {% else %} Unsupported language {% endif %} {% endif %}|?|\+|\+|?|\-|?|?|?|
|{% if lang == 'en' %} Node pessimization on transport errors {% else %} {% if lang == 'ru' %} Пессимизация нод на транспортных ошибках {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+||
|{% if lang == 'en' %} Forced Discovery/ListEndpoints if more than half of the nodes are pessimized {% else %} {% if lang == 'ru' %} Принудительный Discovery/ListEndpoints если пессимизировано более половины нод {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\-|\+|\+||
|{% if lang == 'en' %} Automatic detection of the nearest DC/availability zone by TCP pings {% else %} {% if lang == 'ru' %} Автоматическое определение ближайшего ДЦ / зоны доступности по TCP-пингам {% else %} Unsupported language {% endif %} {% endif %}|\-|\-|\+|\-|\-|\-|\-||
|{% if lang == 'en' %} Automatic detection of the nearest DC/availability zone by Discovery/ListEndpoints response\* {% else %} {% if lang == 'ru' %} Aвтоматическое определение ближайшего ДЦ / зоны доступности по ответу Discovery/ListEndpoints\* {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\-|\-|\-|\-|\-||
|{% if lang == 'en' %} Uniform random selection of nodes (default) {% else %} {% if lang == 'ru' %} Равномерный случайный выбор нод (по умолчанию) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+||
|{% if lang == 'en' %} Load balancing across all nodes of all DCs (default) {% else %} {% if lang == 'ru' %} Балансировка среди всех нод всех ДЦ (по умолчанию) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+||
|{% if lang == 'en' %} Load balancing across all nodes of a particular DC/availability zone (for example, “a”, “vla”) {% else %} {% if lang == 'ru' %} Балансировка среди всех нод конкретного ДЦ / зоны доступности (например, “a”, “vla”) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|?|\-|\-|\-||
|{% if lang == 'en' %} Load balancing across all nodes of all local DCs {% else %} {% if lang == 'ru' %} Балансировка среди всех нод всех локального ДЦ {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|?|\-|\-|\-||
|**Credentials providers**|||||||||
|{% if lang == 'en' %} Anonymous (default) {% else %} {% if lang == 'ru' %} Anonymous (по умолчанию) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|\+|
|Static (user - password)|\+|\+|\+|\+|\-|\-|\-|\+|
|Token: IAM, OAuth|\+|\+|\+|\+|\+|\+|\+|\+|
|Service account (Yandex.Cloud specific)|\+|\+|\+|\+|\+|\+|\-|\+|
|Metadata (Yandex.Cloud specific)|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} **Working with Table service sessions** {% else %} {% if lang == 'ru' %} **Работа с сессиями Table-сервиса** {% else %} Unsupported language {% endif %} {% endif %}|||||||||
|{% if lang == 'en' %} Session pool {% else %} {% if lang == 'ru' %} Пул сессий {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} Limit the number of concurrent sessions on the client {% else %} {% if lang == 'ru' %} Ограничение количества одновременных сессий на клиенте {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} Minimum number of sessions in the pool {% else %} {% if lang == 'ru' %} Несгораемый остаток сессий в пуле {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\-|\-|\-|
|{% if lang == 'en' %} Warm up the pool to the specified number of sessions when the pool is created {% else %} {% if lang == 'ru' %} Прогрев пула до указанного значения количества сессий при создании пула {% else %} Unsupported language {% endif %} {% endif %}|\-|\+|\-|\-|\+|\-|\-|
|{% if lang == 'en' %} Background KeepAlive for idle sessions in the pool {% else %} {% if lang == 'ru' %} Фоновый KeepAlive для простаивающих сессий в пуле {% else %} Unsupported language {% endif %} {% endif %}|\+|\-|\-|\+|\+|\+|\+|
|{% if lang == 'en' %} Background closing of idle sessions in the pool (redundant sessions) {% else %} {% if lang == 'ru' %} Фоновое закрытие простаивающих сессий в пуле (лишние сессии) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\-|\-|\-|
|{% if lang == 'en' %} Automatic dumping of a session from the pool in case of BAD_SESSION/SESSION_BUSY errors {% else %} {% if lang == 'ru' %} Автоматическое выбрасывание сессии из пула при получении ошибок BAD_SESSION / SESSION_BUSY {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} Storage of sessions for possible future reuse\~ {% else %} {% if lang == 'ru' %} Отстойник сессий для возможного переиспользования в будущем\~ {% else %} Unsupported language {% endif %} {% endif %}|\+|\-|\-|\-|\-|\-|\-|
|{% if lang == 'en' %} Retryer on the session pool (a repeat object is a session) {% else %} {% if lang == 'ru' %} Ретраер на пуле сессий (объект для повторов - сессия) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} Retryer on the session pool (a repeat object is a transaction within a session) {% else %} {% if lang == 'ru' %} Ретраер на пуле сессий (объект для повторов - транзакция на сессии) {% else %} Unsupported language {% endif %} {% endif %}|\-|\-|\+|\-|\-|\-|\+|\+|
|{% if lang == 'en' %} Graceful session shutdown support ("session-close" in "x-ydb-server-hints" metadata means to "forget" a session and not use it again) {% else %} {% if lang == 'ru' %} Поддержка graceful shutdown сессий ("session-close" в metadata "x-ydb-server-hints" - означает надо "забыть" сессию и больше ее не использовать) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+||\+|\-|\-|
|{% if lang == 'en' %} Support for server-side load balancing of sessions (a CreateSession request must contain the "session-balancer" value in the "x-ydb-client-capabilities" metadata header) {% else %} {% if lang == 'ru' %} Поддержка серверной балансировки сессий (запрос CreateSession должен содержать в metadata-заголовке "x-ydb-client-capabilities" значение "session-balancer") {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+||\-|\-|\-|
|{% if lang == 'en' %} **Support for YDB data types** {% else %} {% if lang == 'ru' %} **Поддержка типов данных YDB** {% else %} **Unsupported language** {% endif %} {% endif %}|||||||||
|Int/Uint(8,16,32,64)|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} Int128, UInt128 (not available publicly?) {% else %} {% if lang == 'ru' %} Int128, UInt128 (в паблике нету?) {% else %} Unsupported language {% endif %} {% endif %}|\-|\-|\-|\-|\-|\-|\-|\-|
|Float,Double|\+|\+|\+|\+|\+|\+|\+|\+|
|Bool|\+|\+|\+|\+|\+|\+|\+|\+|
|String, Bytes|\+|\+|\+|\+|\+|\+|\+|\+/\-|
|Utf8, Text|\+|\+|\+|\+|\+|\+|\+|\+|
|NULL,Optional,Void|\+|\+|\+|\+|\+|\+/-|\+|\+/\-|
|Struct|\+|\+|\+|\+|\+|\+|\+|+|
|List|\+|\+|\+|\+|\+|\+/-|\+|+|
|Set|?|?|\-|?|?|\-|?|?|
|Tuple|\+|\+|\+|\+|\+|\+|\+|?+|
|Variant\<Struct\>,Variant\<Tuple\>|\+|\+|\+|\+|\+|\+|\-|\-|
|Date,DateTime,Timestamp,Interval|\+|\+|\+|\+|\+|\+|\+|\-|
|TzDate,TzDateTime,TzTimestamp|\+|\+|\+|\+|\+|\-|\-|\-|
|DyNumber|\+|\+|\+|\+|\+|\-|\-|\-|
|{% if lang == 'en' %} Decimal (120 bits) {% else %} {% if lang == 'ru' %} Decimal (120 бит) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\-|\+|
|Json,JsonDocument,Yson|\+|\+|\+|\+|\+|\+|\+|\+/\-|
|{% if lang == 'en' %} **Scheme client** {% else %} {% if lang == 'ru' %} **Scheme клиент** {% else %} **Unsupported language** {% endif %} {% endif %}|||||||||
|MakeDirectory|\+|\+|\+|\+|\+|\-|\+|\+|
|RemoveDirectory|\+|\+|\+|\+|\+|\-|\+|\+|
|ListDirectory|\+|\+|\+|\+|\+|\+|\+|\+|
|ModifyPermissions|\+|\+|\+|\-|\+|\-|\-|\+|
|DescribePath|\+|\+|\+|\+|\+|\-|\-|\+|
|{% if lang == 'en' %} **Table service** {% else %} {% if lang == 'ru' %} **Table-сервис** {% else %} **Unsupported language** {% endif %} {% endif %}|||||||||
|CreateSession|\+|\+|\+|\+|\+|\+|\+|\+|
|DeleteSession|\+|\+|\+|\+|\+|\+|\+|\+|
|KeepAlive|\+|\+|\+|\+|\+|\+|\+|\-|
|CreateTable|\+|\+|\+|\+|\+|\-|\-|\+|
|DropTable|\+|\+|\+|\+|\+|\-|\-|\+|
|AlterTable|\+|\+|\+|\+|\+|\-|\-|\+|
|CopyTable|\+|\+|\+|\+|\-|\-|\-|\+|
|CopyTables|\+|\+|new|\-|\-|\-|\-|\+|
|DescribeTable|\+|\+|\+|\+|\+|\-|\-|\+|
|QueryService|\-|\-|\-|soon|soon|new|\-|\-|
|ExplainDataQuery|\+|\+|\+|\+|\-|\-|\-|\+|
|PrepareDataQuery|\+|\+|\+|\+|\+|\-|\-|\+|
|ExecuteDataQuery|\+|\+|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} \* By default, server cache for all parameter requests (KeepInCache) {% else %} {% if lang == 'ru' %} \* Серверный кэш по умолчанию для всех запросов с параметрами (KeepInCache) {% else %} Unsupported language {% endif %} {% endif %}|\-|\+|\+|\+|\+|\+|\-|\+|
|{% if lang == 'en' %} \* A separate option to enable/disable server cache for a specific request {% else %} {% if lang == 'ru' %} \* Отдельная опция для включения/выключения серверного кэша для конкретного запроса {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\+|\+|\+|\-|\+|
|{% if lang == 'en' %} \* Truncated result as an error (by default) {% else %} {% if lang == 'ru' %} \* Truncated result как ошибка (по дефолту) {% else %} Unsupported language {% endif %} {% endif %}|\-|\-|\+|?|\+|\-|\-|\+|
|{% if lang == 'en' %} \* Truncated result as an error (as an opt-in, opt-out option) {% else %} {% if lang == 'ru' %} \* Truncated result как ошибка (как опция opt-in, opt-out) {% else %} Unsupported language {% endif %} {% endif %}|\-|\-|\+|?|\+|\-|\+|\-|
|ExecuteSchemeQuery|\+|\+|\+|\+|\-|\+|\+|\+|
|BeginTransaction|\+|\+|\+|\+|\+|\-|\-|\+|
|CommitTransaction|\+|\+|\+|\+|\+|\-|\+|\+|
|RollbackTransaction|\+|\+|\+|\+|\+|\-|\+|\+|
|DescribeTableOptions|\+|\+|\+|\-|\-|\-|\-|\-|
|StreamExecuteScanQuery|\+|\+|\+|\+|\+|\+|\+|\+|
|StreamReadTable|\+|\+|\+|\+|\+|\+|\-|\+|
|BulkUpsert|\+|\+|\+|\+|\+|\-|\-|\+|
|ReadRows|new|\-|new|new|\-|\-|\-|\-|
|**Operation**|||||||||
|{% if lang == 'en' %} Consumed Units from metadata of a response to a grpc-request request (for the user to obtain this) {% else %} {% if lang == 'ru' %} Consumed Units из метаданных ответа на grpc-запрос (чтобы пользователь мог получить это) {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\-|\+|\+|\-|\-|\-|
|{% if lang == 'en' %} Obtaining OperationId of the operation for a long-polling status of operation execution {% else %} {% if lang == 'ru' %} Получение OperationId операции для long-polling статуса выполнения операции {% else %} Unsupported language {% endif %} {% endif %}|\+|\+|\+|\-|\-|\+|\-|\-|
|**ScriptingYQL**|||||||||
|ExecuteYql|\+|?|\+|\-|\-|\-|\-|\+|
|ExplainYql|\+|?|\+|\-|\-|\-|\-|\+|
|StreamExecuteYql|\+|?|\+|\-|\-|\-|\-|\-|
|**Coordination service**|||||||||
|CreateNode|\+|?|\+|\-|\-|\-|\-|\-|
|AlterNode|\+|?|\+|\-|\-|\-|\-|\-|
|DropNode|\+|?|\+|\-|\-|\-|\-|\-|
|DescribeNode|\+|?|\+|\-|\-|\-|\-|\-|
|{% if lang == 'en' %} Session (leader election, distributed lock) {% else %} {% if lang == 'ru' %} Session (leader election, распределенный лок) {% else %} Unsupported language {% endif %} {% endif %}|\+|?|\-|soon|\-|\-|\-|\-|
|**Topic service**|||||||||
|CreateTopic|\+|\+|\+|new|\-|\-|\-|\-|
|DescribeTopic|\+|\+|\+|new|\-|\-|\-|\-|
|AlterTopic|\+|\-|\+|new|\-|\-|\-|\-|
|DropTopic|\+|\+|\+|new|\-|\-|\-|\-|
|StreamWrite|\+|\+|\+|new|\-|\-|\-|\-|
|StreamRead|\+|\+|\+|new|\-|\-|\-|\-|
|{% if lang == 'en' %} Federated Topic SDK {% else %} {% if lang == 'ru' %} Федеративный SDK для Topic {% else %} Unsupported language {% endif %} {% endif %}|\-|\-|soon|soon|\-|\-|\-|\-|
|**Ratelimiter service**|||||||||
|CreateResource|\+|?|\+|\-|\-|\-|\-|\-|
|AlterResource|\+|?|\+|\-|\-|\-|\-|\-|
|DropResource|\+|?|\+|\-|\-|\-|\-|\-|
|ListResources|\+|?|\+|\-|\-|\-|\-|\-|
|DescribeResource|\+|?|\+|\-|\-|\-|\-|\-|
|AcquireResource|\+|?|\+|\-|\-|\-|\-|\-|
|{% if lang == 'en' %} **Monitoring** (sending SDK metrics to the monitoring system) {% else %} {% if lang == 'ru' %} **Monitoring** (отправка метрик SDK в систему мониторинга) {% else %} Unsupported language {% endif %} {% endif %}|||||||||
|Solomon / Monitoring|\+|?|\+|\-|\-|\-|\-|\-|
|Prometheus|\-|?|\+|\-|\-|\-|\-|\-|
|{% if lang == 'en' %} SDK event **logging** {% else %} {% if lang == 'ru' %} **Логирование** событий SDK {% else %} Unsupported language {% endif %} {% endif %}|\-|?|\+|\+|\+|\+|\+|\+|
|{% if lang == 'en' %} SDK event **tracing** {% else %} {% if lang == 'ru' %} **Трассировка** событий SDK {% else %} Unsupported language {% endif %} {% endif %}|||||||||
|{% if lang == 'en' %} in OpenTelemetry {% else %} {% if lang == 'ru' %} в OpenTelemetry {% else %} Unsupported language {% endif %} {% endif %}|\-|?|soon|\-|\-|\-|\-|\-|
|{% if lang == 'en' %} in OpenTracing {% else %} {% if lang == 'ru' %} в OpenTracing {% else %} Unsupported language {% endif %} {% endif %}|\-|?|\+|\-|\-|\-|\-|\-|
|**Examples**|||||||||
|Auth|||||||||
|\* token|?|?|\+|\+|\+|\+|\+|\+|
|\* anonymous|?|?|\+|\+|\+|\+|\+|\+|
|\* environ|?|?|\+|\+|\+|\-|\-|\+|
|\* metadata|?|?|\+|\+|\+|\+|\+|\+|
|\* service_account|?|?|\+|\+|\+|\-|\-|\+|
|\* static (username \+ password)|?|?|\+|\+|\+|\+|\-|\+|
|Basic (series)|\+|?|\+|\+|\+|\+|\+|\+|
|Bulk Upsert|\+/-|?|\+|\+|\+|\-|\-|\+|
|Containers (Struct,Variant,List,Tuple)|\-|?|\+|\-|\-|\-|\+|\-|
|Pagination|\+|?|\+|\+|\-|\-|\-|\-|
|Partition policies|\-|?|\+|\-|\-|\-|\-|\-|
|Read table|?|?|\+|\-|\+|\-|\-|\+|
|Secondary index Workaround|\+|?|\-|\+|\-|\-|\-|\-|
|Secondary index builtin|\+|?|\-|\-|\-|\-|\-|\-|
|TTL|\+|?|\+|\-|\-|\-|\-|\-|
|TTL Readtable|\+|?|\+|\-|\-|\-|\-|\-|
|URL Shortener (serverless yandex function)|?|?|\+|?|\+|\-|\-|\-|
|Topic reader|\+||\+||\-|\-|\-|\-|
|Topic writer|\-||\+||\-|\-|\-|\-|
