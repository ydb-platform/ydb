# RFC: classic NBS compatibility gateway для NBS2

## 1. Метаданные

| Поле                                             | Значение                                   |
|--------------------------------------------------|--------------------------------------------|
| Статус                                           | Черновик                                   |
| Авторы                                           | TBD                                        |
| Рецензенты                                       | TBD                                        |
| Создан                                           | 2026-08-11                                 |
| Обновлён                                         | 2026-08-12                                 |
| Целевая ревизия YDB                              | `9cecd841b167fa84354e3f99c1a418c9e5b3f1a1` |
| Кандидат на source revision classic NBS contract | `5e95a6646c3cc8af5797ba307f99ec9431aaecbe` |

Связанные документы:

- [обзор и текущие решения](README.md);
- [исследование и архитектурное обоснование](architecture.md);
- [MVP-1/MVP-2: gRPC и RDMA benchmark](mvp_benchmark.md);
- [MVP-3: функциональная интеграция](mvp_functional.md);
- [production и выкатка](production.md).

Этот RFC фиксирует предлагаемую границу системы и обязательные свойства решения. Детальные шаги реализации, оценки и тестовые матрицы остаются в связанных планах.

## 2. Решаемая проблема

Необходимо интегрировать новый дисковый backend NBS2 с существующей инфраструктурой NBS1. NBS2 должен отвечать за хранение данных и выполнение дисковых операций, а существующее управление подключением дисков и создание NBD/vhost endpoint должно остаться в NBS1.

## 3. Обзор решения

Основным механизмом интеграции являются NBS shards (cells). NBS2 подключается к NBS1 как отдельная cell и предоставляет совместимые интерфейсы classic NBS для обнаружения и монтирования дисков, а также выполнения I/O через gRPC и RDMA. Специальные знания о внутреннем устройстве NBS2 в логику NBS1 cells не добавляются.

Для предоставления этих интерфейсов в `ydbd` встраивается compatibility gateway. Он разрешает `DiskId` через SSProxy/SchemeShard, обрабатывает запросы classic NBS и направляет операции чтения и записи через общий для gRPC и RDMA backend во внутренний data path NBS2. Маршрутизация к partition выполняется через стабильный tablet pipe по `PartitionTabletId`.

`StartEndpoint` остаётся операцией NBS1: стандартный механизм NBS1 выбирает cell и gateway host, создаёт NBD/vhost endpoint и направляет его I/O в NBS2 через штатный cells transport. Gateway обрабатывает запросы обнаружения диска, управления сессией и доступа к данным, но не создаёт пользовательские endpoint.

В первом варианте gateway использует существующий gRPC server `ydbd` и отдельный настраиваемый RDMA port. Необходимость отдельного gRPC listener остаётся открытым вопросом.

## 4. Технические ограничения текущего решения

Legacy NBS cells умеет находить volume в peer cells и привязывать endpoint к выбранному classic NBS gRPC/RDMA backend. NBS2 сейчас не является таким backend:

- cells ожидает classic NBS protobuf service и error semantics, с которыми текущий `Ydb.Nbs` несовместим;
- существующий draft API `Ydb.Nbs` не предоставляет совместимый с NBS1 cells сетевой контракт, управление сессиями и стабильную маршрутизацию запросов по логическому `DiskId`;
- локальный vhost socket NBS2 привязан к конкретному node и не является сетевым cells backend;
- cells выбирает host для data path независимо от host, ответившего на discovery, поэтому node-local routing недостаточен;
- classic NBS session state и writer fencing не защищают автоматически foreign NBS2 volume:
  - session state — сведения об активных подключениях: клиент, режим доступа, `SessionId` и mount sequence;
  - writer generation определяет актуального writer, а writer fencing отклоняет I/O устаревших sessions;
  - classic NBS хранит эти данные и выполняет fencing в своём volume tablet только для собственных volumes. Для NBS2 соответствующие данные должны храниться и проверяться в `VolumeDirect` после реализации production-модели.

Итого, чтобы NBS1 cells мог работать с диском NBS2 как с обычным удалённым NBS-диском: найти его по DiskId, смонтировать и выполнять I/O через gRPC или RDMA, — NBS2 должен предоставлять совместимый интерфейс classic NBS

## 5. Что хотим сделать

1. Оставить `StartEndpoint` и создание guest-facing NBD/vhost endpoint в NBS1.
2. Выполнять discovery NBS2 volume по логическому `DiskId`.
3. Поддержать read/write из NBS1 cells через classic NBS gRPC и RDMA.
4. Не добавлять в NBS1 cells специальных знаний о NBS2.
5. Направлять запросы по стабильному `PartitionTabletId`; рестарт или миграция partition должны обрабатываться механизмом tablet routing.
6. Обеспечить возможность любому gateway host обслужить любой disk своей cell.
7. Использовать один transport-independent backend и одинаковую валидацию и обработку ошибок для gRPC и RDMA.
8. До готовности к production обеспечить единое отказоустойчивое состояние сессий и защиту от конфликтующих writers (`authoritative session state` и `writer fencing`).

## 6. Что не хотим

- переносить в NBS2 клиентской discovery/routing-логики NBS cells; NBS2 реализует только совместимый backend peer cell для существующего NBS1 cells;
- реализовывать `StartEndpoint` или создание guest-facing socket в gateway;
- использовать local NBS2 vhost socket как cells backend;
- создавать временный урощенный benchmark-only RDMA protocol;
- делать placement-aware `GetCellEndpoint` в рамках этого решения. Его базовая реализация возможна (но не обязательна) на этапах MVP для получения нагрузочных результатов.
  - в рамках этого RFC `GetCellEndpoint` остаётся без изменений: он случайно выбирает один из активных hosts cell без учёта размещения partition tablet;
  - gateway направляет запрос к partition через tablet pipe; переход может оказаться локальным или межузловым;
  - placement-aware выбор host остаётся потенциальной оптимизацией;
- поддерживать multi-partition volumes;
- поддерживать автоматическое переключение уже работающего endpoint между gateway hosts;
- реализовывать полную create/delete/resize lifecycle-интеграция NBS2;
- создавать окончательное описание всех состояний session и переходов между ними, а также writer fencing в этом RFC.

## 7. Предлагаемая архитектура

```text
guest
    -> NBD/vhost endpoint в NBS1
    -> NBS1 cells
         control: classic NBS gRPC
         data: classic NBS gRPC или RDMA
    -> compatibility gateway внутри ydbd
         classic gRPC facade ─┐
         classic RDMA target ─┴─> общий gateway backend
                                  -> session/range validation
                                  -> per-volume I/O actor
                                  -> stable tablet pipe

    -> MVP-1/MVP-2/MVP-3: NBS2 partition/FastPathService

    -> production: VolumeDirect session/generation check
                   -> NBS2 partition/FastPathService
    -> DDisk
```

Gateway состоит из следующих логических компонентов:

- **classic NBS gRPC facade** — принимает control-plane и gRPC data-plane RPC, декодирует classic NBS contract и не содержит backend-логики;
- **classic NBS RDMA target** — реализует существующий NBS1 RDMA wire protocol и передаёт I/O в тот же backend;
- **gateway core** — общая для gRPC и RDMA логика обработки запросов: проверяет параметры и сессию, преобразует ошибки NBS2 в ошибки classic NBS и передаёт операцию в backend NBS2;
- **volume resolver** — разрешает `DiskId` через локальный SSProxy/SchemeShard в volume metadata и внутренние tablet IDs;
- **session component** — в MVP хранит состояние mount-сессий в памяти процесса gateway, а в production использует единое отказоустойчивое состояние NBS2;
- **per-volume I/O actors** — направляют запросы через tablet pipe: в MVP непосредственно в partition, а в production через `VolumeDirect`; ограничивают число незавершённых запросов и восстанавливают pipe после разрыва или миграции;
- **partition/FastPathService** — существующий внутренний NBS2 data path до DDisk.

SSProxy/SchemeShard хранит основное описание диска: по `DiskId` из него можно получить `VolumeConfig`, `VolumeTabletId` и `PartitionTabletId`. Gateway использует эти данные для поиска диска. В MVP активные mount-сессии хранятся в памяти процесса gateway; в production состояние сессий, writer generation и право записи должны принадлежать `VolumeDirect`.

DDisk map содержит только привязку partition tablet к DDisk/node и не содержит `DiskId` и полного описания volume, поэтому не может заменить resolver. Таблицы и API DDisk map уже реализованы, но production producer, который гарантированно поддерживает карту заполненной и актуальной, пока отсутствует.

## 8. Сетевой и программный контракт

Gateway реализует classic protobuf service с полным именем `NCloud.NBlockStore.NProto.TBlockStoreService`. Source revision protobuf, коды ошибок, IDs RDMA-сообщений и бинарный формат RDMA-сообщений (`wire layout`) фиксируются до начала переноса; кандидат указан в метаданных RFC.

Минимальный service surface:

| RPC | Назначение |
|---|---|
| `Ping` | compatibility/liveness check |
| `DescribeVolume` | cells discovery по `DiskId` |
| `MountVolume` | создание либо восстановление session |
| `UnmountVolume` | завершение session |
| `ReadBlocks` | чтение через общий backend |
| `WriteBlocks` | запись через общий backend |
| `ZeroBlocks` | discard/write-zeroes после готовности внутреннего NBS2 path |

До сквозной реализации `ZeroBlocks` возвращает `E_NOT_IMPLEMENTED`, а соответствующая capability не рекламируется guest-у.

Для MVP classic NBS gRPC service предлагается зарегистрировать на существующем YDB gRPC server внутри `ydbd`; запрос маршрутизируется по полному имени gRPC service. Classic responses не заворачиваются в YDB Operations.
RDMA target запускается в том же процессе на отдельном конфигурируемом `RdmaPort`; control plane при RDMA transport остаётся на gRPC.

Проверка `Headers.CellId == GatewayConfig.CellId` применяется к `DescribeVolume`, где header участвует в cells discovery. Mount и data RPC уже направлены в выбранную cell и не должны требовать этот header.

gRPC и RDMA адаптеры должны передавать запросы в gateway core через единый интерфейс, скрывающий особенности сообщений и сетевых буферов конкретного транспорта:

- gRPC facade и RDMA target проверяют формат сообщения, transport-specific ограничения и обеспечивают время жизни сетевых буферов;
- gateway core одинаково для обоих transports проверяет `DiskId`, session, диапазон блоков, payload и общие resource limits;
- в production `VolumeDirect` дополнительно проверяет authoritative session и writer generation, обеспечивая writer fencing.

## 9. Control-plane flow

```text
пользователь/control plane
    -> StartEndpoint(disk1) в NBS1 cell-a
        -> cells discovery
            -> DescribeVolume(disk1, CellId=NBS2) в gateway hosts
            -> resolver -> SSProxy/SchemeShard
        -> выбор NBS2 cell и gateway host
        -> MountVolume(disk1) в gateway
        -> создание NBD/vhost endpoint на NBS1 host
```

Gateway не принимает `StartEndpoint` и не создаёт NBD/vhost socket. Его control-plane boundary начинается с `DescribeVolume` и `MountVolume`. Существующий `StartEndpoint` NBS1 выполняет discovery среди local service и peer cells, выбирает cell и создаёт endpoint, привязанный к найденному backend.

Если resolver не находит disk, gateway возвращает `E_NOT_FOUND`. Временная недоступность resolver должна быть retriable и не должна маскироваться под отсутствие disk.

## 10. Data-plane flow

```text
NBS1 endpoint
    -> gRPC ReadBlocks/WriteBlocks
       или RDMA ReadBlocksLocal/WriteBlocksLocal
    -> gRPC facade или RDMA target внутри gateway
         -> разбор запроса
         -> проверка формата и transport-specific ограничений
         -> удержание сетевых буферов до завершения I/O
    -> gateway core
         -> проверка DiskId и session
         -> потенциально проверки диапазона блоков, размера и формата payload
         -> применение общих resource/in-flight limits
    -> MVP-1/MVP-2/MVP-3:
         per-volume I/O actor
         -> stable tablet pipe
         -> partition tablet
    -> production:
         VolumeDirect
         -> проверка authoritative session и writer generation
         -> writer fencing
         -> partition tablet
    -> FastPathService
    -> DDisk
```

Gateway ограничивает количество и суммарный объём in-flight запросов. Данные запроса и связанные с ними сетевые буферы должны оставаться доступными до полного завершения операции чтения или записи в NBS2. Range проверяется на overflow, alignment и выход за границы volume; write payload должен точно соответствовать запрошенному диапазону.

При tablet pipe disconnect запрос с неопределённым результатом завершается через `E_REJECTED`. Gateway восстанавливает pipe для новых запросов, но не повторяет потенциально выполненный write. Решение о retry принимает durable client origin NBS согласно его семантике.

## 11. Ключевые инварианты

1. Единственный внешний идентификатор NBS2 volume — логический `DiskId`.
2. `PartitionTabletId` и actor IDs остаются внутренними деталями NBS2.
3. Любой gateway host может обслужить любой disk своей cell через tablet routing, даже если partition tablet находится на другом node. Размещение может влиять на производительность, но не на корректность.
4. Каждый I/O проверяет существование и актуальность session.
5. В production устаревший writer не может изменить данные ни через один gateway host.
6. Gateway не повторяет write с неопределённым результатом.
7. Неподдерживаемая capability не рекламируется guest-у.
8. gRPC и RDMA используют один backend, validation и error model.

## 12. Управление сессиями и защита от устаревшего writer (writer fencing)

MVP-1/MVP-2 могут использовать явно непроизводственный синтетический `SessionId` и in-memory state для одного benchmark workload.

В MVP-3 временное состояние для одного benchmark workload заменяется process-local реестром, поддерживающим несколько дисков и клиентов, идемпотентный mount и полный жизненный цикл сессии. Потерянная после рестарта session отклоняется через `E_BS_INVALID_SESSION`, чтобы NBS1 выполнил remount.

Process-local состояние не обеспечивает writer fencing: после рестарта gateway или при работе нескольких gateway hosts могут быть независимо разрешены конфликтующие записи. Поэтому на production-этапе session state и writer generation должны принадлежать единому отказоустойчивому компоненту NBS2 — `VolumeDirect`. Рассматривался отдельный volume/session tablet, но в принятой архитектуре он не используется.

Authoritative-модель должна учитывать как минимум `ClientId`, `InstanceId`, режим доступа (`read-only`/`read-write`), `MountSeqNumber`, writer generation, unmount и автоматическое завершение неактивных сессий. `VolumeDirect` проверяет session и writer generation на каждом I/O, обеспечивая writer fencing. Вариант с generation/token, проверяемым непосредственно partition, не выбран. Проверки generation только в gateway недостаточно.

Local auto-vhost NBS2 постоянно выключен в cells deployment, иначе появляется второй путь к данным вне общей модели session ownership и writer fencing. Реализация auto-vhost остаётся в коде и может использоваться в конфигурациях NBS2 без cells.

## 13. Error model

| Ситуация | Classic NBS error |
|---|---|
| Disk отсутствует или path имеет неверный scheme type | `E_NOT_FOUND` |
| Resolver временно недоступен | `E_REJECTED` |
| Неверный `CellId` в `DescribeVolume` | `E_REJECTED` |
| Неверный range, alignment или payload | `E_ARGUMENT` |
| Неизвестная, завершённая или stale session | `E_BS_INVALID_SESSION` |
| Неопределённый in-flight I/O | `E_REJECTED` |
| Отключённый zero/discard | `E_NOT_IMPLEMENTED` |

Существующий volume с неподдерживаемой gateway геометрией должен успешно проходить discovery, после чего `MountVolume` возвращает детерминированный `E_NOT_IMPLEMENTED` или `E_ARGUMENT`. Это не позволяет cells ошибочно интерпретировать unsupported capability как отсутствие disk.

Внутренние NBS2 ошибки переводятся только в error codes зафиксированной classic NBS revision. Пользовательский сетевой запрос не должен приводить к `Y_ABORT` или другой process-wide invariant failure.

## 14. Совместимость

- classic NBS protobuf package, полный service name, номера полей, коды ошибок, id RDMA сообщений и wire layout копируются из одной зафиксированной source revision;
- изменение source revision требует protobuf compatibility tests, RDMA wire tests и повторного NBS1 cells E2E;
- NBS1 cells и его discovery/backend selection не изменяются;
- gRPC и RDMA adapters проверяются одним набором backend contract tests;
- gateway выключен по умолчанию; выключенный gateway не регистрирует classic service/RDMA target и не меняет обычные пути `ydbd`;
- namespace и protobuf symbol конфликты между YDB, NBS2 и classic NBS должны выявляться build/compatibility тестами.

## 15. Безопасность и эксплуатация (черновой вариант)

Для внешнего classic NBS интерфейса gateway должен по возможности использовать существующие настройки и эксплуатационные правила NBS1, а не вводить параллельную модель конфигурации.

Из NBS1 переиспользуются или сохраняются совместимыми:

- идентификатор cell и настройки подключений: `CellId`, gRPC/RDMA ports, transport, таймауты и количество соединений;
- защищённый gRPC: `SecureGrpcPort`, доверенный сертификат центра сертификации (`CA`, `RootCertsFile`), клиентские и серверные сертификаты, а также существующий способ передачи authentication metadata;
- ограничения обработки запросов: максимальный размер сообщения, request timeout, квота памяти, число workers и объём незавершённых запросов;
- classic NBS error semantics и параметры throttling/QoS;
- принятые в NBS1 форматы request logs, метрик и диагностической информации;
- для RDMA используется модель сетевой защиты целевого NBS1 deployment; TLS для RDMA этим RFC не предлагается.

Gateway сохраняет совместимость с параметрами throttling/QoS NBS1, но не должен создавать второй независимый контур ограничения IOPS или bandwidth. До production необходимо выбрать один слой NBS2, отвечающий за применение каждого такого ограничения.

Дополнительно для gateway внутри `ydbd` требуется:

- отдельный флаг включения; по умолчанию classic NBS service и RDMA target не запускаются;
- ограниченные очереди, память и число одновременно выполняющихся запросов;
- изоляция ресурсов: перегрузка gateway не должна блокировать другие сервисы `ydbd`;
- метрики и логи для discovery, mount/session, gRPC/RDMA запросов, очередей, ошибок и переподключений tablet pipe;
- возможность поэтапной выкатки, остановки и отката без потери контроля над активными сессиями.

Для MVP предполагается общий YDB gRPC server.

## 16. Альтернативы

- **Gateway внутри `ydbd`.** **Предлагается**: прямой доступ к actor system, SSProxy и tablet pipes при минимальном новом runtime.

- **Compatibility sidecar.** Не выбран: добавляет IPC/routing и deployment surface, а принятая архитектура не требует отдельного процесса.

- **Расширить `Ydb.Nbs` и изменить cells.** Отклоняется: дублирует transport/backend логику и требует специальных знаний в NBS1 cells.

- **Прямой tablet pipe из NBS1.** Отклоняется: переносит YDB internals и unstable routing contract в legacy NBS process.

- **Local NBS2 vhost socket.** Отклоняется как cells backend: node-local, обходит gateway session model и не даёт нужный сетевой контракт.

Кодовые доказательства и расширенное сравнение находятся в [architecture.md](architecture.md).

## 17. План проверки решения

RFC проверяется последовательными milestones:

1. [MVP-1](mvp_benchmark.md) — доказать совместимость и измерить настоящий cells hot path через gRPC.
2. [MVP-2](mvp_benchmark.md) — провести тот же workload через штатный classic NBS RDMA transport и сравнить результаты.
3. [MVP-3](mvp_functional.md) — заменить временные ограничения и ручные настройки нагрузочного прототипа полноценным разрешением DiskId, управлением жизненным циклом сессий внутри gateway и восстановлением маршрутизации после перезапуска или миграции tablet.
4. [Production](production.md) — довести до "production ready" состояния
   - реализовать единое отказоустойчивое управление сессиями (authoritative session) и защиту от устаревших writers (writer fencing),
   - провести проверки отказоустойчивости и совместимости,
   - добавить мониторинг и средства диагностики,
   - обеспечить безопасность и подготовить поэтапную выкатку.

Решение считается архитектурно подтверждённым только после E2E обоих transports через неизменённый NBS1 cells. Performance acceptance thresholds и полная failure matrix задаются в планах соответствующих этапов, а не в RFC.

## 18. Технические риски / возможные проблемы

1. **Неприемлемый dependency graph classic NBS proto/RDMA stack.**

   Снижение риска: ранний dependency spike и первая попытка перенести существующий RDMA target целиком. Минимальный wire-compatible adapter остаётся резервным вариантом при неприемлемом dependency graph.

2. **Один actor/mailbox либо NBS1 vhost executor становится bottleneck.**

   Снижение риска: метрики очередей; при подтверждении — несколько I/O actors и/или vhost queues.

3. **Session split-brain между gateway hosts.**

   Снижение риска: authoritative state и проверка writer generation вне process-local gateway memory.

4. **Нарушение ordering overlapping writes.**

   Снижение риска: явно зафиксировать ordering contract и проверить конкурентными тестами.

5. **Дополнительные network/actor hops снижают IOPS.**

   Снижение риска: сравнить производительность прямого NBS2 I/O, gRPC и RDMA и определить вклад каждого участка data path с помощью профилирования.

6. **Нагрузка gateway может влиять на другие сервисы `ydbd`.**

   Риск принимается: `ydbd`, в котором включён NBS2 gateway, является dynamic node, предназначенным в том числе для обработки пользовательского I/O. Ограниченные очереди, память и число одновременно выполняющихся запросов остаются обязательными требованиями к gateway независимо от выбранного listener.

## 19. Зафиксированные решения

1. Authoritative владельцем session state и writer generation выбран `VolumeDirect`; он же выполняет writer fencing. Рассматривался отдельный volume/session tablet, но этот вариант не выбран.
2. Writer generation проверяется в `VolumeDirect` на каждом I/O. Вариант с generation/token, проверяемым непосредственно partition, не используется.
3. Существующий classic NBS RDMA target сначала пробуем перенести целиком. Минимальный wire-compatible adapter к NBS2 backend остаётся резервным вариантом, если полный перенос создаст неприемлемый dependency graph.
4. Local auto-vhost постоянно выключен в cells deployment, но остаётся в коде для конфигураций NBS2, которые не используют cells.
5. Sidecar не требуется: compatibility gateway работает внутри `ydbd`. Рассмотренный sidecar-вариант отклонён из-за дополнительного IPC/routing и отдельного deployment surface.

## 20. Оставшиеся открытые вопросы

1. Можно ли зарегистрировать classic NBS service на существующем YDB gRPC server или для него нужен отдельный gRPC listener внутри `ydbd`?
   - существующий endpoint должен быть доступен из сети NBS1 cells, а открытие YDB-порта для этого трафика должно быть допустимо;
   - TLS- и authentication-настройки должны быть совместимы с требованиями NBS1 cells;
   - параметры gRPC-соединений, задаваемые на уровне server/listener, должны подходить для classic NBS traffic;
   - отдельный listener нужен, если classic NBS traffic требуется независимо открывать или закрывать на сетевом уровне.
2. Должен ли gateway упорядочивать одновременно выполняющиеся записи в пересекающиеся диапазоны блоков? Выбранное поведение должно быть одинаковым для gRPC и RDMA и соответствовать гарантиям NBS2 partition.

## 21. Чеклист ревьювера

Нужно подтвердить следующие пункты:

1. classic NBS compatibility gateway встраивается в `ydbd` и выключен по умолчанию;
2. NBS1 cells и `StartEndpoint` не изменяются;
3. первым resolver является существующий SSProxy/SchemeShard;
4. data path использует стабильный tablet pipe по `PartitionTabletId` и передаёт запросы в существующий `FastPathService` NBS2;
5. поддерживаются classic NBS gRPC и wire-compatible cells RDMA transport с общим backend; существующий RDMA target сначала переносится целиком;
6. любой gateway host обязан обслуживать любой disk cell без обязательной placement affinity;
7. authoritative session state и writer generation принадлежат `VolumeDirect`; generation проверяется там на каждом I/O, обеспечивая writer fencing;
8. local auto-vhost выключен для cells deployment и остаётся доступным только для конфигураций без cells;
9. до принятия RFC выбран общий YDB gRPC server или отдельный listener внутри `ydbd`, а для выбранного варианта подтверждены сетевая доступность, совместимость настроек TLS/authentication и параметров gRPC-соединений, а также подходящий способ открывать и закрывать classic NBS traffic;
10. определено, должен ли gateway упорядочивать одновременно выполняющиеся записи в пересекающиеся диапазоны блоков, и подтверждено одинаковое поведение gRPC и RDMA.
