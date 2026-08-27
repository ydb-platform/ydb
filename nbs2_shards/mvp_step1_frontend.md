# MVP Step 1: каркас NBS2 frontend

Этот файл содержит последовательную техническую проработку пункта
«Шаг 1. Встроить каркас NBS2 frontend» из `mvp_benchmark.md`.

Решения из `mvp_step0_frontend.md` являются входными ограничениями и повторно
не выбираются. Здесь фиксируются способы их реализации, границы переносимого
кода и проверки результата.

Правила работы:

1. Пункты обсуждаются строго по порядку.
2. Сначала изучаются текущий YDB-код и classic NBS на зафиксированной revision,
   затем в чате формулируются варианты и рекомендуемое решение.
3. После явного согласования решение записывается в этот файл.
4. Для каждого решения записываются выбранный вариант и краткое обоснование.
5. Решение помечается как целевое либо как временное допущение MVP.
6. Для временного допущения дополнительно указываются отличие от целевой
   архитектуры и условие замены.
7. Если краткой записи недостаточно, рядом добавляется подпункт
   «Детализация».
8. Остальные пункты и документы меняются только после отдельного согласования.
9. Незакрытые пункты обозначаются `[ ]`, согласованные — `[x]`.
10. Этот документ не разрешает реализацию кода: изменения кода выполняются
    только по отдельному запросу.

Формат записи согласованного решения:

```text
Решение: <выбранный вариант>
Обоснование: <почему выбран этот вариант>
Статус: целевое решение | временное допущение MVP
Отличие от целевой архитектуры: <только для временного допущения>
Условие замены: <только для временного допущения>
```

## 1. Цель и границы шага

Шаг должен доказать build/runtime совместимость каркаса classic-compatible
frontend с `ydbd`:

```text
classic NBS client/test
    -> отдельный NBS2 gRPC listener внутри ydbd
    -> classic-compatible frontend
    -> Ping или контролируемая ошибка неподдерживаемого RPC
```

В результат всего шага входят:

- перенос и сборка необходимых classic NBS transport-библиотек;
- выключенная по умолчанию конфигурация frontend;
- создание `TNbsFrontend` под существующим `TNbsService`;
- отдельный classic NBS gRPC server и его lifecycle;
- регистрация classic `TBlockStoreService` и `TBlockStoreDataService`;
- минимальный backend adapter для `Ping` и контролируемых ответов остальных
  RPC каркаса;
- compile/dependency boundary RDMA target без запуска RDMA в MVP-1.

В этот шаг не входят `DataRoute`, NBS1 `StartEndpoint`, benchmark session,
доступ к `PartitionTablet`/`FastPathService` и рабочий read/write path. Они
реализуются последующими шагами MVP-1.

## 2. Принятые входные решения

- source для classic protobuf, gRPC server, RDMA wire protocol и error codes —
  revision `953e9966ce94d1724ccdb3e6676ac1f6036f04ae`;
- frontend использует отдельный classic NBS gRPC server внутри `ydbd`, а не
  существующий YDB `TNbsGRpcService`;
- `TNbsServiceInitializer` остаётся единственным composition root NBS2;
- `TNbsService` создаёт опциональный `TNbsFrontend`;
- gRPC server и RDMA target в итоге получают один classic-compatible
  `IBlockStore` adapter;
- frontend и RDMA выключены по умолчанию; в MVP-1 RDMA target не запускается;
- один insecure gRPC listener использует `GrpcHost`/`GrpcPort`, а classic
  `DataPort`, `SecurePort` и `UnixSocketPath` выключены;
- при выключенном frontend новые runtime-компоненты, workers и listeners не
  создаются;
- runtime-решения этого шага не должны добавлять второй backend path или
  benchmark-only protocol.

Подробные обоснования и статусы этих решений находятся в
`mvp_step0_frontend.md`.

## 3. Технический checklist

- [x] **3.1. Перенос transport-библиотек и проверка сборки.** Перенести в YDB
  минимально необходимый код classic gRPC server и RDMA target вместе с
  dependency closure и убедиться, что перенесённые библиотеки собираются.

  **Цель текущего пункта:** отделить механическую совместимость исходников и
  build graph от последующей регистрации frontend в `ydbd`.

  **Исходное состояние:**

  - classic gRPC server находится в `cloud/blockstore/libs/server` и использует
    `NCloud::NBlockStore::IBlockStore` и полный classic protobuf service;
  - classic RDMA target находится в `cloud/blockstore/libs/service_rdma` и
    использует тот же `IBlockStore` contract;
  - текущий YDB subtree `ydb/core/nbs/cloud/blockstore` не содержит этих двух
    библиотек, `cloud/blockstore/public/api/grpc` и transport-support
    библиотек `cloud/storage/core/libs/grpc`, `uds` и `rdma`;
  - существующий YDB `ydb/core/nbs/cloud/blockstore/libs/service` предоставляет
    NBS2 `NYdb::NBS::NBlockStore::IStorage`, а не classic
    `NCloud::NBlockStore::IBlockStore`, поэтому он не является прямой заменой
    classic service library;
  - текущий `ydbd` получает embedded NBS2 через
    `ydb/core/nbs/cloud/blockstore/bootstrap`, но в этом пункте новые
    библиотеки к bootstrap ещё не подключаются.

  Начальные source roots зафиксированы принятой архитектурой:

  ```text
  cloud/blockstore/libs/server
  cloud/blockstore/libs/service_rdma
  cloud/blockstore/public/api/grpc
  ```

  - [x] **3.1.1. Размещение build spike.** Первый перенос выполняется в
    изолированный classic subtree, после определения необходимого состава код
    переносится в обычную структуру NBS2.

    **Решение:** временно разместить перенесённый код в:

    ```text
    ydb/core/nbs/classic/cloud/blockstore/...
    ydb/core/nbs/classic/cloud/storage/...
    ```

    В этом subtree сохраняются classic namespaces и protobuf packages, а
    include paths и `PEERDIR` механически получают новый prefix. После
    успешной сборки и фиксации фактически нужного dependency closure перенести
    только необходимый код в обычные NBS2 paths под `ydb/core/nbs/cloud/...`,
    интегрируя его с уже существующими NBS2 libraries и proto.

    **Обоснование:** изолированный первый перенос позволяет собрать исходные
    classic library targets без преждевременного объединения одноимённых, но
    несовместимых classic и NBS2 `libs/service` и protobuf. Полученный build
    graph показывает фактически необходимые зависимости и места адаптации.
    После этого итоговый перенос можно выполнить осознанно, не сохраняя второй
    постоянный набор classic infrastructure рядом с NBS2.

    **Статус:** временное допущение MVP.

    **Отличие от целевой архитектуры:** RFC не задаёт layout исходного кода.
    Временный `classic` subtree является только инструментом compile/dependency
    spike и не должен становиться отдельной runtime-реализацией frontend.
    Итоговая реализация размещается в обычной структуре NBS2.

    **Условие замены:** после успешной отдельной сборки classic gRPC server и
    RDMA target, фиксации dependency closure и списка немеханических адаптаций
    нужный код переносится под `ydb/core/nbs/cloud/...`, а временный
    `ydb/core/nbs/classic/...` удаляется.

    **Детализация:** второй перенос не означает копирование поверх существующих
    файлов. Для каждого конфликтующего library/proto target отдельно
    выбирается переиспользование NBS2-кода, адаптация classic-кода либо новый
    NBS2 target. Эта раскладка фиксируется после build spike, когда известны
    реальные зависимости.

  - [x] **3.1.2. Dependency closure и build targets.**

    **Решение:** начать build spike с переноса следующих корневых classic
    targets:

    ```text
    cloud/blockstore/public/api/grpc
    cloud/blockstore/libs/server
    cloud/blockstore/libs/service_rdma
    cloud/blockstore/libs/rdma
    ```

    `cloud/blockstore/libs/rdma` переносится отдельно от
    `cloud/blockstore/libs/service_rdma`: первый target создаёт RDMA server и
    приводит к RDMA implementation libraries, второй реализует blockstore RDMA
    target поверх classic `IBlockStore`.

    Начальный dependency closure для gRPC части:

    ```text
    cloud/blockstore/public/api/protos
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/uds
    ```

    Начальный dependency closure для RDMA части:

    ```text
    cloud/blockstore/libs/storage/protos
    cloud/storage/core/libs/rdma/iface
    cloud/storage/core/libs/rdma/impl
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    ```

    Это начальный, а не окончательный список. Полный closure определяется по
    `ya.make`, include graph и результатам сборки зафиксированной classic
    revision `953e9966ce94d1724ccdb3e6676ac1f6036f04ae`. Необходимые classic
    targets переносятся во временный `ydb/core/nbs/classic/cloud/...` subtree.
    Совместимые общие зависимости из `library/`, `contrib/` и существующих YDB
    targets переиспользуются без копирования.

    Допустимыми механическими адаптациями считаются:

    - добавление временного path prefix в include/import paths;
    - изменение `PEERDIR` на пути временного subtree;
    - адаптация `ya.make` к YDB build tree;
    - замена зависимости на уже существующую совместимую common library;
    - сохранение classic namespaces, protobuf packages, RPC и RDMA wire
      contracts.

    Отдельного решения требуют и не выполняются в build spike скрыто:

    - замена classic `IBlockStore` на NBS2 `IStorage`;
    - удаление RPC из classic service;
    - изменение protobuf fields или packages;
    - переписывание gRPC server или RDMA protocol;
    - добавление заглушки вместо отсутствующей существенной зависимости.

    Корневые targets проверяются отдельно следующими командами:

    ```bash
    ./ya make --build relwithdebinfo \
      ydb/core/nbs/classic/cloud/blockstore/public/api/grpc

    ./ya make --build relwithdebinfo \
      ydb/core/nbs/classic/cloud/blockstore/libs/server

    ./ya make --build relwithdebinfo \
      ydb/core/nbs/classic/cloud/blockstore/libs/service_rdma

    ./ya make --build relwithdebinfo \
      ydb/core/nbs/classic/cloud/blockstore/libs/rdma
    ```

    На этом этапе не запускаются tests, полный `ydbd` и runtime frontend.
    Результат spike фиксируется таблицей со следующими полями:

    ```text
    classic source target
    -> temporary target
    -> copied | reused
    -> mechanical changes
    -> blocker
    ```

    Если сборка требует изменения wire contract, замены `IBlockStore` либо
    существенной переработки server, это фиксируется как dependency blocker и
    выносится на отдельное согласование. Такой результат не считается успешным
    завершением build spike.

    При первом переносе source library targets не сокращаются и их dependency
    closure заранее не оптимизируется. Удаление ненужных для NBS2 частей,
    включая UDS, TLS и test-related code, рассматривается при итоговом переносе
    в пункте 3.1.3 после получения собираемого исходного graph.

    Проверка считается успешной, когда все четыре временных корневых target
    собираются в режиме `relwithdebinfo`, составлена таблица переноса и явно
    перечислены все выявленные немеханические blockers.

    **Обоснование:** перенос исходных targets без предварительного pruning
    отделяет проблемы совместимости build graph от архитектурной адаптации к
    NBS2. Это даёт проверяемый список реально нужных зависимостей и не требует
    заранее угадывать, какие части classic server можно удалить.

    **Статус:** временное допущение MVP.

    **Отличие от целевой архитектуры:** временный closure повторяет исходный
    classic build graph и может включать не используемые итоговым NBS2 frontend
    части. Он нужен только для compile/dependency spike и не изменяет runtime
    архитектуру RFC.

    **Условие замены:** после успешной сборки и фиксации blockers пункт 3.1.3
    переносит только требуемый код в обычные NBS2 paths, устраняет ненужные
    зависимости и удаляет временный `ydb/core/nbs/classic/...` subtree.

  - [x] **3.1.3. Итоговое размещение в NBS2 tree.**

    **Решение:** после успешного build spike разделить перенесённый код на
    постоянный classic compatibility boundary и общие NBS2 transport
    libraries. Использовать следующую итоговую раскладку:

    | Classic source | Итоговое размещение |
    |---|---|
    | `cloud/blockstore/public/api/protos` и `grpc` | `ydb/core/nbs/cloud/blockstore/compat/public/api/{protos,grpc}` |
    | Требуемая часть `cloud/blockstore/config` | `ydb/core/nbs/cloud/blockstore/compat/config` |
    | Требуемые classic `common`, `diagnostics` и `IBlockStore` service contract | `ydb/core/nbs/cloud/blockstore/compat/libs/{common,diagnostics,service}` |
    | `cloud/blockstore/libs/server` | `ydb/core/nbs/cloud/blockstore/compat/libs/server` |
    | `cloud/blockstore/libs/service_rdma` и `rdma` | `ydb/core/nbs/cloud/blockstore/compat/libs/{service_rdma,rdma}` |
    | Требуемые classic storage wire protobuf | `ydb/core/nbs/cloud/storage/core/compat/protos` |
    | Общие `cloud/storage/core/libs/grpc`, `uds` и `rdma` | `ydb/core/nbs/cloud/storage/core/libs/{grpc,uds,rdma}` |
    | Уже существующие NBS2 `common`, `coroutine` и `diagnostics` | переиспользовать и при необходимости дополнить недостающими API без создания их classic-копии |

    Код в `blockstore/compat` и `storage/core/compat` сохраняет classic
    protobuf packages, gRPC RPC contract, `IBlockStore` contract, error codes,
    RDMA message IDs и wire layout. Общие transport implementation libraries
    переносятся как обычный NBS2-код: используют пути `ydb/core/nbs/...` и
    namespace `NYdb::NBS`; compatibility server и RDMA target адаптируются к
    этим внутренним libraries без изменения внешнего classic contract.

    **Правило фиксации состава:** приведённая таблица задаёт постоянный layout
    и dependency boundary, но не является предположительным file manifest.
    Точный список переносимых файлов и targets формируется только из manifest
    фактически успешного build spike пункта 3.1.2. Для каждого элемента этого
    manifest фиксируется:

    ```text
    classic source target/file
    -> temporary target/file
    -> final target/file
    -> copied | reused | adapted | excluded
    -> причина выбора
    -> dependency blocker, если есть
    ```

    До получения такого manifest не добавляются зависимости «на всякий
    случай» и не объявляется точный минимальный состав итогового переноса.
    Если manifest выявляет немеханическое изменение wire contract,
    `IBlockStore` contract или существенную переработку server, перенос
    останавливается для отдельного согласования.

    После переноса итоговые корневые targets проверяются отдельно:

    ```bash
    ./ya make --build relwithdebinfo \
      ydb/core/nbs/cloud/blockstore/compat/public/api/grpc

    ./ya make --build relwithdebinfo \
      ydb/core/nbs/cloud/blockstore/compat/libs/server

    ./ya make --build relwithdebinfo \
      ydb/core/nbs/cloud/blockstore/compat/libs/service_rdma

    ./ya make --build relwithdebinfo \
      ydb/core/nbs/cloud/blockstore/compat/libs/rdma
    ```

    Временный `ydb/core/nbs/classic/...` subtree удаляется только после
    успешной сборки этих четырёх targets и проверки, что каждый элемент
    manifest получил итоговое решение.

    **Обоснование:** существующие NBS2 protobuf и `libs/service` имеют другие
    packages, namespaces и внутренний `IStorage` contract, поэтому classic
    wire/API нельзя переносить поверх них. Отдельный постоянный `compat`
    boundary исключает конфликт и явно показывает назначение classic-кода.
    Общие gRPC, UDS и RDMA libraries при этом становятся обычными NBS2
    libraries и не создают второй постоянный набор transport infrastructure.

    **Статус:** целевое решение. Постоянными являются layout и dependency
    boundary, описанные выше.

    **Временное допущение MVP:** при итоговом переносе не переписывать classic
    gRPC server только ради удаления compile-time UDS/TLS code. Эти возможности
    остаются собранными, но отключаются конфигурацией и не создают listeners
    или runtime components в MVP.

    **Отличие от целевой архитектуры:** RFC не требует UDS/TLS в этом frontend;
    их наличие ограничено внутренним compile-time closure перенесённого server
    и не меняет gRPC/RDMA runtime path.

    **Условие замены:** удалить UDS/TLS compile-time зависимости, если после
    получения рабочего E2E path будет подтверждено, что их изоляция заметно
    уменьшает dependency closure и не требует существенной переработки classic
    server. Для соответствия RFC такая замена не обязательна.

  Граница пункта 3.1:

  - не добавлять `TNbsFrontendConfig` и `TNbsFrontend`;
  - не подключать библиотеки к `TNbsService` или `ydbd` runtime;
  - не открывать gRPC/RDMA ports;
  - не реализовывать `Ping`, session или I/O handlers;
  - не создавать common backend adapter;
  - не запускать тестовый server или RDMA target.

  Пункт будет закрыт, когда согласованный минимальный набор перенесённых
  библиотек находится в обычном NBS2 tree и отдельно собирается в режиме
  `relwithdebinfo`, временный `classic` subtree удалён, а все необходимые
  немеханические изменения перечислены как вход для следующих пунктов. Сборка
  и линковка полного `ydbd` с frontend проверяются после подключения библиотек
  к `TNbsService`, а не в этом пункте.

### Реализационный checkpoint перед продолжением проработки

**Статус реализации пункта 3.1:** не выполнено.

Отметки `[x]` в пункте 3.1 означают, что технические решения согласованы, но
не подтверждают фактический перенос и сборку кода. Перед переходом к
проработке пункта 3.2 необходимо в отдельной сессии реализовать 3.1 по
описанию выше и получить следующие результаты:

- четыре временных корневых target собираются в режиме `relwithdebinfo`;
- сформирован фактический dependency/file manifest и перечислены все
  немеханические blockers;
- каждый blocker, мешающий итоговому переносу, отдельно согласован;
- код перенесён в итоговые `compat` и NBS2 transport paths, а четыре итоговых
  корневых target собираются в режиме `relwithdebinfo`;
- временный `ydb/core/nbs/classic/...` subtree удалён.

До выполнения checkpoint дальнейшая проработка пунктов 3.2–3.9
приостанавливается. После реализации фактические manifest, адаптации и blockers
должны быть сверены с решениями 3.1; при расхождении сначала обновляется
проработка 3.1, затем текущим пунктом снова становится 3.2.

- [ ] **3.2. Frontend configuration schema.** Зафиксировать техническое
  представление принятой `TNbsConfig.NbsFrontendConfig` и способ передачи
  конфигурации в embedded NBS2.

  Нужно определить:

  - расположение protobuf message и номера новых полей;
  - поля `Enabled`, `GrpcHost`, `GrpcPort`, `RdmaEnabled`, `RdmaHost` и
    `RdmaPort` с принятыми defaults;
  - C++ config wrapper либо прямое использование protobuf;
  - построение classic server config с `DataPort = 0`, `SecurePort = 0` и
    пустым `UnixSocketPath`;
  - поведение `RdmaEnabled = true` до реализации runtime RDMA frontend в
    MVP-2;
  - отсутствие в frontend config полей `DiskId`, `TabletId` и `DataRoute`.

- [ ] **3.3. Валидация и активация конфигурации.** Зафиксировать место, момент
  и результат проверки активной frontend-конфигурации.

  Нужно определить:

  - кто валидирует `TNbsConfig.Enabled`, gRPC и RDMA поля;
  - почему при `Enabled = false` остальные frontend-поля игнорируются;
  - порядок validation относительно создания components и открытия ports;
  - формат диагностической ошибки с полным путём поля и причиной;
  - поведение при невозможности bind и при частично выполненном startup;
  - минимальные unit tests для отсутствующей, выключенной, валидной и
    невалидной конфигурации.

- [ ] **3.4. Classic-compatible `IBlockStore` facade каркаса.** Зафиксировать
  минимальную реализацию classic service interface, которую можно передать
  перенесённым gRPC server и RDMA target без подключения NBS2 backend.

  Нужно определить:

  - target и namespace facade;
  - способ реализации полного pure-virtual classic `IBlockStore` interface без
    дублирования boilerplate для неподдерживаемых RPC;
  - ownership facade и его связь с transport components;
  - реализацию `AllocateBuffer`, `Start` и `Stop`, необходимую transport code;
  - успешную семантику `Ping`;
  - контролируемую classic error для `MountVolume`, `UnmountVolume`,
    `ReadBlocks`, `WriteBlocks`, `ZeroBlocks`, `DescribeVolume` и остальных
    неподдерживаемых RPC;
  - отсутствие session state, partition registry и вызовов `IStorage` в этом
    шаге.

- [ ] **3.5. Composition и ownership `TNbsFrontend`.** Зафиксировать классы и
  связи, которыми существующий embedded NBS2 создаёт каркас frontend.

  Нужно определить:

  - расположение `TNbsFrontend` и его public interface;
  - создание frontend внутри `TNbsService`, а не через второй process-wide
    initializer;
  - зависимости, передаваемые из `TNbsService` в frontend;
  - ownership `IBlockStore` facade и classic gRPC server;
  - отсутствие создания RDMA runtime components при `RdmaEnabled = false`;
  - запрет запуска threads и открытия ports из constructors;
  - изменения `ya.make`, необходимые для линковки components в embedded NBS2.

- [ ] **3.6. Создание classic gRPC server и listener.** Зафиксировать
  технический способ запуска перенесённого server на отдельном listener внутри
  `ydbd`.

  Нужно определить:

  - преобразование `TNbsFrontendConfig` в аргументы `CreateServer`;
  - необходимые logging, server stats и certificate-provider dependencies;
  - передачу одного facade как control и data service backend;
  - регистрацию classic `TBlockStoreService` и `TBlockStoreDataService`;
  - использование одного `GrpcHost:GrpcPort` без classic data, secure и UDS
    listeners;
  - изоляцию от существующего YDB `TNbsGRpcService` и общего YDB gRPC server;
  - диагностирование ошибки создания listener.

- [ ] **3.7. Lifecycle и обработка startup/shutdown errors.** Зафиксировать
  реализацию принятого ownership и порядка запуска для каркаса Шага 1.

  Нужно определить:

  - вызовы frontend из `TNbsService::Start()` и `TNbsService::Stop()`;
  - состояния, достаточные для `Starting`, `Ready`, `Stopping` и повторного
    `Stop` после частичного startup;
  - запуск facade до gRPC server и остановку server до facade;
  - fail-fast propagation ошибки listener до запуска `ydbd`;
  - rollback уже созданных или запущенных components;
  - завершение либо отмену принятых transport requests при shutdown;
  - отсутствие RDMA startup/shutdown в MVP-1.

- [ ] **3.8. Выключенное состояние и обратная совместимость.** Зафиксировать,
  что отсутствующая или выключенная frontend-конфигурация не меняет обычный
  embedded NBS2 runtime.

  Нужно определить:

  - ветвление создания frontend при `Enabled = false`;
  - отсутствие facade, gRPC/RDMA workers, listeners и иных side effects;
  - неизменность текущего scheduler/vhost lifecycle `TNbsService`;
  - неизменность существующего YDB `TNbsGRpcService`;
  - сохранение local auto-vhost при выключенном frontend;
  - build/runtime tests конфигураций без новой секции и с явным
    `Enabled = false`.

- [ ] **3.9. Build и runtime verification каркаса.** Зафиксировать бинарные
  проверки результата всего Шага 1.

  Нужно определить:

  - build targets для изменённых libraries и полного `ydbd`;
  - unit tests facade, config validation и lifecycle rollback;
  - запуск `ydbd` с выключенным frontend без новых listeners;
  - запуск с включённым frontend и открытие ровно одного classic gRPC listener;
  - compatible `Ping` через classic NBS client;
  - controlled classic errors для неподдерживаемых RPC, включая
    `DescribeVolume`;
  - проверку отсутствия RDMA listener при `RdmaEnabled = false`;
  - фиксацию состава перенесённых dependencies либо конкретного blocker;
  - подтверждение, что рабочий session/data path и отдельный benchmark protocol
    в каркас не добавлены.

## 4. Ожидаемый результат

После закрытия checklist:

- необходимые classic transport libraries размещены в обычном NBS2 tree и
  собираются; временный `classic` subtree отсутствует;
- `ydbd` собирается с перенесённым classic gRPC server и compile-time RDMA
  dependency boundary;
- при выключенном frontend обычный embedded NBS2 runtime не меняется;
- при включённом frontend отдельный classic gRPC listener запускается и
  отвечает на `Ping`;
- неподдерживаемые RPC завершаются контролируемой classic error;
- `DescribeVolume` не становится runtime API NBS2 frontend;
- RDMA target в MVP-1 не запускается;
- session, `DataRoute`, partition registry и рабочий read/write path остаются
  за пределами этого шага.

## 5. Текущий пункт

Текущая работа — **фактическая реализация пункта 3.1 и прохождение
реализационного checkpoint**. Следующий пункт проработки после checkpoint —
**3.2. Frontend configuration schema**.
