# MVP Step 0: регистрация и включение NBS2 frontend

Этот файл содержит вопросы, которые нужно последовательно зафиксировать для
пункта «определить регистрацию frontend внутри `ydbd`, gRPC/RDMA ports и
конфигурацию безопасного включения».

Правила работы:

1. Пункты обсуждаются строго по порядку.
2. Сначала фиксируются неоднозначности и выбирается решение в чате.
3. После явного согласования решение записывается в этот файл.
4. Для каждого решения записываются выбранный вариант и краткое обоснование.
5. Решение помечается как целевое либо как временное допущение MVP.
6. Для временного допущения дополнительно указываются отличие от целевой
   архитектуры и условие, при котором оно должно быть заменено.
7. Если краткой записи недостаточно, рядом создаётся отдельный подпункт
   «Детализация» с необходимым контекстом и последствиями решения.
8. Остальные документы меняются только после отдельного согласования.
9. Незакрытые пункты обозначаются `[ ]`, согласованные — `[x]`.

Формат записи согласованного решения:

```text
Решение: <выбранный вариант>
Обоснование: <почему выбран этот вариант>
Статус: целевое решение | временное допущение MVP
Отличие от целевой архитектуры: <только для временного допущения>
Условие замены: <только для временного допущения>
```

## 1. Регистрация frontend

- [x] **1.1. gRPC listener.** Classic NBS gRPC service использует отдельный
  gRPC server внутри `ydbd`.

  **Решение:** перенести в NBS2 classic NBS gRPC server из зафиксированной
  source revision `953e9966ce94d1724ccdb3e6676ac1f6036f04ae` и запустить его
  внутри `ydbd` на отдельном listener. Общий gRPC server `ydbd` для classic
  NBS frontend не используется.

  **Обоснование:** отдельный server позволяет повторно использовать штатную
  classic реализацию `TBlockStoreService` и `TBlockStoreDataService`, включая
  async request handling и classic wire contract, вместо переноса этих деталей
  на YDB gRPC framework. Один classic-compatible `IBlockStore` adapter можно
  передать как gRPC server, так и RDMA target, сохранив общий NBS2 backend для
  обоих transport. Frontend получает собственный управляемый lifecycle: server
  начинает принимать запросы только после готовности frontend и прекращает их
  до остановки backend. Это также изолирует classic dependencies, traffic и
  transport configuration от существующих YDB gRPC services.

  Цена решения — отдельные port, completion queues/threads и конфигурация
  listener. Для MVP эта цена принимается ради сокращения объёма новой gRPC
  обвязки и более прямого переиспользования проверенного classic server.

  **Статус:** временное допущение MVP, которое может стать итоговым решением.

  **Отличие от целевой архитектуры:** RFC не фиксирует способ размещения gRPC
  listener, поэтому отдельный server не противоречит целевой архитектуре. На
  этапе MVP окончательный deployment choice не утверждается.

  **Условие замены:** решение пересматривается, если перенос classic server
  потребует несоразмерного набора daemon dependencies либо отдельный listener
  окажется несовместим с требованиями к сети, TLS/authentication или
  эксплуатации. При отсутствии таких ограничений отдельный server может
  остаться итоговым вариантом.

- [x] **1.2. Создание компонентов.** Classic NBS gRPC server, RDMA target и
  общий NBS2 backend adapter создаются централизованно как часть NBS2
  frontend.

  **Решение:** существующий `TNbsServiceInitializer` остаётся единственным
  composition root NBS2 внутри `ydbd` и создаёт `TNbsService`. При включённой
  frontend-конфигурации `TNbsService` создаёт опциональный `TNbsFrontend`,
  который централизованно создаёт и хранит:

  - classic-compatible `IBlockStore` backend adapter;
  - перенесённый classic NBS gRPC server;
  - classic-compatible RDMA target.

  **Обоснование:** все transport используют один backend adapter и собираются
  из одной конфигурации, при этом не появляется второй process-wide initializer
  и отдельный глобальный владелец frontend. `TNbsServiceInitializer` уже
  является точкой создания общих NBS2 runtime-компонентов, поэтому размещение
  frontend под `TNbsService` соответствует существующей структуре кода.

  **Статус:** целевое решение.

  **Детализация:** `FastPathService` не становится process-wide компонентом
  frontend. Его по-прежнему создаёт и хранит конкретная `PartitionTablet`, а
  общий backend adapter предоставляет transport-neutral доступ к выбранной
  локальной partition. Конкретный механизм регистрации и удаления partition в
  frontend рассматривается в пункте 1.4. Порядок запуска и остановки созданных
  компонентов рассматривается отдельно в пункте 1.3.

- [x] **1.3. Lifecycle.** `TNbsService` управляет lifecycle frontend, а
  `TNbsFrontend` — lifecycle принадлежащих ему transport и backend-компонентов.

  **Решение:** `TNbsService::Start()` вызывает `TNbsFrontend::Start()`, а
  `TNbsService::Stop()` — `TNbsFrontend::Stop()`. Конструкторы frontend и его
  компонентов не запускают потоки и не открывают ports.

  Порядок запуска:

  1. подготовить common backend adapter и local-partition registry;
  2. запустить RDMA target, если он включён для текущего этапа;
  3. запустить отдельный classic NBS gRPC server;
  4. перевести frontend в состояние `Ready`.

  gRPC server запускается последним, поскольку через него выполняется
  `MountVolume`: frontend не должен успешно завершать mount до готовности
  настроенного data transport. В MVP-1 шаг запуска RDMA отсутствует.

  Порядок остановки:

  1. перевести frontend в состояние `Stopping` и запретить новые backend calls;
  2. прекратить приём новых classic gRPC requests;
  3. остановить RDMA target;
  4. дождаться завершения либо штатно отменить transport requests in flight;
  5. очистить process-local session state и ссылки local-partition registry.

  **Обоснование:** один владелец обеспечивает согласованный lifecycle общего
  backend и обоих transport. Frontend прекращает принимать новые запросы до
  освобождения backend state, а при запуске не публикует работоспособный
  control path раньше data path.

  **Статус:** целевое решение.

  **Детализация:** состояние `Ready` означает готовность frontend и включённых
  transport, но не наличие конкретной partition. Отсутствующая или уже
  перемещённая локальная partition приводит к контролируемой ошибке запроса и
  не требует перезапуска listener. `FastPathService` остаётся во владении
  `PartitionTablet`; её запуск и остановка не управляют lifecycle frontend.
  Политика обработки ошибки запуска listener и выбор fail-fast поведения
  рассматриваются отдельно в пунктах 2.4 и 3.5.

- [x] **1.4. Strict affinity.** Frontend получает доступ к NBS2 partition только
  через process-local registry и не выполняет меж-host forwarding.

  **Решение:** `TNbsFrontend` хранит registry локальных partition. После
  готовности `FastPathService` конкретная `PartitionTablet` прямым вызовом
  внутри того же процесса регистрирует tuple:

  ```text
  DiskId, TabletId, TabletGeneration, IStoragePtr
  ```

  Регистрация возвращает generation-aware `RegistrationToken`. Перед
  остановкой `FastPathService` partition удаляет регистрацию по этому token.
  Запоздалый unregister старой tablet generation не должен удалить более новую
  регистрацию того же `DiskId`.

  Common backend выполняет `DiskId` lookup только в этом registry и получает
  strong `IStoragePtr` на время запроса. `MountVolume` успешно завершается
  только при наличии требуемой локальной partition. gRPC и RDMA используют
  один registry через общий backend adapter.

  **Обоснование:** registry может быть заполнен только partition из текущего
  процесса, поэтому сам способ получения `IStoragePtr` гарантирует host
  affinity. После unregister новые запросы перестают получать backend, а уже
  принятый запрос удерживает ссылку до terminal completion.

  **Статус:** целевое решение.

  **Детализация:** frontend не обращается к SchemeShard, Hive или tablet pipe
  для поиска partition на другом host и не использует NBS1 storage fallback.
  Отсутствие регистрации, несовпадение ожидаемых metadata или устаревшая
  generation завершают запрос контролируемой ошибкой без forwarding. Точный
  classic error code и stale-route indication фиксируются отдельно вместе с
  error mapping.

## 2. Порты и публикация endpoints

- [x] **2.1. gRPC endpoint.** Frontend использует один явно настроенный
  dedicated insecure gRPC listener.

  **Решение:** frontend-конфигурация содержит обязательные при включённом
  frontend поля `GrpcHost` и `GrpcPort`. `GrpcHost` задаёт bind address, а
  ненулевой `GrpcPort` — отдельный port classic NBS gRPC server. Числовое
  значение port не фиксируется архитектурным документом и задаётся
  benchmark-конфигурацией конкретного окружения.

  Перенесённому classic server явно задаются:

  ```text
  Host = GrpcHost
  Port = GrpcPort
  DataPort = 0
  SecurePort = 0
  UnixSocketPath = ""
  ```

  `TBlockStoreService` обслуживает через этот endpoint `Ping`, session RPC и
  gRPC I/O MVP-1. В MVP-2 тот же endpoint остаётся control plane, а основной
  data path переключается на отдельный RDMA endpoint.

  **Обоснование:** штатный cells gRPC transport создаёт один endpoint по host и
  `GrpcPort` и использует его как для service, так и для gRPC storage. Отдельный
  classic `DataPort` не нужен и только добавил бы второй listener и
  конфигурацию. Явный port исключает неявное использование classic defaults и
  возможный конфликт с co-located classic NBS.

  **Статус:** временное допущение MVP, которое может стать итоговым решением.

  **Отличие от целевой архитектуры:** RFC не требует TLS и не фиксирует
  разделение control/data gRPC listeners. Один insecure listener не меняет
  service/data contracts, но не утверждается как окончательная security и
  deployment configuration.

  **Условие замены:** решение пересматривается при появлении требований к TLS,
  отдельным control/data listeners или независимо управляемому traffic.

  **Детализация:** `GrpcHost` является только bind address. Доступный из NBS1
  cells адрес или FQDN, публикуемый в `DataRoute`, определяется отдельно в
  пункте 2.3. `GrpcPort = 0` и другие ошибки endpoint-конфигурации
  рассматриваются в пункте 2.4.
- [x] **2.2. RDMA endpoint.** RDMA target использует отдельные явно
  настроенные параметры `RdmaEnabled`, `RdmaHost` и `RdmaPort`.

  **Решение:** `RdmaEnabled` независимо включает RDMA target. При
  `RdmaEnabled = false` target не запускается, `RdmaHost` и `RdmaPort` не
  используются. При `RdmaEnabled = true` обязательны непустой `RdmaHost` и
  ненулевой `RdmaPort`.

  `RdmaHost` задаёт bind address RDMA-интерфейса, а не публикуемый для NBS1
  адрес. Числовое значение `RdmaPort` задаётся benchmark-конфигурацией
  конкретного окружения и не фиксируется архитектурным документом.

  Перенесённому classic RDMA target передаются:

  ```text
  Endpoint.Host = RdmaHost
  Endpoint.Port = RdmaPort
  ```

  Один RDMA listener обслуживает все локально зарегистрированные NBS2
  partitions; нужная partition выбирается общим frontend backend через
  process-local registry. RDMA target запускается в `TNbsFrontend::Start()`
  после готовности backend и registry и останавливается через
  `TNbsFrontend::Stop()` в порядке из пункта 1.3.

  В MVP-1 `RdmaEnabled = false`. В MVP-2 `RdmaEnabled = true`, и RDMA target
  принимает штатный cells RDMA data path. gRPC control plane при этом остаётся
  включённым.

  **Обоснование:** отдельный `RdmaHost` не связывает RDMA bind address с
  `GrpcHost`: gRPC может слушать wildcard или обычный сетевой интерфейс, тогда
  как RDMA target может требовать адрес конкретного RDMA/RoCE-интерфейса.
  Отдельный `RdmaEnabled` делает включение явным и позволяет считать отсутствие
  host или port ошибкой конфигурации, а не неявным отключением RDMA.

  **Статус:** временное допущение MVP, которое может стать итоговым решением.

  **Отличие от целевой архитектуры:** RFC требует совместимый RDMA endpoint на
  том же NBS2 host, что и gRPC service, но не фиксирует формат конфигурации и
  способ отдельного включения target. Выбранные параметры не меняют wire
  protocol или общий backend contract.

  **Условие замены:** конфигурация пересматривается, если deployment должен
  получать RDMA endpoint из общего сетевого конфигуратора или управлять им
  централизованно вне NBS2 frontend.

  **Детализация:** публикуемый NBS1 адрес и связь RDMA `Storage` с gRPC
  `Service` определяются в пунктах 2.3 и 2.5. Ошибки конфигурации и открытия
  listener определяются в пунктах 2.4 и 3.5. Механизм общего безопасного
  включения frontend определяется в разделе 3.
- [x] **2.3. DataRoute.** Для MVP `DataRoute` хранится статически на стороне
  classic NBS в cell-владельце диска и возвращается NBS1, обрабатывающему
  `StartEndpoint`, отдельной операцией `GetDataRoute`.

  **Решение:** discovery и получение data route остаются разными операциями.
  Для диска, принадлежащего cell2, последовательность имеет вид:

  ```text
  NBS1_1 (cell1): StartEndpoint
      -> NBS1_2 (cell2): DescribeVolume
      -> SchemeShard cell2
      <- Volume + CellId=cell2 + признак fast_disk

  NBS1_1
      -> NBS1_2: GetDataRoute(DiskId)

  NBS1_2
      -> static benchmark route для DiskId
      <- Nbs2Fqdn + GrpcPort + optional RdmaPort + Transport

  NBS1_1
      -> NBS2 host: MountVolume и data I/O
  ```

  `DescribeVolume` обслуживается NBS1 в cell-владельце и не направляется в
  NBS2 frontend. Статическая route конфигурируется на том же NBS1 и временно
  заменяет получение актуального placement из Hive. Вызывающий `NBS1_1` не
  хранит локальное соответствие `DiskId -> NBS2 endpoint`.

  Минимальный ответ `GetDataRoute` для MVP содержит:

  ```text
  DiskId
  OwnerCellId
  Nbs2Fqdn
  GrpcPort
  optional RdmaPort
  Transport = CELL_DATA_TRANSPORT_GRPC | CELL_DATA_TRANSPORT_RDMA
  ```

  `Nbs2Fqdn` должен быть доступен из `NBS1_1` для обоих transport endpoints.
  Это публикуемый адрес, отличный от bind-only `GrpcHost` и `RdmaHost` в
  конфигурации NBS2 frontend.

  В MVP-1 route материализуется как:

  ```text
  Service = Nbs2Fqdn:GrpcPort
  Storage = Nbs2Fqdn:GrpcPort
  Transport = CELL_DATA_TRANSPORT_GRPC
  ```

  В MVP-2 route материализуется как:

  ```text
  Service = Nbs2Fqdn:GrpcPort
  Storage = Nbs2Fqdn:RdmaPort
  Transport = CELL_DATA_TRANSPORT_RDMA
  ```

  gRPC endpoint продолжает обслуживать control plane в MVP-2 и остаётся
  доступным как gRPC data endpoint. Автоматический RDMA-to-gRPC fallback в MVP
  не реализуется: transport выбирается конфигурацией этапа, а недоступный RDMA
  завершает создание endpoint ошибкой.

  **Обоснование:** текущий cells-код получает из `DescribeVolume` только
  `CellId`, а одни и те же hosts из `TCellConfig` использует для discovery,
  session и data path. Для `fast_disk` это недостаточно: discovery должен идти
  в NBS1 cell-владельца, а `MountVolume` и I/O — на конкретный NBS2 host.
  Отдельный `GetDataRoute` сохраняет разделение обязанностей из RFC.

  **Статус:** временное допущение MVP.

  **Отличие от целевой архитектуры:** RFC предусматривает получение
  `DataRoute` через NBS1 cell-владельца и актуальный placement resolver. В MVP
  сохраняется этот control flow, но resolver заменяется статической записью
  для одного диска; route generation и автоматическое обновление отсутствуют.

  **Условие замены:** статическая запись заменяется production resolver на
  стороне cell-владельца, когда будет реализовано получение placement и
  обработка migration/restart.

  **Детализация:** в текущем cells-коде отсутствует готовая операция
  `GetDataRoute` с endpoint-bearing response. Для MVP необходимо добавить её
  минимальный wire/API contract и построение связанных `Service`/`Storage` из
  ответа. Это требуемая реализация абстрактного контракта RFC, а не изменение
  архитектурного направления. Конкретное соответствие опубликованных
  endpoints одному NBS2 host проверяется по правилам пункта 2.5.
- [x] **2.4. Ошибки конфигурации.** Явно включённый, но не запустившийся
  frontend является фатальной ошибкой запуска всего `ydbd`.

  **Решение:** frontend переходит в `Ready` только после успешного запуска всех
  включённых transport-компонентов. Если classic gRPC server не может открыть
  listener, `TNbsFrontend::Start()` завершается ошибкой и запуск `ydbd`
  прекращается.

  При `RdmaEnabled = true` невозможность запустить RDMA server или открыть RDMA
  endpoint имеет то же поведение. При `RdmaEnabled = false` RDMA server, target,
  workers и listener вообще не создаются и не запускаются.

  Частично выполненный запуск откатывается в обратном порядке. Например, если
  RDMA target уже запущен, а следующий запуск gRPC listener завершился ошибкой,
  frontend останавливает частично созданный gRPC server и RDMA target, после
  чего передаёт ошибку выше. В состояние `Ready` он не переходит.

  Диагностическая ошибка запуска содержит как минимум:

  ```text
  component = grpc | rdma
  bind host
  bind port
  исходную причину
  ```

  **Обоснование:** продолжение работы `ydbd` с молча выключенным frontend
  оставило бы возможность опубликовать статическую route на фактически
  отсутствующий endpoint. Явное включение frontend означает, что невозможность
  выполнить его конфигурацию должна быть видна как ошибка deployment.

  **Статус:** целевое решение.

  **Детализация реализации:** перенесённый classic gRPC server уже завершает
  `Start()` ошибкой, если `BuildAndStart()` не смог создать server. Classic
  RDMA target при `StartEndpoint() == nullptr` сейчас только пишет ошибку в лог
  и возвращается из `Start()`. При интеграции этот результат необходимо
  передать как ошибку в `TNbsFrontend::Start()`, чтобы frontend не мог ложно
  перейти в `Ready`.

  Отсутствие обязательного endpoint в `DataRoute` обнаруживается до отправки
  `MountVolume`: обязательны gRPC `Service`, а при выбранном RDMA transport —
  RDMA `Storage`. Неполная route завершает `StartEndpoint` ошибкой без legacy
  NBS1 storage fallback и без автоматического RDMA-to-gRPC fallback.

  Если endpoint присутствует в route, но недоступен по сети, NBS1 возвращает
  transport/retriable error и не создаёт пользовательский endpoint. Проверка
  согласованности опубликованных и реально запущенных endpoints уточняется в
  пунктах 2.5 и 3.6.
- [x] **2.5. Связность endpoints.** В MVP принадлежность `Service` и `Storage`
  одному NBS2 host обеспечивается структурой статической `DataRoute` и
  process-local проверкой диска.

  **Решение:** `DataRoute` не содержит независимые hosts для `Service` и
  `Storage`. Она содержит один `Nbs2Fqdn` и отдельные `GrpcPort`/`RdmaPort`.
  `Nbs2Fqdn` указывает непосредственно на один NBS2 host и в течение прогона
  не использует load balancer, DNS round-robin или раздельный port forwarding.

  Оба listener создаются одним экземпляром `TNbsFrontend` в одном процессе
  `ydbd` и используют общий session adapter, backend и process-local partition
  registry:

  ```text
  DataRoute {
      Nbs2Fqdn = host-A
      GrpcPort = 9766
      RdmaPort = 10088
  }

  host-A/ydbd/TNbsFrontend
      ├── gRPC listener :9766
      ├── RDMA listener :10088
      └── common backend + local partition registry
  ```

  Backend принимает запрос только для `DiskId`, зарегистрированного локальной
  PartitionTablet. Route на другой host завершается контролируемой ошибкой без
  меж-host forwarding и legacy NBS1 storage fallback.

  **Обоснование:** для одного диска с закреплённым placement отдельный runtime
  identity handshake не нужен, чтобы построить корректный MVP-1/2 path. Один
  host в route исключает конфигурацию разных gRPC/RDMA hosts, а локальный
  registry не позволяет ошибочно провести запрос к отсутствующей partition.

  **Статус:** временное допущение MVP.

  **Отличие от целевой архитектуры:** RFC требует гарантировать принадлежность
  session- и data-endpoints одному NBS2 host/incarnation, но пока не определяет
  конкретный механизм. В MVP не вводятся отдельные `HostId`,
  `FrontendInstanceId` или handshake проверки incarnation.

  **Условие замены:** production `DataRoute` должна получить идентификатор
  host/incarnation либо route generation, проверяемые на обоих transport
  endpoints, когда будут реализованы динамический placement, migration и
  restart recovery.

## 3. Безопасное включение

Под безопасным включением здесь понимается отсутствие влияния на обычный NBS2
при выключенном frontend, а не TLS или authentication.

- [x] **3.1. Значение по умолчанию.** `NbsFrontendConfig.Enabled` по умолчанию
  равен `false`.

  **Решение:** отсутствие секции `NbsFrontendConfig` также означает
  `Enabled = false`. Наличие `GrpcHost`, `GrpcPort`, `RdmaEnabled` или других
  полей само по себе frontend не включает. Новый `ydbd`, запущенный со старой
  конфигурацией, сохраняет прежнее поведение NBS2.

  **Обоснование:** интеграционный frontend открывает дополнительные listeners
  и меняет доступный извне data path, поэтому он всегда требует явного
  включения.

  **Статус:** целевое решение.

  **Детализация:** конкретный источник feature flag и способ его задания
  определяются в пункте 3.2. `RdmaEnabled` управляет RDMA только внутри уже
  включённого frontend и не заменяет общий `Enabled`.
- [x] **3.2. Feature flag.** Frontend включается только через
  `TNbsConfig.NbsFrontendConfig.Enabled = true`.

  **Решение:** в `TNbsConfig` добавляется отдельная вложенная конфигурация:

  ```protobuf
  message TNbsFrontendConfig {
      optional bool Enabled = 1 [default = false];
      // GrpcHost, GrpcPort, RdmaEnabled, RdmaHost, RdmaPort, ...
  }

  message TNbsConfig {
      optional bool Enabled = 1;
      optional TStorageServiceConfig NbsStorageConfig = 2;
      optional TNbsFrontendConfig NbsFrontendConfig = 3;
  }
  ```

  Флаг читается один раз при запуске `ydbd`; runtime-переключение без рестарта
  в MVP не поддерживается. Отдельные CLI flag, environment variable и новый
  service-mask flag не добавляются.

  Существующий `TNbsServiceInitializer` остаётся composition root и передаёт
  конфигурацию в `TNbsService`. `TNbsService` создаёт `TNbsFrontend` только при
  явном включении. `RdmaEnabled` управляет только RDMA внутри уже включённого
  frontend.

  **Обоснование:** существующий `TNbsConfig.Enabled` включает NBS2 в целом,
  включая внутренние компоненты, и не должен автоматически открывать classic
  gRPC/RDMA listeners на всех NBS2 nodes. Отдельный feature flag позволяет
  включать внешний frontend только на выбранных hosts.

  **Статус:** целевое решение.

  **Детализация:** комбинация `TNbsConfig.Enabled = false` и
  `NbsFrontendConfig.Enabled = true` является противоречивой конфигурацией;
  точное fail-fast правило определяется в пункте 3.5.
- [x] **3.3. Выключенное состояние.** При
  `NbsFrontendConfig.Enabled = false` объект `TNbsFrontend` и связанные с ним
  runtime-компоненты не создаются.

  **Решение:** в выключенном состоянии не создаются:

  - classic-compatible `IBlockStore` adapter и benchmark session adapter;
  - classic gRPC server, completion queues, workers и listener;
  - RDMA server, target, workers и listener независимо от остальных
    RDMA-полей;
  - frontend process-local partition registry.

  PartitionTablet не регистрируется во frontend registry. `TNbsService::Start()`
  и `TNbsService::Stop()` выполняют только существующий lifecycle NBS2.

  **Обоснование:** выключенный frontend означает отсутствие его runtime side
  effects, а не создание компонентов с последующим пропуском их `Start()`.

  **Статус:** целевое решение.

  **Детализация:** это поведение применяется после успешной валидации
  конфигурации. Обработка противоречивых комбинаций, например
  `NbsFrontendConfig.Enabled = false` вместе с `RdmaEnabled = true`,
  определяется в пункте 3.5.
- [x] **3.4. Совместимость запуска.** Отсутствующая или выключенная
  `NbsFrontendConfig` не меняет обычный запуск `ydbd` и существующий NBS2 data
  path.

  **Решение:** в выключенном состоянии:

  - существующие `TNbsService`, scheduler и vhost server создаются и
    запускаются в прежнем порядке;
  - lifecycle PartitionTablet и существующий NBS2 data path не меняются;
  - существующий YDB `TNbsGRpcService` не меняется;
  - не появляются обязательные параметры конфигурации, новые ports или
    дополнительные startup-проверки;
  - ошибки и порядок остановки обычного `ydbd` остаются прежними.

  Local auto-vhost при выключенном frontend продолжает работать согласно
  существующей NBS2-конфигурации.

  **Статус:** целевое требование обратной совместимости.

  **Временное допущение MVP:** в benchmark-конфигурации с включённой
  интеграцией local auto-vhost разрешается отключить глобально для всего NBS2
  host. Per-disk фильтрация только для управляемого NBS1 `fast_disk` в MVP не
  требуется. Глобальное отключение является явной настройкой benchmark
  deployment и не применяется вследствие одного лишь обновления binary.

  **Проверка:** конфигурации без `NbsFrontendConfig` и с
  `NbsFrontendConfig.Enabled = false` запускаются без frontend listeners и
  сохраняют существующий NBS2 path.
- [x] **3.5. Невалидная конфигурация.** Конфигурация валидируется до создания
  `TNbsFrontend` и любых связанных runtime-компонентов; валидируется только
  активная ветка конфигурации.

  **Решение:** если `NbsFrontendConfig.Enabled = false`, остальные
  frontend-поля не валидируются и игнорируются. В частности,
  `RdmaEnabled = true` ничего не запускает и не делает конфигурацию ошибочной:
  верхний feature flag имеет приоритет.

  Если `NbsFrontendConfig.Enabled = true`, обязательны:

  - `TNbsConfig.Enabled = true`;
  - непустой `GrpcHost`;
  - `GrpcPort` в диапазоне `1..65535`.

  Если дополнительно `RdmaEnabled = true`, обязательны непустой `RdmaHost` и
  `RdmaPort` в диапазоне `1..65535`. При `RdmaEnabled = false` значения
  `RdmaHost` и `RdmaPort` игнорируются. Неявные classic defaults
  `localhost:9766` и `localhost:10088` не используются.

  Статическая NBS1-side `DataRoute` валидируется отдельно. Для неё обязательны:

  - непустые `DiskId`, `OwnerCellId` и `Nbs2Fqdn`;
  - поддерживаемый `Transport`;
  - `GrpcPort` в диапазоне `1..65535`;
  - `RdmaPort` в диапазоне `1..65535`, если выбран RDMA.

  Любое нарушение активной конфигурации завершает запуск соответствующего
  процесса до создания listeners. Диагностическая ошибка содержит полный путь
  поля, отклонённое значение и причину, например:

  ```text
  NbsFrontendConfig.RdmaPort: expected 1..65535 when RdmaEnabled=true, got 0
  ```

  **Обоснование:** верхний feature flag позволяет безопасно заранее доставить
  заполненную, но ещё не активную конфигурацию. После включения невалидные или
  неполные transport-настройки не должны молча заменяться defaults или
  приводить к частично работающему frontend.

  **Статус:** целевое решение.

  **Детализация:** конфликт корректно заданного port или невозможность bind
  определяются во время запуска по правилу 2.4. Межпроцессное соответствие
  `DataRoute` реально запущенным listeners определяется в пункте 3.6.
- [x] **3.6. Согласованность с DataRoute.** Статическая `DataRoute` публикуется
  сразу и в MVP не является сигналом live-readiness NBS2 frontend.

  **Решение:** отдельные `StaticDataRoute.Enabled`, rollout scripts, preflight
  probes и двухфазное включение не добавляются. NBS1 в cell-владельце сразу
  возвращает настроенную статическую route для известного `DiskId`.

  Доступность проверяется штатным путём создания endpoint:

  - устанавливается gRPC connection и выполняется `MountVolume`;
  - при выбранном RDMA transport в `StartEndpoint` входит создание штатного
    cells RDMA endpoint;
  - ошибка любого обязательного шага прерывает `StartEndpoint`, и
    пользовательский endpoint не считается созданным.

  Неуспешный `StartEndpoint` удаляет частично созданный endpoint и session,
  чтобы весь запрос можно было повторить после готовности NBS2. Ошибка
  возвращается как transport/retriable error без legacy NBS1 storage fallback
  и без автоматического retry write.

  **Обоснование:** статическая route для одного диска и закреплённого placement
  является конфигурацией адреса, а не механизмом health discovery. Возможная
  startup race влияет только на доступность первой попытки `StartEndpoint` и не
  меняет выбранный backend или корректность I/O.

  **Статус:** временное допущение MVP.

  **Ограничения MVP:** ошибка в `Nbs2Fqdn` или ports обнаруживается при
  подключении. После остановки NBS2 route автоматически не отзывается: новые
  `StartEndpoint` и текущий I/O получают transport errors, а восстановление
  выполняется вручную. Route на другой host отклоняется process-local registry
  как неизвестный локальный `DiskId`.

  **Условие замены:** production resolver должен учитывать readiness и
  host/incarnation либо route generation и автоматически переставать
  публиковать устаревшую route.

## 4. Ожидаемый результат

Зафиксирована следующая runtime-схема MVP:

```text
NBS1_1, cell1
    -> NBS1_2, cell2: DescribeVolume
    -> NBS1_2, cell2: GetDataRoute(DiskId)
                            |
                            v
                    static route lookup
                            |
                            v
NBS2 host/ydbd
└── TNbsService
    └── TNbsFrontend, только при NbsFrontendConfig.Enabled=true
        ├── classic-compatible IBlockStore facade
        ├── отдельный classic NBS gRPC server
        ├── classic NBS RDMA target, только при RdmaEnabled=true
        └── process-local partition registry
                ^
                | register/unregister
            PartitionTablet
                └── FastPathService/IStoragePtr
```

`GetDataRoute` является способом получения route, а `static` описывает источник
её содержимого. В MVP операция выполняет `DiskId` lookup в статической
конфигурации NBS1 cell-владельца; Hive lookup, route generation и автоматическое
обновление при migration/restart отсутствуют.

Classic-compatible `IBlockStore` facade является общей transport-neutral
границей для gRPC server и RDMA target. Он реализует нужное MVP-подмножество
classic service, выполняет session validation, находит локальную partition по
`DiskId`, преобразует запрос в вызов `IStoragePtr` и отображает результат или
ошибку обратно в classic response. Facade не реализует wire protocol, не
владеет `FastPathService`, не выполняет discovery и не пересылает запросы между
hosts.

Зафиксирован минимальный набор NBS2 frontend configuration:

- [x] `NbsFrontendConfig.Enabled`;
- [x] `GrpcHost` и `GrpcPort`;
- [x] `RdmaEnabled`, `RdmaHost` и `RdmaPort`.

Зафиксирован минимальный набор static NBS1-side `DataRoute` configuration:

- [x] `DiskId` и `OwnerCellId`;
- [x] `Nbs2Fqdn` и `GrpcPort`;
- [x] опциональный `RdmaPort`;
- [x] `Transport`.

Отдельные `DiskId` и `TabletId` в `NbsFrontendConfig` не добавляются. Привязка
frontend к локальной partition выполняется runtime-регистрацией:

```text
DiskId, TabletId, TabletGeneration, IStoragePtr
```

При `NbsFrontendConfig.Enabled = false` PartitionTablet сохраняет существующее
поведение local auto-vhost. При `NbsFrontendConfig.Enabled = true` local
auto-vhost глобально не создаётся для PartitionTablet на этом NBS2 host;
partition вместо этого регистрируется во frontend registry. Это временное
допущение MVP, не требующее отдельного `DisableLocalAutoVhost` и per-disk
фильтрации.

Итоговый checklist конфигурации:

- [x] отдельный feature flag frontend;
- [x] отдельный classic gRPC listener и его bind address/port;
- [x] опциональный RDMA target и его bind address/port;
- [x] статически сконфигурированная `DataRoute`, возвращаемая через
  `GetDataRoute`;
- [x] process-local привязка frontend к PartitionTablet без дополнительных
  config fields;
- [x] fail-fast правила валидации активной конфигурации.

## 5. Текущий пункт

Checklist шага 0 закрыт; текущего несогласованного пункта в этом файле нет.
