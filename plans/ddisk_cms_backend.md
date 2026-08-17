# Backend: синхронизация DDisk/PersistentBuffer групп BSC → CMS

## Scope

Реализовать только backend. CMS должна персистентно хранить последнее полное состояние DDisk/PersistentBuffer групп и показывать его после рестарта CMS или недоступности BSC.

API намеренно простой:

1. CMS одним запросом получает список всех таблеток и их текущих revision.
2. CMS сравнивает revision с локально сохранёнными значениями.
3. Для каждой таблетки с отличающейся revision CMS отдельным запросом получает полный state этой таблетки: все группы, DDisk и PersistentBuffer.
4. BSC после изменения таблетки отправляет CMS только уведомление с `tablet_id` и новой revision. CMS после уведомления повторно запрашивает полный state этой таблетки.

Считается, что список таблеток с revision помещается в один ответ, а полный state одной таблетки — в один ответ. Не вводятся `bsc_epoch`, page token, group cursor, chunk protocol, ACK и delta journal.

UI в эту фазу не входит. Backend read API должен сохранить данные для последующего отображения всех таблеток, групп, DDisk и PersistentBuffer, включая устройства на недоступных нодах.

## Существующие точки интеграции

- BSC хранит allocation в [`Schema::DirectBlockGroupClaims`](../ydb/core/mind/bscontroller/scheme.h:480): `(TabletId, DirectBlockGroupId, NumVChunksClaimed, Allocation)`.
- Изменения allocation выполняются в [`TTxAllocateDDiskBlockGroup::Execute()`](../ydb/core/mind/bscontroller/ddisk.cpp:347).
- BSC завершает загрузку в [`TBlobStorageController::LoadFinished()`](../ydb/core/mind/bscontroller/impl.h:2312).
- CMS загружает локальное состояние в [`TCms::TTxLoadState`](../ydb/core/cms/cms_tx_load_state.cpp:25), а схему материализует через [`TCms::TTxInitScheme`](../ydb/core/cms/cms_tx_init_scheme.cpp:8).
- Tablet-pipe к BSC уже используется в [`TInfoCollector::RequestBaseConfig()`](../ydb/core/cms/info_collector.cpp:276).
- Существующие BSC protobuf events находятся в [`blobstorage.proto`](../ydb/core/protos/blobstorage.proto:1565), wrappers — в [`blobstorage_events.h`](../ydb/core/blobstorage/base/blobstorage_events.h:537).

## Revision по таблетке

Добавить в BSC таблицу состояния таблеток, например `DirectBlockGroupTabletState`:

- `TabletId` — primary key;
- `Revision` — `uint64`, монотонно увеличивается при каждом изменении конфигурации DBG этой таблетки;
- `LastChangedAt` — диагностическое время.

Правила:

- изменение нескольких групп одной таблетки в одной BSC-транзакции увеличивает revision один раз;
- изменение нескольких таблеток увеличивает revision каждой затронутой таблетки отдельно;
- удаление последней группы не удаляет запись состояния таблетки: revision продолжает существовать и сообщает CMS, что локальные группы надо удалить;
- revision изменяется в одной транзакции вместе с изменением [`DirectBlockGroupClaims`](../ydb/core/mind/bscontroller/scheme.h:480).

Отдельный BSC epoch не нужен. Revision является единственным идентификатором актуальности состояния таблетки.

## API BSC ↔ CMS

Добавить отдельные protobuf messages и actor events, не переиспользуя allocation request [`TEvControllerAllocateDDiskBlockGroup`](../ydb/core/protos/blobstorage.proto:1565).

### Получение списка таблеток и revision

`TEvControllerDDiskInfoListTabletsRequest` — пустой request либо содержит только `protocol_version`.

`TEvControllerDDiskInfoListTabletsResponse`:

- `status`;
- `error_reason`;
- `repeated TTabletRevision tablets`;
- `TTabletRevision` содержит `tablet_id`, `revision`, `groups_count` и `last_changed_at`.

Ответ содержит полный список таблеток в стабильном порядке по `tablet_id`. Если BSC не может сформировать список, CMS сохраняет прежнее состояние и повторяет запрос позже.

При старте CMS:

1. загружает active state из своей БД;
2. создаёт tablet pipe к BSC;
3. запрашивает `ListTablets`;
4. сравнивает ответ с локальными tablet revisions;
5. для каждой таблетки с отличающейся revision вызывает `GetTabletState`;
6. для локальных таблеток, отсутствующих в полном списке BSC, удаляет локальные данные отдельной транзакцией после успешного завершения list scan.

Если revision равна локальной, полный state таблетки не запрашивается.

### Получение полного state одной таблетки

`TEvControllerDDiskInfoGetTabletRequest`:

- `tablet_id`;
- `known_revision` — локальная revision CMS для диагностики и проверки ответа.

`TEvControllerDDiskInfoGetTabletResponse`:

- `status`;
- `error_reason`;
- `tablet_id`;
- `revision` — revision, к которой относится весь ответ;
- `repeated TDirectBlockGroup group`.

Один ответ содержит полный state одной таблетки, включая пустой state при отсутствии групп. Каждый group содержит:

- `tablet_id`;
- `direct_block_group_id`;
- `num_vchunks_claimed`;
- ordered список DDisk entries;
- ordered список PersistentBuffer entries.

DDisk entry содержит исходный serialized `TDDiskId`, а также извлечённые `node_id`, `pdisk_id` и идентификатор диска, если они доступны из типа. Пустые DDisk slots сохраняются как entries без активного DDisk либо через явный `has_ddisk=false`, чтобы не потерять индексы и holes.

Запрос всегда возвращает полную конфигурацию таблетки, а не field-level delta. CMS заменяет локальный state таблетки целиком только после успешного завершения транзакции.

### Уведомление об изменении

`TEvControllerDDiskInfoTabletRevisionChanged`:

- `tablet_id`;
- `revision`.

BSC отправляет notification после commit транзакции, изменившей allocation и revision. В уведомлении нет групп и DDisk данных. CMS:

- сравнивает notification revision с локальной;
- если notification revision больше локальной, ставит tablet в очередь синхронизации;
- запрашивает полный state таблетки;
- игнорирует notification с revision не больше локальной.

Notification является ускорителем, а не источником гарантированной доставки. CMS периодически выполняет `ListTablets`, поэтому потерянное уведомление будет обнаружено при следующей сверке.

## Семантика согласованности

Главный риск упрощённого API — изменение таблетки между `ListTablets` и `GetTabletState`. Он закрывается проверкой revision:

1. BSC при начале `GetTabletState` читает текущую revision таблетки.
2. Весь ответ строится из данных, относящихся к этой revision.
3. BSC в response возвращает эту revision.
4. CMS применяет ответ атомарно и сохраняет именно response revision.
5. После применения CMS повторно сравнивает revision через notification/следующий list scan.

Если реализация чтения не может гарантировать согласованный snapshot при concurrent allocation, BSC должен повторить внутреннее чтение до получения стабильной revision. Нельзя отдавать CMS набор групп, собранный из разных revision.

Допустимая eventual consistency:

- между изменением в BSC и запросом CMS может отображаться прежнее состояние;
- при недоступности BSC CMS показывает последнее полное состояние;
- частичный ответ не заменяет active state;
- если ответ завершился ошибкой, локальная tablet revision не изменяется.

Повторная доставка безопасна: CMS сравнивает response revision с локальной и делает no-op, если локальная revision уже не меньше response revision.

## BSC implementation plan

1. Добавить protobuf messages для `ListTabletsRequest/Response`, `GetTabletStateRequest/Response`, `TTabletRevision`, `TDirectBlockGroup` и `TabletRevisionChanged`.
2. Добавить event wrappers и event ids в blobstorage event declarations.
3. Добавить таблицу `DirectBlockGroupTabletState` в BSC schema и materialize её в существующей схеме BSC.
4. В [`TTxAllocateDDiskBlockGroup`](../ydb/core/mind/bscontroller/ddisk.cpp:15) определить затронутые tablet ids. В той же transaction:
   - применить изменения `DirectBlockGroupClaims`;
   - увеличить revision каждой затронутой таблетки ровно один раз;
   - сохранить revision.
5. В `Complete()` отправить CMS notification для каждой изменившейся таблетки. Notification не отправлять, если transaction завершилась ошибкой или не было изменений.
6. Реализовать `ListTablets` transaction: прочитать все tablet state rows и вернуть один bounded-by-global-limit response.
7. Реализовать `GetTabletState` transaction: прочитать все `DirectBlockGroupClaims` по `TabletId`, распарсить `TDirectBlockGroupAllocation`, собрать полный response и вернуть актуальную revision.
8. На чтении проверить, что revision не изменилась во время формирования ответа. При изменении повторить чтение либо вернуть retryable error.
9. Добавить subscriber lifecycle: CMS подключается к BSC через tablet pipe, BSC запоминает активный CMS sender/pipe и обрабатывает disconnect. Потерянные notifications компенсируются периодическим list scan CMS.
10. Добавить серверный лимит размера одного ответа и явную ошибку `RESPONSE_TOO_LARGE`. Так как по требованиям одна таблетка должна помещаться в один response, превышение лимита является ошибкой конфигурации/ограничения, а не поводом вводить новый протокол страниц.
11. Добавить counters: число list requests, get requests, notifications, response bytes, response-too-large, revision retries и active subscribers.

## CMS persistence and lifecycle

Расширить [`Schema`](../ydb/core/cms/scheme.h:10):

### `DDiskInfoMeta`

Singleton row с:

- последним успешным list scan timestamp;
- scan status;
- последней ошибкой;
- признаком, что полный tablet list был успешно обработан.

### `DDiskInfoTablets`

Primary key `TabletId`, columns:

- `TabletId`;
- `Revision`;
- `GroupsCount`;
- `SyncState`;
- `LastSyncTime`;
- `LastError`.

### `DDiskInfoGroups`

Primary key `(TabletId, DirectBlockGroupId)`, columns:

- `TabletId`;
- `DirectBlockGroupId`;
- `Revision`;
- complete serialized group descriptor;
- optional derived counts for efficient reads.

Не хранить весь cluster state одним большим protobuf blob. Раздельные rows позволяют заменить одну tablet атомарно и читать backend API по tablet/group.

### CMS state

Добавить в [`TCmsState`](../ydb/core/cms/cms_state.h:37) active tablet revisions, sync states, sync queue, BSC pipe id и timer state. В [`TCms`](../ydb/core/cms/cms_impl.h:36) добавить private events/handlers для:

- list response;
- get tablet response;
- revision notification;
- pipe connected/destroyed;
- retry and periodic reconciliation.

### Startup

1. [`TTxInitScheme`](../ydb/core/cms/cms_tx_init_scheme.cpp:8) materializes new tables.
2. [`TTxLoadState`](../ydb/core/cms/cms_tx_load_state.cpp:25) loads active tablet revisions and group descriptors.
3. CMS becomes active with loaded state even if BSC is unavailable.
4. CMS establishes/reestablishes BSC pipe and sends `ListTablets`.
5. Only stale tablets are synchronized with `GetTabletState`.

### Atomic tablet replacement

Для response `GetTabletState` CMS выполняет одну transaction:

1. проверяет `tablet_id`, response `revision` и формат всех групп;
2. удаляет прежние `DDiskInfoGroups` этой таблетки;
3. записывает полный новый набор групп;
4. записывает новую revision и groups count в `DDiskInfoTablets`;
5. фиксирует transaction.

До commit active state не меняется. После commit CMS может считать tablet синхронизированной. Если BSC ответил пустым state с новой revision, transaction удаляет все прежние группы и сохраняет tablet row с новой revision.

При рестарте CMS или отказе BSC:

- последние полные `DDiskInfoGroups` остаются в БД;
- незавершённая transaction не оставляет частичную замену;
- CMS повторяет `GetTabletState` для tablet, если revision ещё stale;
- отсутствие BSC не очищает rows и не переводит CMS в неактивное состояние.

## Backend read API для будущего UI

UI реализуется отдельно, но backend должен предоставить read-only JSON endpoint через существующий JSON proxy pattern:

- `state` — list sync status, последняя успешная сверка и ошибка;
- `tablets` — tablet id, local revision, group count, sync state;
- `groups?tablet_id=...` — полные group descriptors с DDisk/PersistentBuffer identity;
- `devices` — deduplicated DDisk/PersistentBuffer inventory по сохранённым группам.

Все read методы должны поддерживать server-side filtering/pagination для UI, даже если BSC sync response по требованиям непагинированный.

Allocation truth и availability разделяются:

- allocation берётся из сохранённого BSC state;
- node/PDisk availability вычисляется при чтении через текущий CMS cluster info;
- неизвестное состояние обозначается `unknown`, а не `available`;
- group summary должен возвращать `unavailable_ddisk_count`, `unavailable_persistent_buffer_count` и severity, чтобы UI мог сортировать наиболее критичные группы.

Недоступные ноды и устройства не удаляются из inventory: они остаются в сохранённом allocation и только получают статус `unavailable`/`unknown`.

## Тесты

1. Protocol tests: serialization, complete tablet response, empty tablet, holes, malformed DDisk id, revision fields и response-too-large.
2. BSC tests: list all tablets; revision increment once for several changed groups in one transaction; revision increment for each changed tablet; deletion of last group; full tablet read; no notification before commit; notification only on actual changes.
3. Concurrent mutation test: mutate tablet during `GetTabletState` and verify stable response revision or retryable error, never mixed revisions.
4. CMS tablet tests: schema migration, load persisted state, compare list revisions, request only stale tablets, atomic full replacement, empty tablet replacement, duplicate old response, BSC outage, reconnect and periodic reconciliation.
5. Integration test: create many tablets and many groups, verify one list response, modify one tablet, verify only it is fetched, restart CMS, stop BSC, and verify last state remains available.
6. HTTP tests: list tablets/groups/devices, unavailable and unknown node states, stable revisions and criticality fields.
7. Add focused build/test entries to [`ydb/core/cms/ya.make`](../ydb/core/cms/ya.make) and relevant BSC test makefiles.

## Acceptance criteria

- `ListTablets` возвращает полный список tablet ids и current revisions одним ответом.
- CMS при startup не запрашивает неизменившиеся таблетки.
- `GetTabletState` возвращает полную раскладку одной таблетки одним ответом.
- Revision увеличивается при каждой фактической смене конфигурации DBG и сохраняется атомарно с allocation.
- BSC уведомляет CMS только после успешного commit, а CMS после notification запрашивает полный state таблетки.
- Потеря notification не приводит к потере согласованности благодаря периодическому `ListTablets`.
- Рестарт CMS или недоступность BSC сохраняет последнее полное состояние.
- Частичный/ошибочный ответ не заменяет active state.
- Backend сохраняет все DDisk/PersistentBuffer, включая устройства на недоступных нодах, и отдаёт данные для будущей сортировки по критичности.
