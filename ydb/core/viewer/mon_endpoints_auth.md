# Monitoring HTTP endpoints

Документ сгенерирован скриптом `generate_mon_endpoints_doc.py` из канонических данных `test_mon_endpoints_auth` (режим `enforce_user_token_enabled`).

Источник: `ydb/tests/functional/security/canondata/test_mon_endpoints_auth.test_mon_endpoints_auth-enforce_user_token_enabled/mon_endpoints_auth-enforce_user_token_enabled.json`

## Иерархия доступа

Тестовая конфигурация использует встроенные SID: `database@builtin` в `database_allowed_sids`, `viewer@builtin` в `viewer_allowed_sids`, `monitoring@builtin` в `monitoring_allowed_sids`, `root@builtin` в `administration_allowed_sids`.

Доступ наследуется сверху вниз: каждый следующий уровень включает права всех уровней ниже.

```text
administration_allowed_sids
  -> monitoring_allowed_sids
     -> viewer_allowed_sids
        -> database_allowed_sids
```

## Аудит логгирование

Аудит сейчас определяется denylist из `core/mon/audit/audit_denylist.cpp`:

- `POST`, `PUT`, `DELETE` логируются всегда.
- `OPTIONS` не логируется.
- Остальные методы логируются, если URL не попал в denylist.
- Текущий denylist: `/`, `/internal`, `/ver`, `/counters*`, `/viewer*`, `/vdisk*`, `/pdisk*`, `/monitoring*`, `/healthcheck*`, `/operation*`, `/query*`, `/scheme*`, `/storage*`, `/static*`, `/jquery.tablesorter.js`, `/jquery.tablesorter.css`, `/lwtrace/mon/static*`.

## Как скрипт распределяет endpoints по уровням прав

Скрипт читает canon `test_mon_endpoints_auth`: для каждого `method + path + query` там есть HTTP-статусы для запросов без токена и с токенами `user@builtin`, `database@builtin`, `viewer@builtin`, `monitoring@builtin`, `root@builtin`.

Для каждого `method + path` по умолчанию выбирается один query-вариант: вариант с минимальным уровнем прав; при равенстве предпочитается пустой query, затем лексикографический порядок. Опция `--all-queries` выводит все query-варианты.

Уровень прав endpoint определяется минимальным доступом, с которым запрос проходит access check:

- Для уровней прав anonymus/public/database статус `2xx` или `400` означает, что запрос дошёл до handler; `400` здесь считается ошибкой валидации запроса после пройденного access check.
- Для `viewer_allowed_sids`, `monitoring_allowed_sids` и `administration_allowed_sids` успешным доступом считается `2xx`. Если `2xx` нет, уровень прав берётся из политики handler или из регистрации viewer endpoint.
- Endpoints, для которых нет успешного доступа и не найдена политика handler, в документ не выводятся.

## Anonymus (no token)

| Endpoint | Метод | Аудит лог |
| --- | --- | --- |
| `/actors/tablet_counters_aggregator` | GET | логируется |
| `/actors/tablet_counters_aggregator` | POST | логируется |
| `/counters` | GET | не логируется |
| `/counters` | POST | логируется |
| `/counters/hosts` | GET | не логируется |
| `/counters/hosts` | POST | логируется |
| `/followercounters` | GET | логируется |
| `/followercounters` | POST | логируется |
| `/healthcheck?database=/Root` | GET | не логируется |
| `/healthcheck?database=/Root` | POST | логируется |
| `/labeledcounters` | GET | логируется |
| `/labeledcounters` | POST | логируется |
| `/login` | GET | логируется |
| `/login` | POST | логируется |
| `/monitoring/` | GET | не логируется |
| `/monitoring/` | POST | логируется |
| `/node/1/monitoring` | GET | логируется |
| `/node/1/monitoring` | POST | логируется |
| `/ping` | GET | логируется |
| `/ping` | POST | логируется |
| `/status` | GET | логируется |
| `/status` | POST | логируется |
| `/viewer/capabilities` | GET | не логируется |
| `/viewer/capabilities` | POST | логируется |

## Public

_Нет эндпойнтов._

## Database

| Endpoint | Метод | Аудит лог |
| --- | --- | --- |
| `/operation/cancel` | GET | не логируется |
| `/operation/cancel` | POST | логируется |
| `/operation/forget` | GET | не логируется |
| `/operation/forget` | POST | логируется |
| `/operation/get` | GET | не логируется |
| `/operation/get` | POST | логируется |
| `/operation/list` | GET | не логируется |
| `/operation/list` | POST | логируется |
| `/query/script/execute` | GET | не логируется |
| `/query/script/execute` | POST | логируется |
| `/query/script/fetch` | GET | не логируется |
| `/query/script/fetch` | POST | логируется |
| `/scheme/directory` | DELETE | логируется |
| `/scheme/directory` | GET | не логируется |
| `/scheme/directory` | POST | логируется |
| `/storage/groups` | GET | не логируется |
| `/storage/groups` | POST | логируется |
| `/viewer` | GET | не логируется |
| `/viewer` | POST | логируется |
| `/viewer/acl?database=/Root` | GET | не логируется |
| `/viewer/acl?database=/Root` | POST | логируется |
| `/viewer/autocomplete` | GET | не логируется |
| `/viewer/autocomplete` | POST | логируется |
| `/viewer/browse` | GET | не логируется |
| `/viewer/browse` | POST | логируется |
| `/viewer/check_access?database=/Root` | GET | не логируется |
| `/viewer/check_access?database=/Root` | POST | логируется |
| `/viewer/commit_offset` | GET | не логируется |
| `/viewer/commit_offset` | POST | логируется |
| `/viewer/content` | GET | не логируется |
| `/viewer/content` | POST | логируется |
| `/viewer/database_stats?database=/Root` | GET | не логируется |
| `/viewer/database_stats?database=/Root` | POST | логируется |
| `/viewer/describe?database=/Root` | GET | не логируется |
| `/viewer/describe?database=/Root` | POST | логируется |
| `/viewer/describe_consumer` | GET | не логируется |
| `/viewer/describe_consumer` | POST | логируется |
| `/viewer/describe_replication` | GET | не логируется |
| `/viewer/describe_replication` | POST | логируется |
| `/viewer/describe_topic` | GET | не логируется |
| `/viewer/describe_topic` | POST | логируется |
| `/viewer/describe_transfer` | GET | не логируется |
| `/viewer/describe_transfer` | POST | логируется |
| `/viewer/hotkeys?database=/Root` | GET | не логируется |
| `/viewer/hotkeys?database=/Root` | POST | логируется |
| `/viewer/labeledcounters` | GET | не логируется |
| `/viewer/labeledcounters` | POST | логируется |
| `/viewer/metainfo` | GET | не логируется |
| `/viewer/metainfo` | POST | логируется |
| `/viewer/nodelist` | GET | не логируется |
| `/viewer/nodelist` | POST | логируется |
| `/viewer/nodes` | GET | не логируется |
| `/viewer/nodes` | POST | логируется |
| `/viewer/plan2svg` | GET | не логируется |
| `/viewer/plan2svg` | POST | логируется |
| `/viewer/put_record` | GET | не логируется |
| `/viewer/put_record` | POST | логируется |
| `/viewer/query?database=/Root` | GET | не логируется |
| `/viewer/query?database=/Root` | POST | логируется |
| `/viewer/storage_stats?database=/Root` | GET | не логируется |
| `/viewer/storage_stats?database=/Root` | POST | логируется |
| `/viewer/tabletcounters` | GET | не логируется |
| `/viewer/tabletcounters` | POST | логируется |
| `/viewer/tabletinfo` | GET | не логируется |
| `/viewer/tabletinfo` | POST | логируется |
| `/viewer/tenantinfo?database=/Root` | GET | не логируется |
| `/viewer/tenantinfo?database=/Root` | POST | логируется |
| `/viewer/v2/json/tabletinfo` | GET | не логируется |
| `/viewer/v2/json/tabletinfo` | POST | логируется |
| `/viewer/whoami?database=/Root` | GET | не логируется |
| `/viewer/whoami?database=/Root` | POST | логируется |

## Viewer

| Endpoint | Метод | Аудит лог |
| --- | --- | --- |
| `/pdisk/info` | GET | не логируется |
| `/pdisk/info` | POST | логируется |
| `/vdisk/blobindexstat` | GET | не логируется |
| `/vdisk/blobindexstat` | POST | логируется |
| `/vdisk/getblob` | GET | не логируется |
| `/vdisk/getblob` | POST | логируется |
| `/vdisk/vdiskstat` | GET | не логируется |
| `/vdisk/vdiskstat` | POST | логируется |
| `/viewer/bsgroupinfo` | GET | не логируется |
| `/viewer/bsgroupinfo` | POST | логируется |
| `/viewer/cluster` | GET | не логируется |
| `/viewer/cluster` | POST | логируется |
| `/viewer/compute` | GET | не логируется |
| `/viewer/compute` | POST | логируется |
| `/viewer/config` | GET | не логируется |
| `/viewer/config` | POST | логируется |
| `/viewer/counters` | GET | не логируется |
| `/viewer/counters` | POST | логируется |
| `/viewer/feature_flags` | GET | не логируется |
| `/viewer/feature_flags` | POST | логируется |
| `/viewer/graph` | GET | не логируется |
| `/viewer/graph` | POST | логируется |
| `/viewer/groups` | GET | не логируется |
| `/viewer/groups` | POST | логируется |
| `/viewer/hiveinfo` | GET | не логируется |
| `/viewer/hiveinfo` | POST | логируется |
| `/viewer/hivestats` | GET | не логируется |
| `/viewer/hivestats` | POST | логируется |
| `/viewer/multipart_counter` | GET | не логируется |
| `/viewer/multipart_counter` | POST | логируется |
| `/viewer/netinfo` | GET | не логируется |
| `/viewer/netinfo` | POST | логируется |
| `/viewer/nodeinfo` | GET | не логируется |
| `/viewer/nodeinfo` | POST | логируется |
| `/viewer/pdiskinfo` | GET | не логируется |
| `/viewer/pdiskinfo` | POST | логируется |
| `/viewer/peers` | GET | не логируется |
| `/viewer/peers` | POST | логируется |
| `/viewer/pqconsumerinfo` | GET | не логируется |
| `/viewer/pqconsumerinfo` | POST | логируется |
| `/viewer/render` | GET | не логируется |
| `/viewer/render` | POST | логируется |
| `/viewer/simple_counter` | GET | не логируется |
| `/viewer/simple_counter` | POST | логируется |
| `/viewer/sse_counter` | GET | не логируется |
| `/viewer/sse_counter` | POST | логируется |
| `/viewer/storage` | GET | не логируется |
| `/viewer/storage` | POST | логируется |
| `/viewer/storage_usage` | GET | не логируется |
| `/viewer/storage_usage` | POST | логируется |
| `/viewer/sysinfo` | GET | не логируется |
| `/viewer/sysinfo` | POST | логируется |
| `/viewer/tenants` | GET | не логируется |
| `/viewer/tenants` | POST | логируется |
| `/viewer/topicinfo` | GET | не логируется |
| `/viewer/topicinfo` | POST | логируется |
| `/viewer/v2/json/nodeinfo` | GET | не логируется |
| `/viewer/v2/json/nodeinfo` | POST | логируется |
| `/viewer/v2/json/pdiskinfo` | GET | не логируется |
| `/viewer/v2/json/pdiskinfo` | POST | логируется |
| `/viewer/v2/json/sysinfo` | GET | не логируется |
| `/viewer/v2/json/sysinfo` | POST | логируется |
| `/viewer/v2/json/vdiskinfo` | GET | не логируется |
| `/viewer/v2/json/vdiskinfo` | POST | логируется |
| `/viewer/vdiskinfo` | GET | не логируется |
| `/viewer/vdiskinfo` | POST | логируется |

## Monitoring

| Endpoint | Метод | Аудит лог |
| --- | --- | --- |
| `/actors/` | GET | логируется |
| `/actors/` | POST | логируется |
| `/actors/blobstorageproxies` | GET | логируется |
| `/actors/blobstorageproxies` | POST | логируется |
| `/actors/configs_dispatcher` | GET | логируется |
| `/actors/configs_dispatcher` | POST | логируется |
| `/actors/console_configs_provider` | GET | логируется |
| `/actors/console_configs_provider` | POST | логируется |
| `/actors/dnameserver` | GET | логируется |
| `/actors/dnameserver` | POST | логируется |
| `/actors/dsproxynode` | GET | логируется |
| `/actors/dsproxynode` | POST | логируется |
| `/actors/feature_flags` | GET | логируется |
| `/actors/feature_flags` | POST | логируется |
| `/actors/icb` | GET | логируется |
| `/actors/icb` | POST | логируется |
| `/actors/interconnect` | GET | логируется |
| `/actors/interconnect` | POST | логируется |
| `/actors/kqp_node` | GET | логируется |
| `/actors/kqp_node` | POST | логируется |
| `/actors/kqp_proxy` | GET | логируется |
| `/actors/kqp_proxy` | POST | логируется |
| `/actors/kqp_resource_manager` | GET | логируется |
| `/actors/kqp_resource_manager` | POST | логируется |
| `/actors/kqp_spilling_file` | GET | логируется |
| `/actors/kqp_spilling_file` | POST | логируется |
| `/actors/logger` | GET | логируется |
| `/actors/logger` | POST | логируется |
| `/actors/memory_tracker` | GET | логируется |
| `/actors/memory_tracker` | POST | логируется |
| `/actors/netclassifier` | GET | логируется |
| `/actors/netclassifier` | POST | логируется |
| `/actors/nodewarden` | GET | логируется |
| `/actors/nodewarden` | POST | логируется |
| `/actors/pdisks` | GET | логируется |
| `/actors/pdisks` | POST | логируется |
| `/actors/pql2` | GET | логируется |
| `/actors/pql2` | POST | логируется |
| `/actors/quoter_proxy` | GET | логируется |
| `/actors/quoter_proxy` | POST | логируется |
| `/actors/rb` | GET | логируется |
| `/actors/rb` | POST | логируется |
| `/actors/statservice` | GET | логируется |
| `/actors/statservice` | POST | логируется |
| `/actors/tenant_pool` | GET | логируется |
| `/actors/tenant_pool` | POST | логируется |
| `/actors/vdisks` | GET | логируется |
| `/actors/vdisks` | POST | логируется |
| `/cms` | GET | логируется |
| `/cms` | POST | логируется |
| `/grpc` | GET | логируется |
| `/grpc` | POST | логируется |
| `/internal` | GET | не логируется |
| `/internal` | POST | логируется |
| `/jquery.tablesorter.css` | GET | не логируется |
| `/jquery.tablesorter.css` | POST | логируется |
| `/jquery.tablesorter.js` | GET | не логируется |
| `/jquery.tablesorter.js` | POST | логируется |
| `/memory/fragmentation` | GET | логируется |
| `/memory/fragmentation` | POST | логируется |
| `/memory/heap` | GET | логируется |
| `/memory/heap` | POST | логируется |
| `/memory/peakheap` | GET | логируется |
| `/memory/peakheap` | POST | логируется |
| `/memory/statistics` | GET | логируется |
| `/memory/statistics` | POST | логируется |
| `/nodetabmon` | GET | логируется |
| `/nodetabmon` | POST | логируется |
| `/pdisk/restart` | GET | не логируется |
| `/pdisk/restart` | POST | логируется |
| `/pdisk/status` | GET | не логируется |
| `/pdisk/status` | POST | логируется |
| `/static/css/bootstrap.min.css` | GET | не логируется |
| `/static/css/bootstrap.min.css` | POST | логируется |
| `/static/fonts/glyphicons-halflings-regular.eot` | GET | не логируется |
| `/static/fonts/glyphicons-halflings-regular.eot` | POST | логируется |
| `/static/fonts/glyphicons-halflings-regular.svg` | GET | не логируется |
| `/static/fonts/glyphicons-halflings-regular.svg` | POST | логируется |
| `/static/fonts/glyphicons-halflings-regular.ttf` | GET | не логируется |
| `/static/fonts/glyphicons-halflings-regular.ttf` | POST | логируется |
| `/static/fonts/glyphicons-halflings-regular.woff` | GET | не логируется |
| `/static/fonts/glyphicons-halflings-regular.woff` | POST | логируется |
| `/static/js/bootstrap.min.js` | GET | не логируется |
| `/static/js/bootstrap.min.js` | POST | логируется |
| `/static/js/jquery.min.js` | GET | не логируется |
| `/static/js/jquery.min.js` | POST | логируется |
| `/tablet` | GET | логируется |
| `/tablet` | POST | логируется |
| `/tablets` | GET | логируется |
| `/tablets` | POST | логируется |
| `/trace` | GET | логируется |
| `/trace` | POST | логируется |
| `/vdisk/evict` | GET | не логируется |
| `/vdisk/evict` | POST | логируется |
| `/ver` | GET | не логируется |
| `/ver` | POST | логируется |
| `/viewer/healthcheck` | GET | не логируется |
| `/viewer/healthcheck` | POST | логируется |
| `/viewer/v2` | GET | не логируется |
| `/viewer/v2` | POST | логируется |
| `/viewer/v2/json/config` | GET | не логируется |
| `/viewer/v2/json/config` | POST | логируется |
| `/viewer/v2/json/nodelist` | GET | не логируется |
| `/viewer/v2/json/nodelist` | POST | логируется |
| `/viewer/v2/json/storage` | GET | не логируется |
| `/viewer/v2/json/storage` | POST | логируется |

## Admin

| Endpoint | Метод | Аудит лог |
| --- | --- | --- |
| `/viewer/bscontrollerinfo` | GET | не логируется |
| `/viewer/bscontrollerinfo` | POST | логируется |
| `/viewer/topic_data` | GET | не логируется |
| `/viewer/topic_data` | POST | логируется |
