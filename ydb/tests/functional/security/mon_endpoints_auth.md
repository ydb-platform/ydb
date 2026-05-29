# Monitoring HTTP endpoints

Документ сгенерирован скриптом `generate_mon_endpoints_doc.py` из канонических данных `test_mon_endpoints_auth` (режим `enforce_user_token_enabled`).

Источник: `ydb/tests/functional/security/canondata/test_mon_endpoints_auth.test_mon_endpoints_auth-enforce_user_token_enabled/mon_endpoints_auth-enforce_user_token_enabled.json`

## Правила аудит-логирования (целевое состояние)

- Все модифицирующие методы, кроме OPTIONS, аудируются.
- Все запросы уровня `monitoring_allowed_sids` и `admin_allowed_sids` становятся аудируемыми, кроме статики.
- Ограниченный набор эндпойнтов, у которых нельзя поднять уровень до `monitoring_allowed_sids`.
- Обращения в `/viewer/acl`, `/viewer/describe` к объектам без схемных прав становятся аудируемыми (решение по внутреннему отказу, не по внешнему HTTP-коду).
- Исключения: `/internal` (псевдостатика).

## Как определяется «текущий уровень»

- Для public/database уровней **400** считается признаком, что запрос прошёл access check и дошёл до валидации handler.
- Для viewer+ уровней **2xx** из canon считается успешным доступом.
- Если **2xx** нет, для ручек viewer+ уровень берётся из политики handler (`CheckAccessViewer` / `CheckAccessMonitoring` / `CheckAccessAdministration` в C++), а не из первого **400** в canon.
- **400** в комментарии для viewer+ — диагностика (невалидный метод/body/параметры), а не доказательство, что этого токена достаточно для действия.

## Группа 1 — public (no token)

| Endpoint | Метод | Текущий уровень | Аудит лог | Комментарий |
| --- | --- | --- | --- | --- |
| `/actors/tablet_counters_aggregator` | GET | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/actors/tablet_counters_aggregator` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/counters` | GET | public | не логируется | без токена: 200 — запрос дошёл до handler |
| `/counters` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/counters/hosts` | GET | public | не логируется | без токена: 200 — запрос дошёл до handler |
| `/counters/hosts` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/followercounters` | GET | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/followercounters` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/healthcheck?database=/Root` | GET | public | не логируется | без токена: 200 — запрос дошёл до handler |
| `/healthcheck?database=/Root` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/labeledcounters` | GET | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/labeledcounters` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/login` | GET | public | логируется | без токена: 400 — запрос дошёл до handler |
| `/login` | POST | public | логируется | без токена: 400 — запрос дошёл до handler |
| `/monitoring/` | GET | public | не логируется | без токена: 200 — запрос дошёл до handler |
| `/monitoring/` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/node/1/monitoring` | GET | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/node/1/monitoring` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/ping` | GET | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/ping` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/status` | GET | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/status` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |
| `/viewer/capabilities` | GET | public | не логируется | без токена: 200 — запрос дошёл до handler |
| `/viewer/capabilities` | POST | public | логируется | без токена: 200 — запрос дошёл до handler |

## Группа 2 — database

| Endpoint | Метод | Текущий уровень | Аудит лог | Комментарий |
| --- | --- | --- | --- | --- |
| `/operation/cancel` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/cancel` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/forget` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/forget` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/get` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/get` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/list` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/operation/list` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/query/script/execute` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/query/script/execute` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/query/script/fetch` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/query/script/fetch` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/scheme/directory` | DELETE | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/scheme/directory` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/scheme/directory` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/storage/groups?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/storage/groups?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/acl?database=/Root` | GET | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/acl?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/autocomplete?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/autocomplete?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/browse` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/browse` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/bscontrollerinfo` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/bscontrollerinfo` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/bsgroupinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/bsgroupinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/check_access?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/check_access?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/cluster` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/cluster` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/commit_offset` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/commit_offset` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/compute` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/compute` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/config` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/config` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/content` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/content` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/counters` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/counters` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/database_stats?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/database_stats?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe?database=/Root` | GET | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_consumer` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_consumer` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_replication` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_replication` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_topic` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_topic` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_transfer` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/describe_transfer` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/feature_flags?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/feature_flags?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/graph` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/graph` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/groups?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/groups?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/hiveinfo` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/hiveinfo` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/hivestats` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/hivestats` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/hotkeys?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/hotkeys?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/labeledcounters` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/labeledcounters` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/metainfo` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/metainfo` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/multipart_counter?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/multipart_counter?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/netinfo` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/netinfo` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/nodeinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/nodeinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/nodelist?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/nodelist?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/nodes?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/nodes?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/pdiskinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/pdiskinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/peers?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/peers?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/plan2svg` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/plan2svg` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/pqconsumerinfo` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/pqconsumerinfo` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/put_record` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/put_record` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/query?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/query?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/render?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/render?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/simple_counter?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/simple_counter?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/sse_counter?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/sse_counter?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/storage` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/storage` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/storage_stats?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/storage_stats?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/storage_usage` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/storage_usage` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/sysinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/sysinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/tabletcounters` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/tabletcounters` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/tabletinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/tabletinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler; политика: database |
| `/viewer/tenantinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/tenantinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/tenants` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/tenants` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/topic_data` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/topic_data` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/topicinfo` | GET | database | не логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/topicinfo` | POST | database | логируется | database@builtin: 200 — запрос дошёл до handler |
| `/viewer/v2/json/nodeinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/nodeinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/pdiskinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/pdiskinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/sysinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/sysinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/tabletinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/tabletinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/vdiskinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/v2/json/vdiskinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/vdiskinfo?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/vdiskinfo?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/whoami?database=/Root` | GET | database | не логируется | database@builtin: 400 — запрос дошёл до handler |
| `/viewer/whoami?database=/Root` | POST | database | логируется | database@builtin: 400 — запрос дошёл до handler |

## Группа 3 — viewer

| Endpoint | Метод | Текущий уровень | Аудит лог | Комментарий |
| --- | --- | --- | --- | --- |
| `/pdisk/info` | GET | viewer | не логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/pdisk/info` | POST | viewer | логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/vdisk/blobindexstat` | GET | viewer | не логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/vdisk/blobindexstat` | POST | viewer | логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/vdisk/getblob` | GET | viewer | не логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/vdisk/getblob` | POST | viewer | логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/vdisk/vdiskstat` | GET | viewer | не логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |
| `/vdisk/vdiskstat` | POST | viewer | логируется | viewer@builtin: 400 — запрос дошёл до handler, но ответ bad request |

## Группа 4 — monitoring

| Endpoint | Метод | Текущий уровень | Аудит лог | Комментарий |
| --- | --- | --- | --- | --- |
| `/actors/` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/blobstorageproxies` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/blobstorageproxies` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/configs_dispatcher` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/configs_dispatcher` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/console_configs_provider` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/console_configs_provider` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/dnameserver` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/dnameserver` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/dsproxynode` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/dsproxynode` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/feature_flags` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/feature_flags` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/icb` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/icb` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/interconnect` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/interconnect` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_node` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_node` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_proxy` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_proxy` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_resource_manager` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_resource_manager` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_spilling_file` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/kqp_spilling_file` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/logger` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/logger` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/memory_tracker` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/memory_tracker` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/netclassifier` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/netclassifier` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/nodewarden` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/nodewarden` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/pdisks` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/pdisks` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/pql2` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/pql2` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/quoter_proxy` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/quoter_proxy` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/rb` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/rb` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/statservice` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/statservice` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/tenant_pool` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/tenant_pool` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/vdisks` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/actors/vdisks` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/cms` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/cms` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/grpc` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/grpc` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/internal` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/internal` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/jquery.tablesorter.css` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/jquery.tablesorter.css` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/jquery.tablesorter.js` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/jquery.tablesorter.js` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/fragmentation` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/fragmentation` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/heap` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/heap` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/peakheap` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/peakheap` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/statistics` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/memory/statistics` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/nodetabmon` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/nodetabmon` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/pdisk/restart` | GET | monitoring | не логируется | политика: CheckAccessMonitoring; canon: viewer@builtin: 400, monitoring@builtin: 400, root@builtin: 400; canon доходит до handler с viewer@builtin (400 ≠ уровень доступа) |
| `/pdisk/restart` | POST | monitoring | логируется | политика: CheckAccessMonitoring; canon: viewer@builtin: 400, monitoring@builtin: 400, root@builtin: 400; canon доходит до handler с viewer@builtin (400 ≠ уровень доступа) |
| `/pdisk/status` | GET | monitoring | не логируется | политика: CheckAccessMonitoring; canon: viewer@builtin: 400, monitoring@builtin: 400, root@builtin: 400; canon доходит до handler с viewer@builtin (400 ≠ уровень доступа) |
| `/pdisk/status` | POST | monitoring | логируется | политика: CheckAccessMonitoring; canon: viewer@builtin: 400, monitoring@builtin: 400, root@builtin: 400; canon доходит до handler с viewer@builtin (400 ≠ уровень доступа) |
| `/static/css/bootstrap.min.css` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/css/bootstrap.min.css` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.eot` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.eot` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.svg` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.svg` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.ttf` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.ttf` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.woff` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/fonts/glyphicons-halflings-regular.woff` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/static/js/bootstrap.min.js` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/js/bootstrap.min.js` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/static/js/jquery.min.js` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/static/js/jquery.min.js` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/tablet` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/tablet` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/tablets` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/tablets` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/trace` | GET | monitoring | логируется | monitoring@builtin: 200 |
| `/trace` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/vdisk/evict` | GET | monitoring | не логируется | политика: CheckAccessMonitoring; canon: viewer@builtin: 400, monitoring@builtin: 400, root@builtin: 400; canon доходит до handler с viewer@builtin (400 ≠ уровень доступа) |
| `/vdisk/evict` | POST | monitoring | логируется | политика: CheckAccessMonitoring; canon: viewer@builtin: 400, monitoring@builtin: 400, root@builtin: 400; canon доходит до handler с viewer@builtin (400 ≠ уровень доступа) |
| `/ver` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/ver` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/viewer/healthcheck` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/viewer/healthcheck` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/viewer/v2` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/viewer/v2` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/viewer/v2/json/config` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/viewer/v2/json/config` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/viewer/v2/json/nodelist` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/viewer/v2/json/nodelist` | POST | monitoring | логируется | monitoring@builtin: 200 |
| `/viewer/v2/json/storage` | GET | monitoring | не логируется | monitoring@builtin: 200 |
| `/viewer/v2/json/storage` | POST | monitoring | логируется | monitoring@builtin: 200 |

## Группа 5 — unavailable

| Endpoint | Метод | Текущий уровень | Аудит лог | Комментарий |
| --- | --- | --- | --- | --- |
| `/actors/lease` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/lease` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/row_dispatcher` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/row_dispatcher` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/schemeboard` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/schemeboard` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/sqsgc` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/sqsgc` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/yq_control_plane_proxy` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/yq_control_plane_proxy` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/yq_health` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/actors/yq_health` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/fq_diag/fetcher` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/fq_diag/fetcher` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/fq_diag/local_worker_manager` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/fq_diag/local_worker_manager` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/fq_diag/quotas` | GET | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/fq_diag/quotas` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/operation` | GET | unavailable | не логируется | нет успешного доступа (2xx) и нет политики handler |
| `/operation` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/pdisk` | GET | unavailable | не логируется | нет успешного доступа (2xx) и нет политики handler |
| `/pdisk` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/query` | GET | unavailable | не логируется | нет успешного доступа (2xx) и нет политики handler |
| `/query` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/scheme` | GET | unavailable | не логируется | нет успешного доступа (2xx) и нет политики handler |
| `/scheme` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/storage` | GET | unavailable | не логируется | нет успешного доступа (2xx) и нет политики handler |
| `/storage` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
| `/vdisk` | GET | unavailable | не логируется | нет успешного доступа (2xx) и нет политики handler |
| `/vdisk` | POST | unavailable | логируется | нет успешного доступа (2xx) и нет политики handler |
