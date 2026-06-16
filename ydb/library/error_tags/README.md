# YDB user-facing error codes (`YDBE-XXXXX`)

This directory is the source of truth for YDB user-facing error labels of the
form **`YDBE-XXXXX`** — the prefix `YDBE` ("YDB Error") plus exactly 5 digits.

A label is **not a contract**: application logic must not depend on it. It is a
stable, machine-readable anchor that a user or support engineer can search for,
independent of the (localizable, changing) message text.

Printed form — in square brackets at the start of the message:

```
[YDBE-10014] List of Node Broker endpoints is empty
```

## Files

- [`registry.txt`](registry.txt) — the registry: one code per line, `code` then
  whitespace then a short one-line description. Comments start with `#`.
- [`validate_error_tags.py`](validate_error_tags.py) — the CI validator.

## Registry format

```
YDBE-10001 Failed to parse YAML config: syntax error
YDBE-10007 DomainsConfig is not provided
YDBE-21001 Local node cannot join cluster
```

The format is intentionally trivial to parse: the line starts with the code,
the rest of the line is the description.

## Component ranges (first two digits)

The first two digits group codes by component. This is a *guideline*, not a
hard rule: it only helps keep collisions between new codes rare. Teams may
adjust the split as long as a range has no real codes in it yet, and a code
that ends up in the "wrong" range still works.

### 0x — platform / libraries

| Code | Component               | Path |
|------|-------------------------|------|
| `00` | generic / util          | `library/` (common), `core/util`, `core/base` (base primitives) |
| `01` | actors                  | `library/actors` |
| `02` | interconnect            | `library/actors/interconnect` |
| `03` | grpc-lib                | `library/grpc`, `core/grpc_services`, `core/grpc_streaming`, `core/grpc_caching` |
| `04` | monitoring / whiteboard | `core/mon`, `core/node_whiteboard`, `core/control`, `library/logger` |
| `05` | tracing                 | `core/tracing`, `core/jaeger_tracing`, `library/wilson_ids` |
| `06` | erasure / formats       | `core/erasure`, `core/formats`, `core/io_formats` |
| `07` | scheme types            | `core/scheme`, `core/scheme_types` |
| `08`–`09` | *reserved*         | |

### 1x — control plane / cluster lifecycle

| Code | Component                 | Path |
|------|---------------------------|------|
| `10` | config / init             | `core/config`, `core/driver_lib`, `library/yaml_config` |
| `11` | cms / console             | `core/cms` |
| `12` | discovery                 | `core/discovery`, `core/mind/tenant_node_enumeration*` |
| `13` | node_broker / nameservice | `core/mind/node_broker*`, `core/mind/dynamic_nameserver*` |
| `14` | tenant / slot broker      | `core/mind/tenant_*` |
| `15` | health check              | `core/health_check` |
| `16` | security / auth           | `core/security`, `library/login`, `library/aclib`, `library/security` |
| `17` | audit                     | `core/audit` |
| `18`–`19` | *reserved*           | |

### 2x — blobstorage

| Code | Component             | Path |
|------|-----------------------|------|
| `20` | dsproxy               | `core/blobstorage/dsproxy`, `core/blobstorage/groupinfo` |
| `21` | distconf / nodewarden | `core/blobstorage/nodewarden` |
| `22` | pdisk                 | `core/blobstorage/pdisk`, `library/pdisk_io` |
| `23` | vdisk                 | `core/blobstorage/vdisk`, `core/blobstorage/incrhuge` |
| `24` | bs_controller         | `core/mind/bscontroller` |
| `25` | bridge                | `core/blobstorage/bridge` |
| `26` | blob_depot            | `core/blob_depot` |
| `27`–`29` | *reserved*       | |

### 3x — tablet infrastructure / system tablets

| Code | Component                    | Path |
|------|------------------------------|------|
| `30` | tablet executor              | `core/tablet_flat` |
| `31` | tablet infra                 | `core/tablet` |
| `32` | state storage / scheme board | `core/base` (statestorage), `core/tx/scheme_board` |
| `33` | hive                         | `core/mind/hive` |
| `34` | keyvalue                     | `core/keyvalue` |
| `35` | kesus / quoter               | `core/kesus`, `core/quoter` |
| `36` | sequence / allocator         | `core/tx/sequenceshard`, `core/tx/sequenceproxy`, `core/tx/tx_allocator*` |
| `37`–`39` | *reserved*              | |

### 4x — transactions / data plane

| Code | Component                    | Path |
|------|------------------------------|------|
| `40` | datashard                    | `core/tx/datashard` |
| `41` | schemeshard                  | `core/tx/schemeshard` |
| `42` | coordinator / mediator       | `core/tx/coordinator`, `core/tx/mediator`, `core/tx/time_cast` |
| `43` | tx_proxy / scheme_cache      | `core/tx/tx_proxy`, `core/tx/scheme_cache`, `core/tx/long_tx_service` |
| `44` | replication / transfer / CDC | `core/tx/replication`, `core/transfer`, `core/change_exchange` |
| `45` | locks                        | `core/tx/locks` |
| `46` | backup / export-import       | `core/backup`, `library/backup` |
| `47`–`49` | *reserved*              | |

### 5x — columnshard / OLAP

| Code | Component              | Path |
|------|------------------------|------|
| `50` | columnshard            | `core/tx/columnshard` |
| `51` | olap engine / sharding | `core/tx/sharding`, `core/tx/program`, `core/tx/tiering` |
| `52` | conveyor / priorities  | `core/tx/conveyor*`, `core/tx/priorities`, `core/tx/limiter` |
| `53` | statistics             | `core/statistics` |
| `54`–`59` | *reserved*        | |

### 6x — query

| Code | Component               | Path |
|------|-------------------------|------|
| `60` | kqp                     | `core/kqp` |
| `61` | yql engine              | `library/yql`, `core/engine` |
| `62` | fq (federated queries)  | `core/fq`, `core/external_sources` |
| `63` | resource pools          | `core/resource_pools` |
| `64`–`69` | *reserved*         | |

### 7x — streaming / external protocols

| Code | Component              | Path |
|------|------------------------|------|
| `70` | topics / persqueue     | `core/persqueue`, `library/persqueue` |
| `71` | kafka proxy            | `core/kafka_proxy`, `library/kafka` |
| `72` | pg wire / proxy        | `core/pgproxy`, `core/local_pgwire` |
| `73` | http proxy / ymq (sqs) | `core/http_proxy`, `core/ymq`, `library/http_proxy` |
| `74` | public http            | `core/public_http` |
| `75`–`79` | *reserved*        | |

### 8x — client

| Code | Component | Path |
|------|-----------|------|
| `80` | sdk       | sdk |
| `81` | cli       | `public/lib/ydb_cli` |
| `82` | driver    | driver |
| `83`–`89` | *reserved* | |

### 9x — observability / misc

| Code | Component             | Path |
|------|-----------------------|------|
| `90` | viewer                | `core/viewer` |
| `91` | sys_view              | `core/sys_view` |
| `92` | metering              | `core/metering` |
| `93` | load test / workload  | `core/load_test`, `library/workload` |
| `94` | graph                 | `core/graph` |
| `95` | memory controller     | `core/memory_controller` |
| `99` | unknown / unclassified| for temporarily unclassified codes |

### Currently registered components

| Code | Component             | Path |
|------|-----------------------|------|
| `02` | interconnect          | `library/actors/interconnect` |
| `10` | config / init         | `core/config`, `library/yaml_config` |
| `21` | distconf / nodewarden | `core/blobstorage/nodewarden` |

## Adding a new code

1. Pick the component prefix (first two digits); take a free one if none fits.
2. Take the next free code inside that prefix.
3. Add a line to `registry.txt`: code and a short description.
4. Use it in the message: `[YDBE-XXXXX] <error text>`.
5. (Optional) run the validator locally; otherwise CI will catch problems.

## Running the validator

```bash
python3 ydb/library/error_tags/validate_error_tags.py
```

It checks:

1. **Format** — every registry code matches `^YDBE-\d{5}$`.
2. **Uniqueness** — no duplicate codes in the registry.
3. **Coverage** — every `YDBE-XXXXX` used in C/C++ sources is in the registry.

It also warns about orphan codes (registered but unused) without failing.
