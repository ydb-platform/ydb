# NBS-Like DBG Load Actor

This directory documents the implemented `NbsDbgLikeLoad` load generator and its persistent load tablet. It replaces the original numbered design specification. Treat the current source and protocol as authoritative when changing behavior.

The load tablet approximates the NBS PersistentBuffer replication path over Direct Block Groups (DBGs). It owns a persistent allocation and per-DBG actors; short-lived load workers issue writes and reads through tablet pipes. It is a performance tool, with less recovery and consistency machinery than an NBS partition.

## Read Only the Relevant Guide

| Task | Start Here |
| --- | --- |
| Create tablets, run workloads, interpret parameters/results, clean up | [Contributor usage guide](../../../../docs/en/core/contributor/load-actors-nbs-dbg-like.md) |
| Change actor ownership, allocation, persistence, routing, or restart | [Architecture](architecture.md) |
| Change address selection, write/read paths, flush, erase, or draining | [Workload](workload.md) |
| Choose tests, inspect counters, or check current limitations | [Testing and Observability](testing.md) |
| Change shared DBG allocation/placement | [Direct Block Groups](../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md) |
| Change DDisk or PB contracts | [DDisk](../../../../docs/en/core/contributor/distributed-storage/ddisk.md) and [PersistentBuffer](../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md) |
| Compare the production NBS partition | [Direct Partition Source Guide](../../../nbs/cloud/blockstore/libs/storage/partition_direct/README.md) |

## Source Map

All paths below are relative to this directory. Configuration reference tables live in the contributor usage guide; do not copy them into local implementation notes.

| Source | Responsibility |
| --- | --- |
| [nbs_dbg_like_load.cpp](../../nbs_dbg_like_load.cpp) | Factory, multi-tablet coordinator, per-tablet proxy, load workers, and result aggregation |
| [nbs_dbg_like_load_tablet.cpp](../../nbs_dbg_like_load_tablet.cpp) | Persistent tablet, per-DBG actors, peer connections, LSN state, flush, erase, and read routing |
| [nbs_dbg_like_alloc_helper.cpp](../../nbs_dbg_like_alloc_helper.cpp) | BSC request construction/response parsing and shared address-routing parameters |
| [nbs_dbg_like_load_service.cpp](../../nbs_dbg_like_load_service.cpp) | HTTP tablet lifecycle helpers, Hive/tenant resolution, and tablet UI |
| [service_actor.cpp](../../service_actor.cpp) | General load service, HTTP dispatch, and result publication |
| [events.h](../../events.h) | Typed event wrappers, payload serialization, and finish statistics |
| [nbs_dbg_like_load_defs.h](../../nbs_dbg_like_load_defs.h) | Host limits, sector size, phases, and LSN states |
| [load_test.proto](../../../protos/load_test.proto) | Configuration and transport messages |

When changing a contract, update its owning document and relevant tests. Keep proposed extensions separate from the description of implemented behavior.
