# Reliable Detection: Is a YDB EDS a Table (via Connector) or a Topic (via PQ)?

## Problem

When a user creates an External Data Source (EDS) with `SOURCE_TYPE="Ydb"`, the system
needs to decide whether to route access through the **connector** (for tables) or through
the **PQ provider** (for topics). Both cases use the same `SOURCE_TYPE="Ydb"` — there is no
separate source type to distinguish them.

Previously, the code in [`kqp_metadata_loader.cpp`](ydb/core/kqp/gateway/kqp_metadata_loader.cpp)
used a heuristic called `GetSchemeEntryType` that performed a **remote `DescribePath` call**
to the target YDB cluster to inspect the actual scheme entry type (Topic vs Table). This had
two problems:

1. **Performance**: An extra network round-trip on every metadata load.
2. **Silent fallback**: When `federatedQuerySetup` was not available (e.g., in non-federated
   contexts), the function silently returned `Table`, causing topics to be **misrouted** to
   the connector.

## Solution: `DatabaseNames` in Connector Config (with backward-compatible fallback)

Instead of adding a new `SOURCE_TYPE` (the previous `YdbConnector` approach, now reverted),
we add a **`repeated string DatabaseNames`** field to the connector configuration proto.
Each connector declares which database names it handles. At metadata load time, the system
checks whether the EDS's `database_name` matches any connector's `DatabaseNames` list.

**Backward compatibility**: The old `GetSchemeEntryType` heuristic is **kept** as a fallback.
The new `DatabaseNames`-based routing is only used when at least one connector has a
non-empty `DatabaseNames` list. When no connector has `DatabaseNames` configured, the
system falls back to the old `GetSchemeEntryType` behavior (remote `DescribePath` call).
This allows a gradual rollout: (1) ship the version with config support, (2) configure
`DatabaseNames` on the connectors, (3) remove `GetSchemeEntryType` in the next version.

### 1. Proto Change

File: [`gateways_config.proto`](yql/essentials/providers/common/proto/gateways_config.proto)

```protobuf
message TGenericConnectorConfig {
    // ... existing fields ...

    // List of database names that this connector handles.
    // When loading metadata for an EDS with SOURCE_TYPE="Ydb", the system
    // checks if the EDS database_name matches any entry in this list.
    // If it matches, the EDS is routed to the connector (table access).
    // If it does not match, the EDS is routed to the PQ provider (topic access).
    // Default = empty (connector handles all YDB databases)
    repeated string DatabaseNames = 9;

    reserved 1, 2;
}
```

The `TGenericGatewayConfig` message contains:
- `optional TGenericConnectorConfig Connector = 5;` — the default connector
- `repeated TGenericConnectorConfig Connectors = 8;` — a list of additional connectors

Both are checked during routing.

### 2. Routing Logic in `kqp_metadata_loader.cpp`

File: [`kqp_metadata_loader.cpp`](ydb/core/kqp/gateway/kqp_metadata_loader.cpp)

#### Helper Function: `IsYdbDataSourceRoutedToConnector`

```cpp
bool IsYdbDataSourceRoutedToConnector(const TString& databaseName,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
{
    if (!federatedQuerySetup) {
        // No federated query setup — default to connector (table access).
        return true;
    }

    const auto& gatewayConfig = federatedQuerySetup->GenericGatewayConfig;

    // Check the default connector.
    if (gatewayConfig.HasConnector()) {
        const auto& dbNames = gatewayConfig.GetConnector().GetDatabaseNames();
        if (dbNames.empty() || std::find(dbNames.begin(), dbNames.end(), databaseName) != dbNames.end()) {
            return true;
        }
    }

    // Check the list of connectors.
    for (const auto& connector : gatewayConfig.GetConnectors()) {
        const auto& dbNames = connector.GetDatabaseNames();
        if (dbNames.empty() || std::find(dbNames.begin(), dbNames.end(), databaseName) != dbNames.end()) {
            return true;
        }
    }

    return false;
}
```

**Logic:**
1. If `FederatedQuerySetup` is not set → return `true` (backward compatibility: route to connector).
2. Check the default `Connector` (field 5 of `TGenericGatewayConfig`):
   - If `DatabaseNames` is **empty** → this connector handles **all** YDB databases → return `true`.
   - If `DatabaseNames` **contains** the EDS's `database_name` → return `true`.
3. Check each connector in the `Connectors` list (field 8) with the same logic.
4. If no connector matches → return `false` (route to PQ provider as a topic).

#### Helper Function: `HasDatabaseNamesConfigured`

```cpp
bool HasDatabaseNamesConfigured(const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
{
    if (!federatedQuerySetup) {
        return false;
    }

    const auto& gatewayConfig = federatedQuerySetup->GenericGatewayConfig;

    if (gatewayConfig.HasConnector() && !gatewayConfig.GetConnector().GetDatabaseNames().empty()) {
        return true;
    }

    for (const auto& connector : gatewayConfig.GetConnectors()) {
        if (!connector.GetDatabaseNames().empty()) {
            return true;
        }
    }

    return false;
}
```

**Logic:** Returns `true` if any connector (default or in the list) has a non-empty
`DatabaseNames` list. This is used to decide whether to use the new config-based routing
or fall back to the old `GetSchemeEntryType` heuristic.

#### Routing Decision in `KindExternalDataSource` Case

In the `KindExternalDataSource` case of the metadata loading switch, the routing uses
a **dual approach** for backward compatibility:

**Step 1 — New routing (early, before secrets subscription):**

If `HasDatabaseNamesConfigured` returns `true`, the new config-based routing is applied
immediately. No remote `DescribePath` call is needed:

```cpp
const bool useNewRouting = HasDatabaseNamesConfigured(federatedQuerySetup);
if (useNewRouting &&
    resolveEntityInsideDataSource &&
    externalDataSourceMetadata.Metadata->ExternalSource.Type == ToString(NYql::EDatabaseType::Ydb))
{
    const auto& props = externalDataSourceMetadata.Metadata->ExternalSource.Properties.GetProperties();
    auto it = props.find("database_name");
    const TString databaseName = (it != props.end()) ? it->second : TString();
    if (!IsYdbDataSourceRoutedToConnector(databaseName, federatedQuerySetup)) {
        // Route to PQ provider (topic access).
        externalDataSourceMetadata.Metadata->ExternalSource.Type = ToString(NYql::EDatabaseType::YdbTopics);
    }
}
```

**Step 2 — Old fallback (inside secrets subscription lambda):**

If `useNewRouting` is `false` (no connector has `DatabaseNames` configured), the old
`GetSchemeEntryType` heuristic is used inside the secrets subscription lambda. This
performs a remote `DescribePath` call to distinguish Topic from Table:

```cpp
if (!useNewRouting &&
    externalDataSourceMetadata.Metadata->ExternalSource.Type == ToString(NYql::EDatabaseType::Ydb) && externalPath &&
    settings.ExternalSourceFactory && settings.ExternalSourceFactory->IsAvailableProvider(TString(NYql::PqProviderName)))
{
    // ... extract properties, build structured token, call GetSchemeEntryType ...
    GetSchemeEntryType(federatedQuerySetup, source.DataSourceLocation, databaseName, useTls, structuredTokenJson, path)
        .Subscribe([...] (const NThreading::TFuture<TGetSchemeEntryResult>& result) mutable {
            TGetSchemeEntryResult value = result.GetValue();
            if (!value.EntryType) {
                // Error: couldn't determine entity type
                // ...
                return;
            }
            if (*value.EntryType == NYdb::NScheme::ESchemeEntryType::Topic) {
                externalDataSourceMetadata.Metadata->ExternalSource.Type = ToString(NYql::EDatabaseType::YdbTopics);
            }
            f(externalDataSourceMetadata);  // continue with loadDynamicMetadata
        });
} else {
    loadDynamicMetadata(externalDataSourceMetadata);
}
```

**How the dual approach works:**
1. `HasDatabaseNamesConfigured(federatedQuerySetup)` checks if any connector has a
   non-empty `DatabaseNames` list.
2. If **yes** (`useNewRouting = true`): the new config-based routing is applied early.
   The EDS's `database_name` is matched against connector `DatabaseNames`. If no match →
   type changes to `"YdbTopics"` (PQ provider). The old `GetSchemeEntryType` path is
   skipped entirely.
3. If **no** (`useNewRouting = false`): the old `GetSchemeEntryType` heuristic runs inside
   the secrets subscription lambda, performing a remote `DescribePath` call. This
   preserves the exact pre-change behavior.

### 3. Data Flow

```
User creates EDS:
  CREATE EXTERNAL DATA SOURCE `my_eds` WITH (
    SOURCE_TYPE="Ydb",
    LOCATION="grpc://some-ydb-cluster:2135",
    DATABASE_NAME="/my/database/path"
  );

User queries: SELECT * FROM `my_eds`.`my_table`;

                    ┌─────────────────────────────────┐
                    │  kqp_metadata_loader.cpp          │
                    │                                  │
                    │  1. Load EDS metadata from       │
                    │     scheme cache                 │
                    │     → ExternalSource.Type="Ydb"  │
                    │     → Properties["database_name"] │
                    │       = "/my/database/path"      │
                    │                                  │
                    │  2. HasDatabaseNamesConfigured?   │
                    │     (any connector has non-empty  │
                    │      DatabaseNames?)              │
                    │                                  │
                    │  ┌─ YES (useNewRouting=true) ──┐  │
                    │  │ 3a. IsYdbDataSourceRoutedToConnector( │
                    │  │     "/my/database/path",     │  │
                    │  │     federatedQuerySetup)     │  │
                    │  │                              │  │
                    │  │ 4a. MATCH → Type stays "Ydb" │  │
                    │  │     → Connector (table)      │  │
                    │  │ 4b. NO MATCH → Type="YdbTopics" │
                    │  │     → PQ provider (topic)    │  │
                    │  │ (no remote DescribePath call)│  │
                    │  └──────────────────────────────┘  │
                    │                                  │
                    │  ┌─ NO (useNewRouting=false) ──┐  │
                    │  │ 3b. GetSchemeEntryType()     │  │
                    │  │     (remote DescribePath)    │  │
                    │  │                              │  │
                    │  │ 4c. Topic → Type="YdbTopics" │  │
                    │  │     → PQ provider (topic)    │  │
                    │  │ 4d. Table → Type stays "Ydb" │  │
                    │  │     → Connector (table)      │  │
                    │  │ (old behavior, backward compat) │
                    │  └──────────────────────────────┘  │
                    └─────────────────────────────────┘
```

### 4. Configuration Examples

#### Example A: No `DatabaseNames` configured (old behavior, backward compatible)

```protobuf
GenericGatewayConfig {
    Connector {
        Endpoint { Host: "localhost" Port: 12345 }
        # DatabaseNames not set
    }
}
```

Result: `HasDatabaseNamesConfigured` returns `false`, so the old `GetSchemeEntryType`
heuristic is used. A remote `DescribePath` call distinguishes Topic from Table.
This is the exact pre-change behavior — no deployment changes needed.

#### Example B: Connector handles only specific databases (new routing)

```protobuf
GenericGatewayConfig {
    Connector {
        Endpoint { Host: "localhost" Port: 12345 }
        DatabaseNames: "/cluster1/db1"
        DatabaseNames: "/cluster1/db2"
    }
}
```

Result: `HasDatabaseNamesConfigured` returns `true`, so the new config-based routing
is used (no remote `DescribePath` call):
- EDS with `database_name="/cluster1/db1"` → connector (table)
- EDS with `database_name="/cluster1/db2"` → connector (table)
- EDS with `database_name="/cluster2/db3"` → PQ provider (topic)

#### Example C: Multiple connectors for different databases

```protobuf
GenericGatewayConfig {
    Connectors {
        Endpoint { Host: "connector-a" Port: 12345 }
        DatabaseNames: "/db_a"
    }
    Connectors {
        Endpoint { Host: "connector-b" Port: 12345 }
        DatabaseNames: "/db_b"
    }
}
```

Result:
- EDS with `database_name="/db_a"` → connector-a (table)
- EDS with `database_name="/db_b"` → connector-b (table)
- EDS with `database_name="/db_c"` → PQ provider (topic)

#### Example D: Mixed — default connector + specific connectors

```protobuf
GenericGatewayConfig {
    Connector {
        Endpoint { Host: "default-connector" Port: 12345 }
        DatabaseNames: "/default_db"
    }
    Connectors {
        Endpoint { Host: "special-connector" Port: 12345 }
        DatabaseNames: "/special_db"
    }
}
```

Result:
- EDS with `database_name="/default_db"` → default connector (table)
- EDS with `database_name="/special_db"` → special connector (table)
- EDS with `database_name="/other_db"` → PQ provider (topic)

### 5. What Was Kept (Backward Compatibility)

The old heuristic-based approach is **kept** as a fallback, not removed:

- **`GetSchemeEntryType`** function — **retained**. This function performs a remote
  `DescribePath` call to distinguish Ydb topics from Ydb tables. It is used as the
  fallback path when no connector has `DatabaseNames` configured. This preserves the
  exact pre-change behavior for deployments that have not yet configured
  `DatabaseNames`.
- **`ComposeStructuredTokenJsonForExternalDataSource`** function — **retained** (used
  by `GetSchemeEntryType` to build the auth token for the remote `DescribePath` call).
- Related includes (`yql_token_builder.h`, `factory.h`) — **retained**.

> **Rollout plan**: `GetSchemeEntryType` can be removed in the **next** version after
> all deployments have configured `DatabaseNames` on their connectors. Until then, it
> serves as the backward-compatible fallback.

### 6. What Was NOT Changed

- **No new `SOURCE_TYPE`** — the `EDatabaseType` enum, `EGenericDataSourceKind` enum,
  external source factory, and all provider files remain unchanged from their original
  state (the `YdbConnector` type was fully reverted).
- **`EDatabaseType::Ydb`** still covers both tables and topics. The disambiguation
  happens at metadata load time based on the connector config, not at the source type level.
- The `YdbTopics` type already existed in `EDatabaseType` and is used by the PQ provider
  for topic access. We simply change the `ExternalSource.Type` field from `"Ydb"` to
  `"YdbTopics"` when the routing decision says "topic".

### 7. Backward Compatibility

| Scenario | `FederatedQuerySetup` | `DatabaseNames` configured? | Routing used | Result |
|----------|----------------------|---------------------------|--------------|--------|
| Non-federated query | not set | N/A | New (returns `true`) | connector (table) — same as before |
| Federated, no `DatabaseNames` | set | no (all empty) | **Old** (`GetSchemeEntryType`) | remote `DescribePath` — same as before |
| Federated, `DatabaseNames` set | set | yes | **New** (`IsYdbDataSourceRoutedToConnector`) | match → connector (table); no match → PQ (topic) |

The first two rows preserve existing behavior exactly. Only when an administrator
explicitly configures `DatabaseNames` on at least one connector does the routing
switch to the new config-based approach — eliminating the remote `DescribePath` call
and its silent-fallback problem.

### 8. Rollout Plan

1. **Version 1 (this change)**: Ship with `DatabaseNames` field support. The old
   `GetSchemeEntryType` heuristic remains active as the fallback when no connector has
   `DatabaseNames` configured. No behavior change for existing deployments.
2. **Configure**: Administrators set `DatabaseNames` on connectors for the YDB
   databases that should be accessed via the connector (table access). After
   configuration, the new routing is used — no remote `DescribePath` call.
3. **Version 2 (future)**: Remove `GetSchemeEntryType` and
   `ComposeStructuredTokenJsonForExternalDataSource` (dead code after all deployments
   have configured `DatabaseNames`).

### 9. Files Changed

| File | Change |
|------|--------|
| [`gateways_config.proto`](yql/essentials/providers/common/proto/gateways_config.proto) | Added `repeated string DatabaseNames = 9;` to `TGenericConnectorConfig` |
| [`kqp_metadata_loader.cpp`](ydb/core/kqp/gateway/kqp_metadata_loader.cpp) | Added `IsYdbDataSourceRoutedToConnector()` and `HasDatabaseNamesConfigured()` helpers; added dual routing logic in `KindExternalDataSource` case (new if configured, old `GetSchemeEntryType` fallback if not); added `#include <algorithm>`; restored `GetSchemeEntryType` and `ComposeStructuredTokenJsonForExternalDataSource` for backward compatibility |
| [`kqp_federated_query_helpers.h`](ydb/core/kqp/federated_query/kqp_federated_query_helpers.h) | Unchanged (restored to HEAD — `GetSchemeEntryType` and `TGetSchemeEntryResult` retained) |
| [`kqp_federated_query_helpers.cpp`](ydb/core/kqp/federated_query/kqp_federated_query_helpers.cpp) | Unchanged (restored to HEAD — `GetSchemeEntryType` implementation retained) |
