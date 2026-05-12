# YDB Workload Library

This library provides a framework for implementing load generation workloads that can be run
against a YDB database via the `ydb workload` CLI command.

## Directory Structure

```
ydb/library/workload/
├── abstract/           # Core interfaces and factory
│   ├── workload_query_generator.h  # Main interfaces
│   ├── workload_factory.h          # Workload registration factory
│   └── colors.h / colors.cpp       # Terminal color helpers
├── benchmark_base/     # Base classes for TPC-style benchmark workloads
├── clickbench/         # ClickBench benchmark
├── fulltext/           # Full-text search workload
├── kv/                 # Key-Value workload
├── log/                # Log workload (append-heavy time-series)
├── mixed/              # Mixed read/write workload
├── query/              # Custom query workload
├── stock/              # Stock exchange simulation workload
├── tpc_base/           # Common code for TPC workloads
├── tpcc/               # TPC-C OLTP benchmark
├── tpcds/              # TPC-DS analytical benchmark
├── tpch/               # TPC-H analytical benchmark
└── vector/             # Vector similarity search workload
```

## Architecture Overview

Each workload consists of two main classes:

1. **`TWorkloadParams`** (or a subclass) — holds all configuration parameters and
   creates the generator.
2. **`IWorkloadQueryGenerator`** (or a subclass) — generates YQL/SQL queries and
   bulk data operations for the workload.

A workload is registered with the global factory using a static registrar object in a
`registrar.cpp` file (compiled with `GLOBAL` in `ya.make` so the object is always linked).

```
┌─────────────────────────────────────────────────────────────┐
│  YDB CLI (ydb workload <key> init / run / clean / import)   │
└─────────────────────────────┬───────────────────────────────┘
                              │  looks up key in
                              ▼
                    TWorkloadFactory
                    (NObjectFactory)
                              │  creates
                              ▼
                    TWorkloadParams  ──── holds ──── CLI options
                              │
                              │  CreateGenerator()
                              ▼
                IWorkloadQueryGenerator
                  ├── GetDDLQueries()          ← init
                  ├── GetInitialData()         ← init
                  ├── GetCleanPaths()          ← clean
                  └── GetWorkload(type)        ← run
```

## Key Interfaces

### `TWorkloadParams` (`abstract/workload_query_generator.h`)

Base class for all workload parameters. Subclass and override:

| Method | Description |
|--------|-------------|
| `GetWorkloadName()` | Human-readable name of the workload |
| `CreateGenerator()` | Factory: return a `THolder<IWorkloadQueryGenerator>` |
| `ConfigureOpts(opts, commandType, workloadType)` | Register command-line options for `init`, `run`, `clean`, or `import` commands |
| `GetDescription(commandType, workloadType)` | Optional: override CLI help text |
| `Validate(commandType, workloadType)` | Optional: validate parameter combinations after parsing |
| `Init()` | Optional: derived initialization after options are parsed |
| `CreateDataInitializers()` | Optional: return bulk-data initializers used by `import` |

The base class also exposes `DbPath`, `BulkSize`, `Verbose`, and client pointers
(`QueryClient`, `TableClient`, `SchemeClient`, `TopicClient`) that are filled in by the CLI
before calling `CreateGenerator()`.

### `IWorkloadQueryGenerator` (`abstract/workload_query_generator.h`)

Interface for the workload query generator:

| Method | Description |
|--------|-------------|
| `Init()` | Called once before queries start (e.g., load reference data) |
| `GetDDLQueries()` | Return a YQL string that creates all tables (used by `init`) |
| `GetInitialData()` | Return `TQueryInfoList` that populates tables with seed data (used by `init`) |
| `GetCleanPaths()` | Return full table paths to drop during `clean` |
| `GetWorkload(int type)` | Return `TQueryInfoList` for one iteration of workload type `type` |
| `GetSupportedWorkloadTypes()` | Return `TVector<TWorkloadType>` describing each run sub-command |

The convenience template `TWorkloadQueryGeneratorBase<TParams>` stores a `const TParams&`
reference and provides a no-op `Init()`.

### `TWorkloadType`

Describes a single `run` sub-command:

```cpp
TWorkloadType(
    int type,           // integer passed to GetWorkload(type)
    TString commandName,// CLI sub-command name (e.g., "upsert", "select")
    TString description,// help text
    EKind kind          // Workload (default) or Benchmark
)
```

### `TQueryInfo`

Holds a single query to execute:

```cpp
TQueryInfo info;
info.Query  = "--!syntax_v1\nUPSERT INTO ...";  // YQL query string
info.Params = paramsBuilder.Build();             // bound parameters
// Optional helpers:
info.UseReadRows = true;     // use ReadRows API instead of query
info.UseStaleRO  = true;     // stale read-only
info.TableOperation = ...;   // arbitrary table client operation (e.g., BulkUpsert)
```

### Bulk Data Initializers

For workloads that load data in bulk (used by `ydb workload <key> import`):

- Implement `TWorkloadDataInitializer` with `GetBulkInitialData()` returning a list of
  `IBulkDataGenerator` objects.
- Each generator produces `TDataPortion` objects containing CSV, Arrow IPC, or YDB `TValue`
  data.
- Return the initializer list from `TWorkloadParams::CreateDataInitializers()`.

## How to Add a New Workload

### Step 1: Create the Directory

```
mkdir ydb/library/workload/my_workload
```

### Step 2: Implement the Params Class

Create `my_workload.h`:

```cpp
#pragma once
#include <ydb/library/workload/abstract/workload_query_generator.h>

namespace NYdbWorkload {

class TMyWorkloadParams final : public TWorkloadParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts,
                       const ECommandType commandType,
                       int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override { return "My workload"; }

    // Add your parameters here:
    ui64 RowCount = 1000;
    TString TableName = "my_table";
};

} // namespace NYdbWorkload
```

### Step 3: Implement the Query Generator

Add the generator class to `my_workload.h`:

```cpp
class TMyWorkloadGenerator final
    : public TWorkloadQueryGeneratorBase<TMyWorkloadParams>
{
public:
    using TBase = TWorkloadQueryGeneratorBase<TMyWorkloadParams>;
    using TBase::TBase;

    std::string GetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;
    TVector<std::string> GetCleanPaths() const override;
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    enum class EType { Write, Read };
};
```

### Step 4: Implement the Bodies

Create `my_workload.cpp`:

```cpp
#include "my_workload.h"

namespace NYdbWorkload {

// --- Params ---

void TMyWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts,
                                      const ECommandType commandType,
                                      int /*workloadType*/)
{
    switch (commandType) {
    case ECommandType::Init:
        opts.AddLongOption("rows", "Number of rows to insert during init")
            .DefaultValue(RowCount).StoreResult(&RowCount);
        opts.AddLongOption("table", "Table name")
            .DefaultValue(TableName).StoreResult(&TableName);
        break;
    case ECommandType::Run:
        opts.AddLongOption("table", "Table name")
            .DefaultValue(TableName).StoreResult(&TableName);
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TMyWorkloadParams::CreateGenerator() const {
    return MakeHolder<TMyWorkloadGenerator>(this);
}

// --- Generator ---

std::string TMyWorkloadGenerator::GetDDLQueries() const {
    return "--!syntax_v1\n"
           "CREATE TABLE `" + Params.DbPath + "/" + Params.TableName + "` ("
           "    id   Uint64 NOT NULL,"
           "    data String,"
           "    PRIMARY KEY (id)"
           ");";
}

TQueryInfoList TMyWorkloadGenerator::GetInitialData() {
    // Return empty list if the init command only creates the schema.
    return {};
}

TVector<std::string> TMyWorkloadGenerator::GetCleanPaths() const {
    return { Params.DbPath + "/" + Params.TableName };
}

TVector<TMyWorkloadGenerator::TWorkloadType>
TMyWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {
        TWorkloadType(static_cast<int>(EType::Write), "write", "Write random rows"),
        TWorkloadType(static_cast<int>(EType::Read),  "read",  "Read random rows"),
    };
}

TQueryInfoList TMyWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
    case EType::Write: {
        NYdb::TParamsBuilder pb;
        pb.AddParam("$id").Uint64(RandomNumber<ui64>()).Build();
        pb.AddParam("$data").String("hello").Build();
        return TQueryInfoList(1, TQueryInfo(
            "--!syntax_v1\n"
            "DECLARE $id AS Uint64;"
            "DECLARE $data AS String;"
            "UPSERT INTO `" + Params.TableName + "` (id, data) VALUES ($id, $data);",
            pb.Build()));
    }
    case EType::Read: {
        NYdb::TParamsBuilder pb;
        pb.AddParam("$id").Uint64(RandomNumber<ui64>()).Build();
        return TQueryInfoList(1, TQueryInfo(
            "--!syntax_v1\n"
            "DECLARE $id AS Uint64;"
            "SELECT * FROM `" + Params.TableName + "` WHERE id = $id;",
            pb.Build()));
    }
    }
    return {};
}

} // namespace NYdbWorkload
```

### Step 5: Register the Workload

Create `registrar.cpp`:

```cpp
#include "my_workload.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TMyWorkloadParams> MyWorkloadRegistrar("my-workload");

} // namespace NYdbWorkload
```

The string `"my-workload"` becomes the CLI sub-command:
`ydb workload my-workload init / run / clean`.

### Step 6: Create `ya.make`

```
LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    my_workload.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

END()
```

> **Important:** `registrar.cpp` must be listed as `GLOBAL` so the static registrar object
> is always linked into the binary, making the workload available in the factory.

### Step 7: Add to the Root `ya.make`

Edit `ydb/library/workload/ya.make` and add your library to both `PEERDIR` and `RECURSE`
sections:

```
LIBRARY()

PEERDIR(
    ...
    ydb/library/workload/my_workload
)

END()

RECURSE(
    ...
    my_workload
)
```

### Step 8: (Optional) Add Import Support

If your workload supports bulk data loading via `ydb workload my-workload import`:

1. Create an `IBulkDataGenerator` subclass that produces data portions.
2. Create a `TWorkloadDataInitializer` subclass whose `GetBulkInitialData()` returns
   your generators.
3. Override `CreateDataInitializers()` in `TMyWorkloadParams` to return the initializer list.

See `ydb/library/workload/log/log.h` for an example with `TWorkloadDataInitializer`.

## Using the `benchmark_base` Classes

For TPC-style benchmarks that describe tables using YAML and load data from CSV/TSV/PSV files,
inherit from the helper classes in `ydb/library/workload/benchmark_base/`:

- `TWorkloadBaseParams` — extends `TWorkloadParams` with path, store type, S3 options, etc.
- `TWorkloadGeneratorBase` — extends `IWorkloadQueryGenerator`; reads a YAML table definition
  and generates DDL automatically; you only need to implement `GetTablesYaml()`,
  `GetSpecialDataTypes()`, and `GetWorkload()`.
- `TWorkloadDataInitializerBase` — provides a standard file-based data loader.

See `ydb/library/workload/clickbench/` or `ydb/library/workload/tpch/` for complete examples.

## Testing Your Workload

After building, test the lifecycle commands:

```bash
# Build
./ya make ydb/apps/ydb

# Initialize tables
./ydb -e <endpoint> -d <database> workload my-workload init

# Run a 30-second write load with 8 threads
./ydb -e <endpoint> -d <database> workload my-workload run write -s 30 -t 8

# Clean up tables
./ydb -e <endpoint> -d <database> workload my-workload clean
```

Use `--help` on any sub-command to see the available options:

```bash
./ydb workload my-workload init --help
./ydb workload my-workload run --help
```
