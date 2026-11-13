#pragma once

#include <util/generic/string.h>

#include <optional>

namespace NYdbWorkload::NScheme {

enum class EObjectType { Table, Topic };

struct TGroupConfig {
    ui32 Count = 0;
    ui32 InitialCount = 0;
    ui32 Partitions = 0;
    ui32 AlterBatchSize = 1;
};

struct TObject {
    EObjectType Type;
    TString Path;
    ui32 Partitions;
    ui32 AlterBatchSize;
};

struct TConfig {
    TString Path;
    ui32 CreateConcurrency = 1;
    ui32 AlterConcurrency = 1;
    std::optional<TGroupConfig> Tables;
    std::optional<TGroupConfig> Topics;

    ui64 ObjectCount() const;
    ui64 InitialObjectCount() const;
    TObject Object(ui64 index) const;
    TObject InitialObject(ui64 index) const;
};

TConfig ParseConfig(const TString& yaml);

} // namespace NYdbWorkload::NScheme
