#pragma once

#include <util/generic/string.h>

#include <optional>

namespace NYdbWorkload::NScheme {

inline constexpr ui32 MAX_IN_FLIGHT_OPERATIONS = 256;

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
    ui32 DescribeConcurrency = 0;
    ui32 ListConcurrency = 0;
    std::optional<TGroupConfig> Tables;
    std::optional<TGroupConfig> Topics;

    ui64 TotalConcurrency() const;
    ui64 ObjectCount() const;
    ui64 InitialObjectCount() const;
    TObject Object(ui64 index) const;
    TObject InitialObject(ui64 index) const;
};

// Only explicitly supplied CLI options override YAML fields.
struct TConfigOverrides {
    std::optional<TString> Path;
    std::optional<TString> Tables;
    std::optional<TString> Topics;
    std::optional<TString> Partitions;
    std::optional<TString> Operations;
};

TConfig ParseConfig(const TString& yaml, const TConfigOverrides& overrides = {});

} // namespace NYdbWorkload::NScheme
