#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <optional>
#include <variant>
#include <vector>

namespace NKikimr::NColumnShard {

struct TStepColumn {
    ui32 Id;
    TString Name;

    bool operator==(const TStepColumn&) const = default;
};

struct TReserveMemoryStepDetails {
    struct TStage {
        ui32 Stage;
        std::vector<TString> MemoryTypes;
    };

    std::vector<TStage> Stages;
    std::optional<ui64> RequestedBytes;

    void Merge(const TReserveMemoryStepDetails& other);
};

struct TFetchColumnsStepDetails {
    std::vector<TStepColumn> Columns;

    void Merge(const TFetchColumnsStepDetails& other);
};

struct TAssembleStepDetails {
    std::vector<TStepColumn> Columns;

    void Merge(const TAssembleStepDetails& other);
};

struct TPKFilterDetails {
    std::vector<TStepColumn> Columns;
    std::vector<TString> Predicates;

    void Merge(const TPKFilterDetails& other);
};

struct TShardingFilterStepDetails {
    std::vector<TStepColumn> Columns;

    void Merge(const TShardingFilterStepDetails& other);
};

struct TDeletionFilterStepDetails {
    std::vector<TStepColumn> Columns;

    void Merge(const TDeletionFilterStepDetails& other);
};

struct TSnapshotFilterStepDetails {
    struct TSnapshot {
        ui64 PlanStep;
        ui64 TxId;

        bool operator==(const TSnapshot&) const = default;
    };

    std::vector<TSnapshot> Snapshots;

    void Merge(const TSnapshotFilterStepDetails& other);
};

struct TLimitStepDetails {
    struct TLimit {
        ui64 Limit;
        bool Reverse;

        bool operator==(const TLimit&) const = default;
    };

    std::vector<TLimit> Limits;

    void Merge(const TLimitStepDetails& other);
};

struct TPrepareResultStepDetails {
    std::vector<bool> StartResultBuildingInplace;

    void Merge(const TPrepareResultStepDetails& other);
};

struct TBuildResultStepDetails {
    struct TPageBounds {
        ui64 MinStartIndex;
        ui64 MaxStartIndex;
        ui64 MinRecordsCount;
        ui64 MaxRecordsCount;
    };

    std::optional<TPageBounds> PageBounds;

    void Merge(const TBuildResultStepDetails& other);
};

using TStepDetails = std::variant<std::monostate, TReserveMemoryStepDetails, TFetchColumnsStepDetails, TAssembleStepDetails,
    TPKFilterDetails, TShardingFilterStepDetails, TDeletionFilterStepDetails, TSnapshotFilterStepDetails, TLimitStepDetails,
    TPrepareResultStepDetails, TBuildResultStepDetails>;

struct TStepDescription {
    TString Name;
    TStepDetails Details = {};
};

}   // namespace NKikimr::NColumnShard
