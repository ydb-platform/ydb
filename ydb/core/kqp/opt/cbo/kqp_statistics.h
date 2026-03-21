#pragma once

// ydb/core/kqp/opt/cbo/kqp_statistics.h
//
// KQP-owned independent copy of TOptimizerStatistics and related types.
// The YQL copy lives in yql/essentials/core/yql_statistics.h and is unchanged.
// Different namespaces (NKikimr::NKqp vs NYql) avoid ODR conflicts.
//
// TKqpStatsStore provides a KQP-owned per-node statistics map, parallel to
// TTypeAnnotationContext::StatisticsMap, allowing KQP optimizer passes to
// own their stats without modifying the shared YQL annotation context.

#include <yql/essentials/core/yql_statistics.h>
#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>

#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <iostream>

namespace NYql { class TExprNode; }

namespace NKikimr::NKqp {

// Statistics-type enums — aliased (same underlying type, no ambiguity)
using EStatisticsType = NYql::EStatisticsType;
using EStorageType    = NYql::EStorageType;

// TShufflingOrderingsByJoinLabels — stays as alias (FSM independence is next step)
using TShufflingOrderingsByJoinLabels = NYql::TShufflingOrderingsByJoinLabels;

// Enum value aliases (unscoped enums need explicit import)
using NYql::BaseTable;
using NYql::FilteredFactTable;
using NYql::ManyManyJoin;
using NYql::NA;
using NYql::RowStorage;
using NYql::ColumnStorage;

// Join-column type alias (used in TOptimizerStatistics nested structs)
using TJoinColumn = NYql::NDq::TJoinColumn;

// -------------------------------------------------------------------------
// IProviderStatistics — alias (empty virtual base, shared with NYql to allow
// derived types like TS3ProviderStatistics to be stored without conversion)
// -------------------------------------------------------------------------
using IProviderStatistics = NYql::IProviderStatistics;

// -------------------------------------------------------------------------
// TColumnStatistics — alias (column stat data crosses YQL/KQP boundary via
// TypeAnnotationContext, so must be the same type to allow direct assignment)
// -------------------------------------------------------------------------
using TColumnStatistics = NYql::TColumnStatistics;

// -------------------------------------------------------------------------
// TOptimizerStatistics — independent copy
// -------------------------------------------------------------------------
struct TOptimizerStatistics {
    struct TKeyColumns : public TSimpleRefCount<TKeyColumns> {
        TVector<TString> Data;
        explicit TKeyColumns(TVector<TString> data) : Data(std::move(data)) {}

        TVector<NYql::NDq::TJoinColumn> ToJoinColumns(const TString& alias) {
            TVector<NYql::NDq::TJoinColumn> columns;
            columns.reserve(Data.size());
            for (const auto& column : Data) {
                columns.push_back(NYql::NDq::TJoinColumn(alias, column));
            }
            return columns;
        }
    };

    struct TSortColumns : public TSimpleRefCount<TSortColumns> {
        TVector<TString> Columns;
        TVector<TString> Aliases;

        TSortColumns(const TVector<TString>& cols, const TVector<TString>& aliases)
            : Columns(cols)
            , Aliases(aliases)
        {}
    };

    // TColumnStatMap — alias so TIntrusivePtr<TColumnStatMap> is assignment-compatible
    // with TypeAnnotationContext::ColumnStatisticsByTableName entries
    using TColumnStatMap = NYql::TOptimizerStatistics::TColumnStatMap;

    struct TShuffledByColumns : public TSimpleRefCount<TShuffledByColumns> {
        TVector<NYql::NDq::TJoinColumn> Data;
        explicit TShuffledByColumns(TVector<NYql::NDq::TJoinColumn> data) : Data(std::move(data)) {}
        TString ToString() {
            TString result;
            for (const auto& column : Data) {
                result.append(column.RelName).append(".").append(column.AttributeName).append(", ");
            }
            if (!result.empty()) {
                result.pop_back();
                result.pop_back();
            }
            return result;
        }
    };

    EStatisticsType Type = BaseTable;
    double Nrows = 0;
    int Ncols = 0;
    double ByteSize = 0;
    double Cost = 0;
    double Selectivity = 1.0;
    TIntrusivePtr<TKeyColumns> KeyColumns;
    TIntrusivePtr<TColumnStatMap> ColumnStatistics;

    TIntrusivePtr<TShuffledByColumns> ShuffledByColumns;

    TIntrusivePtr<TSortColumns> SortColumns;
    EStorageType StorageType = EStorageType::NA;
    std::shared_ptr<IProviderStatistics> Specific;
    std::shared_ptr<TVector<TString>> Labels = {};

    TString SourceTableName;
    TSimpleSharedPtr<THashSet<TString>> Aliases;
    TIntrusivePtr<NYql::NDq::TTableAliasMap> TableAliases;

    NYql::NDq::TOrderingsStateMachine::TLogicalOrderings LogicalOrderings;
    NYql::NDq::TOrderingsStateMachine::TLogicalOrderings SortingOrderings;
    NYql::NDq::TOrderingsStateMachine::TLogicalOrderings ReversedSortingOrderings;

    std::optional<std::size_t> ShuffleOrderingIdx;
    std::int64_t SortingOrderingIdx = -1;
    std::int64_t ShufflingOrderingIdx = -1;

    bool CBOFired = false;

    TOptimizerStatistics(TOptimizerStatistics&&) = default;
    TOptimizerStatistics& operator=(TOptimizerStatistics&&) = default;
    TOptimizerStatistics(const TOptimizerStatistics&) = default;
    TOptimizerStatistics& operator=(const TOptimizerStatistics&) = default;
    TOptimizerStatistics() = default;

    explicit TOptimizerStatistics(
        EStatisticsType type,
        double nrows = 0.0,
        int ncols = 0,
        double byteSize = 0.0,
        double cost = 0.0,
        TIntrusivePtr<TKeyColumns> keyColumns = {},
        TIntrusivePtr<TColumnStatMap> columnMap = {},
        EStorageType storageType = EStorageType::NA,
        std::shared_ptr<IProviderStatistics> specific = nullptr
    );

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);

    TString ToString() const;
};

std::shared_ptr<TOptimizerStatistics> OverrideStatistics(
    const TOptimizerStatistics& s,
    const TStringBuf& tablePath,
    const std::shared_ptr<NJson::TJsonValue>& stats);

// -------------------------------------------------------------------------
// TKqpStatsStore — KQP-owned per-node statistics map.
//
// Stores optimizer statistics for expression nodes, keyed by UniqueId().
// This is a parallel store to TTypeAnnotationContext::StatisticsMap, allowing
// KQP to own its stats without touching the shared YQL annotation context.
//
// Interface mirrors TTypeAnnotationContext::GetStats / SetStats so that
// call-site changes are mechanical.
// -------------------------------------------------------------------------
class TKqpStatsStore {
    THashMap<ui64, std::shared_ptr<TOptimizerStatistics>> Map_;
public:
    bool ContainsStats(const NYql::TExprNode* input) const;
    std::shared_ptr<TOptimizerStatistics> GetStats(const NYql::TExprNode* input) const;
    void SetStats(const NYql::TExprNode* input, std::shared_ptr<TOptimizerStatistics> stats);
};

} // namespace NKikimr::NKqp
