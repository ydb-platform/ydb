#pragma once

// ydb/core/kqp/opt/cbo/kqp_statistics.h
//
// Re-exports NYql::TOptimizerStatistics and related types into the
// NKikimr::NKqp namespace.  The KQP optimizer graph types (IBaseOptimizerNode,
// IProviderContext, EJoinKind, …) are independently owned by NKikimr::NKqp,
// but the statistics type itself remains NYql::TOptimizerStatistics for now.
// Full struct independence is a follow-up step once TOrderingsStateMachine is independent.
//
// TKqpStatsStore provides a KQP-owned per-node statistics map, parallel to
// TTypeAnnotationContext::StatisticsMap, allowing KQP optimizer passes to
// own their stats without modifying the shared YQL annotation context.

#include <yql/essentials/core/yql_statistics.h>

#include <util/generic/hash.h>
#include <memory>

namespace NYql { class TExprNode; }

namespace NKikimr::NKqp {

// Statistics-type family — aliased so that NKikimr::NKqp:: code can refer to
// them without dragging in the NYql:: namespace explicitly.
using EStatisticsType        = NYql::EStatisticsType;
using EStorageType           = NYql::EStorageType;
using IProviderStatistics    = NYql::IProviderStatistics;
using TColumnStatistics      = NYql::TColumnStatistics;
using TShufflingOrderingsByJoinLabels = NYql::TShufflingOrderingsByJoinLabels;
using TOptimizerStatistics   = NYql::TOptimizerStatistics;
using NYql::OverrideStatistics;

// Enum value aliases (unscoped enums, need explicit import)
using NYql::BaseTable;
using NYql::FilteredFactTable;
using NYql::ManyManyJoin;
using NYql::NA;
using NYql::RowStorage;
using NYql::ColumnStorage;

/**
 * TKqpStatsStore — KQP-owned per-node statistics map.
 *
 * Stores optimizer statistics for expression nodes, keyed by UniqueId().
 * This is a parallel store to TTypeAnnotationContext::StatisticsMap, allowing
 * KQP to own its stats without touching the shared YQL annotation context.
 *
 * Interface mirrors TTypeAnnotationContext::GetStats / SetStats so that
 * call-site changes are mechanical.
 */
class TKqpStatsStore {
    THashMap<ui64, std::shared_ptr<TOptimizerStatistics>> Map_;
public:
    bool ContainsStats(const NYql::TExprNode* input) const;
    std::shared_ptr<TOptimizerStatistics> GetStats(const NYql::TExprNode* input) const;
    void SetStats(const NYql::TExprNode* input, std::shared_ptr<TOptimizerStatistics> stats);
};

} // namespace NKikimr::NKqp
