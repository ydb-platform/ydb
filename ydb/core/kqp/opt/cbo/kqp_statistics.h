#pragma once

// ydb/core/kqp/opt/cbo/kqp_statistics.h
//
// Re-exports NYql::TOptimizerStatistics and related types into the
// NKikimr::NKqp namespace.  The KQP optimizer graph types (IBaseOptimizerNode,
// IProviderContext, EJoinKind, …) are independently owned by NKikimr::NKqp,
// but the statistics type itself remains NYql::TOptimizerStatistics for now,
// because it is stored inside NYql::TTypeAnnotationContext which is shared
// between KQP and the YQL optimizer infrastructure.
//
// When KQP eventually gains its own statistics-annotation context the alias
// can be replaced with a proper independent definition.

#include <yql/essentials/core/yql_statistics.h>

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

} // namespace NKikimr::NKqp
