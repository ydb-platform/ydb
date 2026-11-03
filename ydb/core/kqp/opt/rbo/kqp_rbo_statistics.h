#pragma once

#include "kqp_info_unit.h"
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;
class TColumnLineage;
struct TColumnLineageEntry;

enum ELogicalCardinality: ui32 {
    ZeroOrMore,
    Zero,
    ZeroOrOne,
    One,
    OneOrMore
};

class TRBOMetadata {
public:
    EStatisticsType Type = EStatisticsType::BaseTable;
    EStorageType StorageType = EStorageType::NA;
    ELogicalCardinality LogicalCard = ELogicalCardinality::ZeroOrMore;

    // Keep source-key order for ordered consumers; membership uses Unordered().
    TOrderedIUs<> KeyColumns;
    // Output columns allowed to reuse source-table column statistics.
    TUnorderedIUs SourceStatsColumns;
    // Hint relation at this boundary. Aggregate replaces the relation even for
    // grouping keys whose ID and global value provenance remain unchanged.
    TMappedIUs<ui32> HintRelations;
    ui32 ColumnsCount = 0;
    // This is a descriptive fact: "this node's rows are physically partitioned by these columns".
    // The per-side *requirement* ("shuffle this input by these keys for the parent join") is
    // NOT stored here — it lives on TJoinOptimizerNode. It is propagated through renames, projections,
    // joins and such. When it reaches leafs of a CBO Tree it's used to set the initial orderings.
    TOrderedIUs<> ShuffledByColumns;

    TString ToString(ui32 printOptions, const TInfoUnitRegistry& registry);
};

class TRBOStatistics {
public:
    double ERows = 0;
    double EBytes = 0;
    double Selectivity = 1.0;

    TString ToString(ui32 printOptions);
};

TOptimizerStatistics BuildOptimizerStatistics(IOperator& op, const TColumnLineage& lineage, bool withStatsAndCosts, const NYql::TTypeAnnotationContext& typeCtx);
const TColumnLineageEntry* FindSourceStatistics(const IOperator& op, TInfoUnitId id, const TColumnLineage& lineage);

}
}
