#pragma once

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

void BuildKeyTupleCells(const TTupleType* tupleType, const NUdf::TUnboxedValue& tupleValue,
    TVector<TCell>& cells, const TTypeEnvironment& env);

struct TParseReadTableResultBase {
    ui32 CallableId = 0;
    TTableId TableId;

    TSmallVec<NTable::TTag> Columns;
    TSmallVec<NTable::TTag> SystemColumns;
    TSmallVec<bool> SkipNullKeys;
    TNode* ItemsLimit = nullptr;
    bool Reverse = false;
};

struct TParseReadTableResult : TParseReadTableResultBase {
    TTupleLiteral* FromTuple = nullptr;
    bool FromInclusive = false;
    TTupleLiteral* ToTuple = nullptr;
    bool ToInclusive = false;
};

struct TParseReadTableRangesResult : TParseReadTableResultBase {
    TTupleLiteral* Ranges = nullptr;
};

void ParseReadColumns(const TType* readType, const TRuntimeNode& tagsNode,
    TSmallVec<NTable::TTag>& columns, TSmallVec<NTable::TTag>& systemColumns);

TParseReadTableResult ParseWideReadTable(TCallable& callable);
TParseReadTableRangesResult ParseWideReadTableRanges(TCallable& callable);

IComputationNode* WrapKqpScanWideReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx);
IComputationNode* WrapKqpScanWideReadTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx);

IComputationNode* WrapKqpScanBlockReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx);

} // namespace NMiniKQL
} // namespace NKikimr

