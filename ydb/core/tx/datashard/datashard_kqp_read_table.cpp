#include "datashard_kqp_compute.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

namespace {

void ValidateKeyType(const TType* keyType, NScheme::TTypeInfo expectedType) {
    auto type = keyType;

    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    // TODO: support pg types
    MKQL_ENSURE_S(type->GetKind() != TType::EKind::Pg, "pg types are not supported");
    auto dataType = AS_TYPE(TDataType, type)->GetSchemeType();

    MKQL_ENSURE_S(dataType == expectedType.GetTypeId());
}

void ValidateKeyTuple(const TTupleType* tupleType, const NDataShard::TUserTable& tableInfo,
    const TKqpDatashardComputeContext& computeCtx)
{
    MKQL_ENSURE_S(tupleType);
    MKQL_ENSURE_S(tupleType->GetElementsCount() <= tableInfo.KeyColumnIds.size());

    for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
        auto& columnInfo = computeCtx.GetKeyColumnInfo(tableInfo, i);
        ValidateKeyType(tupleType->GetElementType(i), columnInfo.Type);
    }
}

void ValidateRangeBound(const TTupleType* tupleType, const NDataShard::TUserTable& tableInfo,
    const TKqpDatashardComputeContext& computeCtx)
{
    MKQL_ENSURE_S(tupleType);
    MKQL_ENSURE_S(tupleType->GetElementsCount() == tableInfo.KeyColumnIds.size() + 1);

    for (ui32 i = 0; i < tableInfo.KeyColumnIds.size(); ++i) {
        auto& columnInfo = computeCtx.GetKeyColumnInfo(tableInfo, i);
        auto elementType = tupleType->GetElementType(i);
        ValidateKeyType(AS_TYPE(TOptionalType, elementType)->GetItemType(), columnInfo.Type);
    }
}

struct TKeyRangesType {
    TTupleType* From = nullptr;
    TTupleType* To = nullptr;
};

TMaybe<TKeyRangesType> ParseKeyRangesType(const TTupleType* rangeTupleType) {
    MKQL_ENSURE_S(rangeTupleType);
    MKQL_ENSURE_S(rangeTupleType->GetElementsCount() == 1);

    auto rangesType = rangeTupleType->GetElementType(0);
    if (rangesType->GetKind() == TType::EKind::Void) {
        return {};
    }

    auto listType = AS_TYPE(TListType, AS_TYPE(TTupleType, rangesType)->GetElementType(0));
    auto tupleType = AS_TYPE(TTupleType, listType->GetItemType());
    MKQL_ENSURE_S(tupleType->GetElementsCount() == 2);

    auto fromType = AS_TYPE(TTupleType, tupleType->GetElementType(0));
    auto toType = AS_TYPE(TTupleType, tupleType->GetElementType(1));

    return TKeyRangesType {
        .From = fromType,
        .To = toType
    };
}

TSerializedTableRange BuildFullRange(ui32 keyColumnsSize) {
    /* Build range from NULL, ... NULL to +inf, ... +inf */
    TVector<TCell> fromKeyValues(keyColumnsSize);
    return TSerializedTableRange(fromKeyValues, true, TVector<TCell>(), false);
}

TSerializedTableRange BuildRange(const TTupleType* fromType, const NUdf::TUnboxedValue& fromValue,
    const TTupleType* toType, const NUdf::TUnboxedValue& toValue, const TTypeEnvironment& typeEnv, ui32 keyColumnsSize) {

    auto fillTupleCells = [&typeEnv, keyColumnsSize](const auto tupleType, const auto& tupleValue) {
        TVector<TCell> cells;
        cells.reserve(keyColumnsSize);

        for (ui32 i = 0; i < keyColumnsSize; ++i) {
            auto type = tupleType->GetElementType(i);
            auto value = tupleValue.GetElement(i);

            if (type->IsOptional()) {
                if (!value) {
                    return cells;
                }

                type = AS_TYPE(TOptionalType, type)->GetItemType();
                value = value.GetOptionalValue();

                if (type->IsOptional()) {
                    if (!value) {
                        cells.emplace_back(TCell());
                        continue;
                    }

                    type = AS_TYPE(TOptionalType, type)->GetItemType();
                    value = value.GetOptionalValue();
                }
            }

            // TODO: support pg types
            MKQL_ENSURE_S(type->GetKind() != TType::EKind::Pg, "pg types are not supported");
            auto typeInfo = NScheme::TTypeInfo(AS_TYPE(TDataType, type)->GetSchemeType());
            cells.emplace_back(MakeCell(typeInfo, value, typeEnv, /* copy */ true));
        }

        return cells;
    };

    Y_ENSURE(fromType->GetElementsCount() == keyColumnsSize + 1);
    bool fromInclusive = !!fromValue.GetElement(keyColumnsSize).Get<int>();
    auto fromCells = fillTupleCells(fromType, fromValue);

    if (fromCells.empty()) {
        fromInclusive = true;
    }

    if (fromInclusive) {
        while (fromCells.size() != keyColumnsSize) {
            fromCells.emplace_back(TCell());
        }
    }

    Y_ENSURE(toType->GetElementsCount() == keyColumnsSize + 1);
    bool toInclusive = !!toValue.GetElement(keyColumnsSize).Get<int>();
    auto toCells = fillTupleCells(toType, toValue);

    if (!toInclusive && !toCells.empty()) {
        while (toCells.size() != keyColumnsSize) {
            toCells.emplace_back(TCell());
        }
    }

    return TSerializedTableRange(fromCells, fromInclusive, toCells, toInclusive);
}

template <bool IsReverse>
TVector<TSerializedTableRange> CreateTableRanges(const TParseReadTableRangesResult& parseResult,
    const IComputationNode* rangesNode, const TTypeEnvironment& typeEnv, TComputationContext& ctx, ui32 keyColumnsSize)
{
    auto keyRangesType = ParseKeyRangesType(parseResult.Ranges->GetType());
    if (!keyRangesType) {
        return {BuildFullRange(keyColumnsSize)};
    }

    auto list = rangesNode->GetValue(ctx).GetElement(0).GetElement(0);

    TVector<TSerializedTableRange> ranges;
    auto listIt = list.GetListIterator();
    TUnboxedValue tuple;
    while (listIt.Next(tuple)) {
        const auto& from = tuple.GetElement(0);
        const auto& to = tuple.GetElement(1);

        ranges.emplace_back(BuildRange(keyRangesType->From, from, keyRangesType->To, to, typeEnv, keyColumnsSize));
    }

    if constexpr (IsReverse) {
        Reverse(ranges.begin(), ranges.end());
    }

    return ranges;
}

template <bool IsReverse>
class TKqpWideReadTableWrapperBase : public TStatelessWideFlowCodegeneratorNode<TKqpWideReadTableWrapperBase<IsReverse>> {
public:
    TKqpWideReadTableWrapperBase(const TTableId& tableId, TKqpDatashardComputeContext& computeCtx,
            const TSmallVec<TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys)
        : TStatelessWideFlowCodegeneratorNode<TKqpWideReadTableWrapperBase<IsReverse>>(this)
        , TableId(tableId)
        , ComputeCtx(computeCtx)
        , SystemColumnTags(systemColumnTags)
        , SkipNullKeys(skipNullKeys)
        , ShardTableStats(ComputeCtx.GetDatashardCounters())
        , TaskTableStats(ComputeCtx.GetTaskCounters(ComputeCtx.GetCurrentTaskId())) {
    }

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        return this->ReadValue(ctx, output);
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        Y_UNUSED(ctx, block);
        Y_ABORT("LLVM compilation is not implemented for OLTP-workload");
    }
#endif

protected:
    virtual EFetchResult ReadValue(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const = 0;

    EFetchResult ReadNext(NUdf::TUnboxedValue* const* output) const {
        TKqpTableStats stats;
        bool fetched = ComputeCtx.ReadRowWide(TableId, *Iterator, SystemColumnTags, SkipNullKeys, output, stats);

        if (stats.InvisibleRowSkips) {
            ComputeCtx.BreakSetLocks();
        }

        ShardTableStats += stats;
        TaskTableStats += stats;

        if (fetched) {
            if (Remains) {
                Remains = *Remains - 1;
            }

            return EFetchResult::One;
        }

        if (ComputeCtx.IsTabletNotReady() || ComputeCtx.HadInconsistentReads()) {
            return EFetchResult::Yield;
        }

        return EFetchResult::Finish;
    }

protected:
    const TTableId TableId;
    TKqpDatashardComputeContext& ComputeCtx;
    TSmallVec<TTag> SystemColumnTags;
    TSmallVec<bool> SkipNullKeys;
    TKqpTableStats& ShardTableStats;
    TKqpTableStats& TaskTableStats;
    using TTableIterator = std::conditional_t<IsReverse, NTable::TTableReverseIter, NTable::TTableIter>;
    mutable TAutoPtr<TTableIterator> Iterator;
    mutable std::optional<ui64> Remains;
};

template <bool IsReverse>
class TKqpWideReadTableWrapper : public TKqpWideReadTableWrapperBase<IsReverse> {
public:
    TKqpWideReadTableWrapper(TKqpDatashardComputeContext& computeCtx, const TParseReadTableResult& parseResult,
            IComputationNode* fromNode, IComputationNode* toNode, IComputationNode* itemsLimit)
        : TKqpWideReadTableWrapperBase<IsReverse>(parseResult.TableId, computeCtx, parseResult.SystemColumns,
            parseResult.SkipNullKeys)
        , ParseResult(parseResult)
        , FromNode(fromNode)
        , ToNode(toNode)
        , ItemsLimit(itemsLimit)
        , ColumnTags(parseResult.Columns)
    {
        this->ShardTableStats.NSelectRange++;
        this->TaskTableStats.NSelectRange++;
    }

private:
    EFetchResult ReadValue(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const final {
        if (!this->Iterator) {
            TVector<TCell> fromCells;
            BuildKeyTupleCells(ParseResult.FromTuple->GetType(), FromNode->GetValue(ctx), fromCells, ctx.TypeEnv);

            TVector<TCell> toCells;
            BuildKeyTupleCells(ParseResult.ToTuple->GetType(), ToNode->GetValue(ctx), toCells, ctx.TypeEnv);

            auto range = TTableRange(fromCells, ParseResult.FromInclusive, toCells, ParseResult.ToInclusive);

            if (ItemsLimit) {
                this->Remains = ItemsLimit->GetValue(ctx).Get<ui64>();
            }

            if constexpr (IsReverse) {
                this->Iterator = this->ComputeCtx.CreateReverseIterator(ParseResult.TableId, range, ColumnTags);
            } else {
                this->Iterator = this->ComputeCtx.CreateIterator(ParseResult.TableId, range, ColumnTags);
            }
        }

        if (this->Remains && *this->Remains == 0) {
            return EFetchResult::Finish;
        }

        return this->ReadNext(output);
    }

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(FromNode);
        this->FlowDependsOn(ToNode);
        if (ItemsLimit) {
            this->FlowDependsOn(ItemsLimit);
        }
    }

private:
    TParseReadTableResult ParseResult;
    IComputationNode* FromNode;
    IComputationNode* ToNode;
    IComputationNode* ItemsLimit;
    TSmallVec<TTag> ColumnTags;
};

template <bool IsReverse>
class TKqpWideReadTableRangesWrapper : public TKqpWideReadTableWrapperBase<IsReverse> {
public:
    TKqpWideReadTableRangesWrapper(TKqpDatashardComputeContext& computeCtx,
        const TParseReadTableRangesResult& parseResult, IComputationNode* rangesNode, IComputationNode* itemsLimit)
        : TKqpWideReadTableWrapperBase<IsReverse>(parseResult.TableId, computeCtx, parseResult.SystemColumns,
            parseResult.SkipNullKeys)
        , ParseResult(parseResult)
        , RangesNode(rangesNode)
        , ItemsLimit(itemsLimit)
        , ColumnTags(parseResult.Columns) {}

private:
    EFetchResult ReadValue(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const final {
        if (!RangeId) {
            const auto localTid = this->ComputeCtx.GetLocalTableId(ParseResult.TableId);
            const auto* tableInfo = this->ComputeCtx.Database->GetScheme().GetTableInfo(localTid);
            Ranges = CreateTableRanges<IsReverse>(ParseResult, RangesNode, ctx.TypeEnv, ctx, tableInfo->KeyColumns.size());
            RangeId = 0;

            if (ItemsLimit) {
                this->Remains = ItemsLimit->GetValue(ctx).Get<ui64>();
            }
        }

        Y_ENSURE(RangeId);
        while (*RangeId < Ranges.size()) {
            this->ShardTableStats.NSelectRange++;
            this->TaskTableStats.NSelectRange++;

            if (!this->Iterator) {
                auto range = Ranges[*RangeId].ToTableRange();

                if constexpr (IsReverse) {
                    this->Iterator = this->ComputeCtx.CreateReverseIterator(ParseResult.TableId, range, ColumnTags);
                } else {
                    this->Iterator = this->ComputeCtx.CreateIterator(ParseResult.TableId, range, ColumnTags);
                }
            }

            if (this->Remains && *this->Remains == 0) {
                return EFetchResult::Finish;
            }

            auto status = this->ReadNext(output);
            if (status != EFetchResult::Finish) {
                return status;
            }

            this->Iterator.Reset();
            ++(*RangeId);
        }

        return EFetchResult::Finish;
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(RangesNode);
        if (ItemsLimit) {
            this->FlowDependsOn(ItemsLimit);
        }
    }

    TParseReadTableRangesResult ParseResult;
    IComputationNode* RangesNode;
    IComputationNode* ItemsLimit;
    TSmallVec<TTag> ColumnTags;
    mutable TVector<TSerializedTableRange> Ranges;
    mutable std::optional<ui32> RangeId;
};

}

IComputationNode* WrapKqpWideReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTableRanges(callable);
    auto rangesNode = LocateNode(ctx.NodeLocator, *parseResult.Ranges);

    auto tableInfo = computeCtx.GetTable(parseResult.TableId);
    MKQL_ENSURE(tableInfo, "Table not found: " << parseResult.TableId.PathId.ToString());

    auto keyRangesType = ParseKeyRangesType(parseResult.Ranges->GetType());
    if (keyRangesType) {
        ValidateRangeBound(keyRangesType->From, *tableInfo, computeCtx);
        ValidateRangeBound(keyRangesType->To, *tableInfo, computeCtx);
    }

    IComputationNode* itemsLimit = nullptr;
    if (parseResult.ItemsLimit) {
        itemsLimit = LocateNode(ctx.NodeLocator, *parseResult.ItemsLimit);
    }

    if (parseResult.Reverse) {
        return new TKqpWideReadTableRangesWrapper<true>(computeCtx, parseResult, rangesNode, itemsLimit);
    }

    return new TKqpWideReadTableRangesWrapper<false>(computeCtx, parseResult, rangesNode, itemsLimit);
}

IComputationNode* WrapKqpWideReadTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTable(callable);
    auto fromNode = LocateNode(ctx.NodeLocator, *parseResult.FromTuple);
    auto toNode = LocateNode(ctx.NodeLocator, *parseResult.ToTuple);

    auto tableInfo = computeCtx.GetTable(parseResult.TableId);
    MKQL_ENSURE(tableInfo, "Table not found: " << parseResult.TableId.PathId.ToString());

    ValidateKeyTuple(parseResult.FromTuple->GetType(), *tableInfo, computeCtx);
    ValidateKeyTuple(parseResult.ToTuple->GetType(), *tableInfo, computeCtx);

    IComputationNode* itemsLimit = nullptr;
    if (parseResult.ItemsLimit) {
        itemsLimit = LocateNode(ctx.NodeLocator, *parseResult.ItemsLimit);
    }

    if (parseResult.Reverse) {
        return new TKqpWideReadTableWrapper<true>(computeCtx, parseResult, fromNode, toNode, itemsLimit);
    }

    return new TKqpWideReadTableWrapper<false>(computeCtx, parseResult, fromNode, toNode, itemsLimit);
}

} // namespace NMiniKQL
} // namespace NKikimr
