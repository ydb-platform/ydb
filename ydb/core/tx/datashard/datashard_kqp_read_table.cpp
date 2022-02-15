#include "datashard_kqp_compute.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/util/yverify_stream.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

namespace {

void ValidateKeyType(const TType* keyType, const std::pair<NScheme::TTypeId, TString>& keyColumn) {
    auto type = keyType;

    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    auto dataType = AS_TYPE(TDataType, type)->GetSchemeType();

    MKQL_ENSURE_S(dataType == keyColumn.first);
}

void ValidateKeyTuple(const TTupleType* tupleType, const TVector<std::pair<NScheme::TTypeId, TString>>& keyColumns) {
    MKQL_ENSURE_S(tupleType);
    MKQL_ENSURE_S(tupleType->GetElementsCount() <= keyColumns.size());

    for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
        ValidateKeyType(tupleType->GetElementType(i), keyColumns[i]);
    }
}

void ValidateRangeBound(const TTupleType* tupleType, const TVector<std::pair<NScheme::TTypeId, TString>>& keyColumns) {
    MKQL_ENSURE_S(tupleType);
    MKQL_ENSURE_S(tupleType->GetElementsCount() == keyColumns.size() + 1);

    for (ui32 i = 0; i < keyColumns.size(); ++i) {
        auto elementType = tupleType->GetElementType(i);
        ValidateKeyType(AS_TYPE(TOptionalType, elementType)->GetItemType(), keyColumns[i]);
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

TSerializedTableRange CreateTableRange(const TParseReadTableResult& parseResult, const IComputationNode* fromNode,
    const IComputationNode* toNode, const TTypeEnvironment& typeEnv, TComputationContext& ctx)
{
    TVector<TCell> fromCells;
    BuildKeyTupleCells(parseResult.FromTuple->GetType(), fromNode->GetValue(ctx), fromCells, typeEnv);

    TVector<TCell> toCells;
    BuildKeyTupleCells(parseResult.ToTuple->GetType(), toNode->GetValue(ctx), toCells, typeEnv);

    return TSerializedTableRange(fromCells, parseResult.FromInclusive, toCells, parseResult.ToInclusive);
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

            cells.emplace_back(MakeCell(AS_TYPE(TDataType, type)->GetSchemeType(), value, typeEnv, /* copy */ true));
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

void CreateRangePoints(ui64 localTid, const TSerializedTableRange& serializedTableRange, TSmallVec<TRawTypeValue>& from,
    TSmallVec<TRawTypeValue>& to, TKqpDatashardComputeContext& computeCtx)
{
    const auto* tableInfo = computeCtx.Database->GetScheme().GetTableInfo(localTid);
    auto tableRange = serializedTableRange.ToTableRange();
    ConvertTableKeys(computeCtx.Database->GetScheme(), tableInfo, tableRange.From, from, nullptr);
    ConvertTableKeys(computeCtx.Database->GetScheme(), tableInfo, tableRange.To, to, nullptr);
}

template <bool IsReverse>
class TKqpWideReadTableWrapperBase : public TStatelessWideFlowCodegeneratorNode<TKqpWideReadTableWrapperBase<IsReverse>> {
public:
    TKqpWideReadTableWrapperBase(const TTableId& tableId, TKqpDatashardComputeContext& computeCtx,
        const TTypeEnvironment& typeEnv, const TSmallVec<TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys)
        : TStatelessWideFlowCodegeneratorNode<TKqpWideReadTableWrapperBase<IsReverse>>(this)
        , TableId(tableId)
        , ComputeCtx(computeCtx)
        , TypeEnv(typeEnv)
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
        Y_FAIL("LLVM compilation is not implemented for OLTP-workload");
    }
#endif

protected:
    virtual EFetchResult ReadValue(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const = 0;

    EFetchResult ReadNext(NUdf::TUnboxedValue* const* output) const {
        bool breakLocks = false;
        while (Iterator->Next(NTable::ENext::Data) == NTable::EReady::Data) {
            if (!breakLocks && (breakLocks = bool(Iterator->Stats.InvisibleRowSkips))) {
                ComputeCtx.BreakSetLocks();
            }
            TDbTupleRef rowKey = Iterator->GetKey();

            ComputeCtx.AddKeyAccessSample(TableId, rowKey.Cells());

            ui64 deletedRowSkips = std::exchange(Iterator->Stats.DeletedRowSkips, 0);
            ui64 invisibleRowSkips = std::exchange(Iterator->Stats.InvisibleRowSkips, 0);

            ShardTableStats.SelectRangeDeletedRowSkips += deletedRowSkips;
            ShardTableStats.InvisibleRowSkips +=  invisibleRowSkips;

            TaskTableStats.SelectRangeDeletedRowSkips += deletedRowSkips;
            TaskTableStats.InvisibleRowSkips +=  invisibleRowSkips;

            Y_VERIFY(SkipNullKeys.size() <= rowKey.ColumnCount);
            bool skipRow = false;
            for (ui32 i = 0; i < SkipNullKeys.size(); ++i) {
                if (SkipNullKeys[i] && rowKey.Columns[i].IsNull()) {
                    skipRow = true;
                    break;
                }
            }
            if (skipRow) {
                continue;
            }

            TDbTupleRef rowValues = Iterator->GetValues();

            size_t columnsCount = rowValues.ColumnCount + SystemColumnTags.size();

            ui64 rowSize = 0;
            for (ui32 i = 0; i < rowValues.ColumnCount; ++i) {
                rowSize += rowValues.Columns[i].IsNull() ? 1 : rowValues.Columns[i].Size();
                if (auto out = *output++) {
                    *out = GetCellValue(rowValues.Cells()[i], rowValues.Types[i]);
                }
            }

            // Some per-row overhead to deal with the case when no columns were requested
            rowSize = std::max(rowSize, (ui64) 8);

            for (ui32 i = rowValues.ColumnCount, j = 0; i < columnsCount; ++i, ++j) {
                auto out = *output++;
                if (!out) {
                    continue;
                }

                switch (SystemColumnTags[j]) {
                    case TKeyDesc::EColumnIdDataShard:
                        *out = TUnboxedValue(TUnboxedValuePod(ComputeCtx.GetShardId()));
                        break;
                    default:
                        throw TSchemeErrorTabletException();
                }
            }

            if (Remains) {
                Remains = *Remains - 1;
            }

            ShardTableStats.SelectRangeRows++;
            ShardTableStats.SelectRangeBytes += rowSize;

            TaskTableStats.SelectRangeRows++;
            TaskTableStats.SelectRangeBytes += rowSize;

            return EFetchResult::One;
        }

        if (!breakLocks && bool(Iterator->Stats.InvisibleRowSkips)) {
            ComputeCtx.BreakSetLocks();
        }

        auto deletedRowSkips = std::exchange(Iterator->Stats.DeletedRowSkips, 0);
        auto invisibleRowSkips = std::exchange(Iterator->Stats.InvisibleRowSkips, 0);

        ShardTableStats.SelectRangeDeletedRowSkips += deletedRowSkips;
        ShardTableStats.InvisibleRowSkips += invisibleRowSkips;

        TaskTableStats.SelectRangeDeletedRowSkips += deletedRowSkips;
        TaskTableStats.InvisibleRowSkips += invisibleRowSkips;

        if (Iterator->Last() == NTable::EReady::Page) {
            ComputeCtx.SetTabletNotReady();
            return EFetchResult::Yield;
        }

        return EFetchResult::Finish;
    }

protected:
    const TTableId TableId;
    TKqpDatashardComputeContext& ComputeCtx;
    const TTypeEnvironment& TypeEnv;
    TSmallVec<TTag> SystemColumnTags;
    TSmallVec<bool> SkipNullKeys;
    TKqpTableStats& ShardTableStats;
    TKqpTableStats& TaskTableStats;
    using TTableIterator = std::conditional_t<IsReverse, NTable::TTableReverseIt, NTable::TTableIt>;
    mutable TAutoPtr<TTableIterator> Iterator;
    mutable std::optional<ui64> Remains;
};

template <bool IsReverse>
class TKqpWideReadTableWrapper : public TKqpWideReadTableWrapperBase<IsReverse> {
public:
    TKqpWideReadTableWrapper(TKqpDatashardComputeContext& computeCtx, const TTypeEnvironment& typeEnv,
        const TParseReadTableResult& parseResult, IComputationNode* fromNode, IComputationNode* toNode,
        IComputationNode* itemsLimit)
        : TKqpWideReadTableWrapperBase<IsReverse>(parseResult.TableId, computeCtx, typeEnv,
            ExtractTags(parseResult.SystemColumns), parseResult.SkipNullKeys)
        , ParseResult(parseResult)
        , FromNode(fromNode)
        , ToNode(toNode)
        , ItemsLimit(itemsLimit)
        , LocalTid(computeCtx.GetLocalTableId(parseResult.TableId))
        , ColumnTags(ExtractTags(parseResult.Columns))
    {
        this->ShardTableStats.NSelectRange++;
        this->TaskTableStats.NSelectRange++;
    }

private:
    EFetchResult ReadValue(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const final {
        if (!this->Iterator) {
            auto serializedTableRange = CreateTableRange(ParseResult, FromNode, ToNode, this->TypeEnv, ctx);
            auto tableRange = serializedTableRange.ToTableRange();
            this->ComputeCtx.ReadTable(ParseResult.TableId, tableRange);

            TSmallVec<TRawTypeValue> from, to;
            CreateRangePoints(LocalTid, serializedTableRange, from, to, this->ComputeCtx);

            NTable::TKeyRange keyRange;
            keyRange.MinKey = from;
            keyRange.MaxKey = to;
            keyRange.MinInclusive = tableRange.InclusiveFrom;
            keyRange.MaxInclusive = tableRange.InclusiveTo;

            if (ItemsLimit) {
                this->Remains = ItemsLimit->GetValue(ctx).Get<ui64>();
            }

            if constexpr (IsReverse) {
                this->Iterator = this->ComputeCtx.Database->IterateRangeReverse(LocalTid, keyRange, ColumnTags, this->ComputeCtx.GetReadVersion());
            } else {
                this->Iterator = this->ComputeCtx.Database->IterateRange(LocalTid, keyRange, ColumnTags, this->ComputeCtx.GetReadVersion());
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
    ui64 LocalTid;
    TSmallVec<TTag> ColumnTags;
    ui64 TaskId;
};

template <bool IsReverse>
class TKqpWideReadTableRangesWrapper : public TKqpWideReadTableWrapperBase<IsReverse> {
public:
    TKqpWideReadTableRangesWrapper(TKqpDatashardComputeContext& computeCtx, const TTypeEnvironment& typeEnv,
        const TParseReadTableRangesResult& parseResult, IComputationNode* rangesNode, IComputationNode* itemsLimit)
        : TKqpWideReadTableWrapperBase<IsReverse>(parseResult.TableId, computeCtx, typeEnv,
            ExtractTags(parseResult.SystemColumns), parseResult.SkipNullKeys)
        , ParseResult(parseResult)
        , RangesNode(rangesNode)
        , ItemsLimit(itemsLimit)
        , LocalTid(computeCtx.GetLocalTableId(parseResult.TableId))
        , ColumnTags(ExtractTags(parseResult.Columns)) {
    }

private:
    EFetchResult ReadValue(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const final {
        if (!RangeId) {
            const auto* tableInfo = this->ComputeCtx.Database->GetScheme().GetTableInfo(LocalTid);
            Ranges = CreateTableRanges<IsReverse>(ParseResult, RangesNode, this->TypeEnv, ctx, tableInfo->KeyColumns.size());
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
                auto& range = Ranges[*RangeId];
                auto tableRange = range.ToTableRange();
                this->ComputeCtx.ReadTable(ParseResult.TableId, tableRange);

                TSmallVec<TRawTypeValue> from, to;
                CreateRangePoints(LocalTid, range, from, to, this->ComputeCtx);

                NTable::TKeyRange keyRange;
                keyRange.MinKey = from;
                keyRange.MaxKey = to;
                keyRange.MinInclusive = tableRange.InclusiveFrom;
                keyRange.MaxInclusive = tableRange.InclusiveTo;

                if constexpr (IsReverse) {
                    this->Iterator = this->ComputeCtx.Database->IterateRangeReverse(LocalTid, keyRange, ColumnTags, this->ComputeCtx.GetReadVersion());
                } else {
                    this->Iterator = this->ComputeCtx.Database->IterateRange(LocalTid, keyRange, ColumnTags, this->ComputeCtx.GetReadVersion());
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
    ui64 LocalTid;
    TSmallVec<TTag> ColumnTags;
    ui64 TaskId;
    mutable TVector<TSerializedTableRange> Ranges;
    mutable std::optional<ui32> RangeId;
};

void FetchRowImpl(const TDbTupleRef& dbTuple, TUnboxedValue& row, TComputationContext& ctx, TKqpTableStats& tableStats,
    const TKqpDatashardComputeContext& computeCtx, const TSmallVec<TTag>& systemColumnTags)
{
    size_t columnsCount = dbTuple.ColumnCount + systemColumnTags.size();

    TUnboxedValue* rowItems = nullptr;
    row = ctx.HolderFactory.CreateDirectArrayHolder(columnsCount, rowItems);

    ui64 rowSize = 0;
    for (ui32 i = 0; i < dbTuple.ColumnCount; ++i) {
        rowSize += dbTuple.Columns[i].IsNull() ? 1 : dbTuple.Columns[i].Size();
        rowItems[i] = GetCellValue(dbTuple.Cells()[i], dbTuple.Types[i]);
    }

    // Some per-row overhead to deal with the case when no columns were requested
    rowSize = std::max(rowSize, (ui64)8);

    for (ui32 i = dbTuple.ColumnCount, j = 0; i < columnsCount; ++i, ++j) {
        switch (systemColumnTags[j]) {
            case TKeyDesc::EColumnIdDataShard:
                rowItems[i] = TUnboxedValue(TUnboxedValuePod(computeCtx.GetShardId()));
                break;
            default:
                throw TSchemeErrorTabletException();
        }
    }

    tableStats.NSelectRow++;
    tableStats.SelectRowRows++;
    tableStats.SelectRowBytes += rowSize;
}

template <typename TTableIterator>
bool TryFetchRowImpl(const TTableId& tableId, TTableIterator& iterator, TUnboxedValue& row, TComputationContext& ctx,
    TKqpTableStats& tableStats, TKqpDatashardComputeContext& computeCtx, const TSmallVec<TTag>& systemColumnTags,
    const TSmallVec<bool>& skipNullKeys)
{
    while (iterator.Next(NTable::ENext::Data) == NTable::EReady::Data) {
        TDbTupleRef rowKey = iterator.GetKey();
        computeCtx.AddKeyAccessSample(tableId, rowKey.Cells());

        Y_VERIFY(skipNullKeys.size() <= rowKey.ColumnCount);
        bool skipRow = false;
        for (ui32 i = 0; i < skipNullKeys.size(); ++i) {
            if (skipNullKeys[i] && rowKey.Columns[i].IsNull()) {
                skipRow = true;
                break;
            }
        }

        if (skipRow) {
            continue;
        }

        TDbTupleRef rowValues = iterator.GetValues();
        FetchRowImpl(rowValues, row, ctx, tableStats, computeCtx, systemColumnTags);
        return true;
    }

    return false;
}

} // namespace

bool TryFetchRow(const TTableId& tableId, TTableIt& iterator, TUnboxedValue& row, TComputationContext& ctx,
    TKqpTableStats& tableStats, TKqpDatashardComputeContext& computeCtx, const TSmallVec<TTag>& systemColumnTags,
    const TSmallVec<bool>& skipNullKeys)
{
    return TryFetchRowImpl(tableId, iterator, row, ctx, tableStats, computeCtx, systemColumnTags, skipNullKeys);
}

bool TryFetchRow(const TTableId& tableId, TTableReverseIt& iterator, TUnboxedValue& row, TComputationContext& ctx,
    TKqpTableStats& tableStats, TKqpDatashardComputeContext& computeCtx, const TSmallVec<TTag>& systemColumnTags,
    const TSmallVec<bool>& skipNullKeys)
{
    return TryFetchRowImpl(tableId, iterator, row, ctx, tableStats, computeCtx, systemColumnTags, skipNullKeys);
}

void FetchRow(const TDbTupleRef& dbTuple, TUnboxedValue& row, TComputationContext& ctx, TKqpTableStats& tableStats,
    const TKqpDatashardComputeContext& computeCtx, const TSmallVec<TTag>& systemColumnTags)
{
    return FetchRowImpl(dbTuple, row, ctx, tableStats, computeCtx, systemColumnTags);
}

IComputationNode* WrapKqpWideReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTableRanges(callable);
    auto rangesNode = LocateNode(ctx.NodeLocator, *parseResult.Ranges);

    auto keyColumns = computeCtx.GetKeyColumnsInfo(parseResult.TableId);
    auto keyRangesType = ParseKeyRangesType(parseResult.Ranges->GetType());
    if (keyRangesType) {
        ValidateRangeBound(keyRangesType->From, keyColumns);
        ValidateRangeBound(keyRangesType->To, keyColumns);
    }

    IComputationNode* itemsLimit = nullptr;
    if (parseResult.ItemsLimit) {
        itemsLimit = LocateNode(ctx.NodeLocator, *parseResult.ItemsLimit);
    }

    if (parseResult.Reverse) {
        return new TKqpWideReadTableRangesWrapper<true>(computeCtx, ctx.Env, parseResult, rangesNode, itemsLimit);
    }

    return new TKqpWideReadTableRangesWrapper<false>(computeCtx, ctx.Env, parseResult, rangesNode, itemsLimit);
}

IComputationNode* WrapKqpWideReadTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTable(callable);
    auto fromNode = LocateNode(ctx.NodeLocator, *parseResult.FromTuple);
    auto toNode = LocateNode(ctx.NodeLocator, *parseResult.ToTuple);

    auto keyColumns = computeCtx.GetKeyColumnsInfo(parseResult.TableId);
    ValidateKeyTuple(parseResult.FromTuple->GetType(), keyColumns);
    ValidateKeyTuple(parseResult.ToTuple->GetType(), keyColumns);

    IComputationNode* itemsLimit = nullptr;
    if (parseResult.ItemsLimit) {
        itemsLimit = LocateNode(ctx.NodeLocator, *parseResult.ItemsLimit);
    }

    if (parseResult.Reverse) {
        return new TKqpWideReadTableWrapper<true>(computeCtx, ctx.Env, parseResult, fromNode, toNode, itemsLimit);
    }

    return new TKqpWideReadTableWrapper<false>(computeCtx, ctx.Env, parseResult, fromNode, toNode, itemsLimit);
}

} // namespace NMiniKQL
} // namespace NKikimr
