#include "datashard_kqp_compute.h"

#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/kqp/runtime/kqp_runtime_impl.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

namespace {

struct TParseLookupTableResult {
    ui32 CallableId = 0;
    TTableId TableId;
    TRuntimeNode LookupKeys;
    TVector<ui32> KeyIndices;
    TVector<NUdf::TDataTypeId> KeyTypes;

    TSmallVec<TKqpComputeContextBase::TColumn> Columns;
    TSmallVec<TKqpComputeContextBase::TColumn> SystemColumns;
    TSmallVec<bool> SkipNullKeys;
};

void ValidateLookupKeys(const TType* inputType, const THashMap<TString, NScheme::TTypeId>& keyColumns) {
    MKQL_ENSURE_S(inputType);
    auto rowType = AS_TYPE(TStructType, AS_TYPE(TStreamType, inputType)->GetItemType());

    for (ui32 i = 0; i < rowType->GetMembersCount(); ++i) {
        auto name = rowType->GetMemberName(i);
        auto dataType = NKqp::UnwrapDataTypeFromStruct(*rowType, i);

        auto columnType = keyColumns.FindPtr(name);
        MKQL_ENSURE_S(columnType);
        MKQL_ENSURE_S(dataType == *columnType, "Key column type mismatch, column: " << name);
    }
}

TParseLookupTableResult ParseLookupTable(TCallable& callable) {
    MKQL_ENSURE_S(callable.GetInputsCount() >= 4);

    TParseLookupTableResult result;

    result.CallableId = callable.GetUniqueId();
    MKQL_ENSURE_S(result.CallableId);

    auto tableNode = callable.GetInput(0);
    auto keysNode = callable.GetInput(1);
    auto keysIndicesNode = callable.GetInput(2);
    auto tagsNode = callable.GetInput(3);

    result.TableId = NKqp::ParseTableId(tableNode);
    result.LookupKeys = keysNode;

    auto keyIndices = AS_VALUE(TListLiteral, keysIndicesNode);
    result.KeyIndices.resize(keyIndices->GetItemsCount());
    for (ui32 i = 0; i < result.KeyIndices.size(); ++i) {
        result.KeyIndices[i] = AS_VALUE(TDataLiteral, keyIndices->GetItems()[i])->AsValue().Get<ui32>();;
    }

    auto keyTypes = AS_TYPE(TStructType, AS_TYPE(TStreamType, keysNode.GetStaticType())->GetItemType());
    result.KeyTypes.resize(keyTypes->GetMembersCount());
    for (ui32 i = 0; i < result.KeyTypes.size(); ++i) {
        if (keyTypes->GetMemberType(i)->IsOptional()) {
            auto type = AS_TYPE(TDataType, AS_TYPE(TOptionalType, keyTypes->GetMemberType(i))->GetItemType());
            result.KeyTypes[i] = type->GetSchemeType();
        } else {
            auto type = AS_TYPE(TDataType, keyTypes->GetMemberType(i));
            result.KeyTypes[i] = type->GetSchemeType();
        }
    }

    ParseReadColumns(callable.GetType()->GetReturnType(), tagsNode, result.Columns, result.SystemColumns); 

    return result;
}

class TKqpLookupRowsWrapper : public TStatelessFlowComputationNode<TKqpLookupRowsWrapper> {
    using TBase = TStatelessFlowComputationNode<TKqpLookupRowsWrapper>;

public:
    TKqpLookupRowsWrapper(TComputationMutables& mutables, TKqpDatashardComputeContext& computeCtx,
        const TTypeEnvironment& typeEnv, const TParseLookupTableResult& parseResult, IComputationNode* lookupKeysNode)
        : TBase(mutables, this, EValueRepresentation::Boxed)
        , ComputeCtx(computeCtx)
        , TypeEnv(typeEnv)
        , ParseResult(parseResult)
        , LookupKeysNode(lookupKeysNode)
        , LocalTid(ComputeCtx.GetLocalTableId(ParseResult.TableId))
        , ColumnTags(ExtractTags(ParseResult.Columns))
        , SystemColumnTags(ExtractTags(ParseResult.SystemColumns))
        , ShardTableStats(ComputeCtx.GetDatashardCounters())
        , TaskTableStats(ComputeCtx.GetTaskCounters(ComputeCtx.GetCurrentTaskId()))
        , TableInfo(ComputeCtx.Database->GetScheme().GetTableInfo(LocalTid))
    {
        MKQL_ENSURE_S(TableInfo);

        MKQL_ENSURE_S(TableInfo->KeyColumns.size() == ParseResult.KeyIndices.size(),
            "Incomplete row key in LookupRows.");

        CellTypes.reserve(ColumnTags.size());
        for (size_t i = 0; i < ColumnTags.size(); ++i) {
            CellTypes.emplace_back(TableInfo->Columns.at(ColumnTags[i]).PType);
        }
    }

    TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto keysValues = LookupKeysNode->GetValue(ctx);

        while (!Finished) {
            NUdf::TUnboxedValue key;
            auto status = keysValues.Fetch(key);
            switch (status) {
                case NUdf::EFetchStatus::Ok: {
                    TVector<TCell> keyCells(TableInfo->KeyColumns.size());
                    FillKeyTupleValue(key, ParseResult.KeyIndices, ParseResult.KeyTypes, keyCells, TypeEnv);

                    TSmallVec<TRawTypeValue> keyValues;
                    ConvertTableKeys(ComputeCtx.Database->GetScheme(), TableInfo, keyCells, keyValues, nullptr);

                    if (keyValues.size() != TableInfo->KeyColumns.size()) {
                        throw TSchemeErrorTabletException();
                    }

                    ComputeCtx.ReadTable(ParseResult.TableId, keyCells);

                    NTable::TRowState dbRow;
                    NTable::TSelectStats stats;
                    ui64 flags = 0; // TODO: Check DisableByKeyFilter
                    auto ready = ComputeCtx.Database->Select(LocalTid, keyValues, ColumnTags, dbRow, stats, flags, ComputeCtx.GetReadVersion());
                    if (stats.Invisible)
                        ComputeCtx.BreakSetLocks();

                    switch (ready) {
                        case EReady::Page:
                            ComputeCtx.SetTabletNotReady();
                            return TUnboxedValue::MakeYield();
                        case EReady::Gone:
                            continue;
                        case EReady::Data:
                            break;
                        default:
                            MKQL_ENSURE_S(false, "Unexpected local db select status: " << (ui32)ready);
                    };

                    MKQL_ENSURE_S(CellTypes.size() == dbRow.Size(), "Invalid local db row size.");

                    TDbTupleRef dbTuple(CellTypes.data(), (*dbRow).data(), dbRow.Size());
                    TUnboxedValue result;
                    TKqpTableStats tableStats;
                    FetchRow(dbTuple, result, ctx, tableStats, ComputeCtx, SystemColumnTags);

                    ShardTableStats.NSelectRow++;
                    ShardTableStats.SelectRowRows++;
                    ShardTableStats.SelectRowBytes += tableStats.SelectRowBytes;

                    TaskTableStats.NSelectRow++;
                    TaskTableStats.SelectRowRows++;
                    TaskTableStats.SelectRowBytes += tableStats.SelectRowBytes;

                    return result;
                }

                case NUdf::EFetchStatus::Finish:
                    Finished = true;
                    return TUnboxedValue::MakeFinish();

                case NUdf::EFetchStatus::Yield:
                    return TUnboxedValue::MakeYield();
            }

            MKQL_ENSURE_S(false, "Unexpected key fetch status: " << (ui32)status);
        }

        return TUnboxedValue::MakeFinish();
    }

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(LookupKeysNode);
    }

private:
    TKqpDatashardComputeContext& ComputeCtx;
    const TTypeEnvironment& TypeEnv;
    TParseLookupTableResult ParseResult;
    IComputationNode* LookupKeysNode;
    ui64 LocalTid;
    TSmallVec<TTag> ColumnTags;
    TSmallVec<TTag> SystemColumnTags;
    TKqpTableStats& ShardTableStats;
    TKqpTableStats& TaskTableStats;
    const NTable::TScheme::TTableInfo* TableInfo;
    TSmallVec<NScheme::TTypeId> CellTypes;
    mutable bool Finished = false;
};

class TKqpLookupTableWrapper : public TStatelessFlowComputationNode<TKqpLookupTableWrapper> {
    using TBase = TStatelessFlowComputationNode<TKqpLookupTableWrapper>;

public:
    TKqpLookupTableWrapper(TComputationMutables& mutables, TKqpDatashardComputeContext& computeCtx,
        const TTypeEnvironment& typeEnv, const TParseLookupTableResult& parseResult, IComputationNode* lookupKeysNode)
        : TBase(mutables, this, EValueRepresentation::Boxed)
        , ComputeCtx(computeCtx)
        , TypeEnv(typeEnv)
        , ParseResult(parseResult)
        , LookupKeysNode(lookupKeysNode)
        , LocalTid(ComputeCtx.GetLocalTableId(ParseResult.TableId))
        , ColumnTags(ExtractTags(ParseResult.Columns))
        , SystemColumnTags(ExtractTags(ParseResult.SystemColumns))
        , ShardTableStats(ComputeCtx.GetDatashardCounters())
        , TaskTableStats(ComputeCtx.GetTaskCounters(computeCtx.GetCurrentTaskId()))
        , TableInfo(ComputeCtx.Database->GetScheme().GetTableInfo(LocalTid))
    {
        MKQL_ENSURE_S(TableInfo);
    }

    TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        while (!Finished) {
            if (!Iterator) {
                auto keysValues = LookupKeysNode->GetValue(ctx);

                NUdf::TUnboxedValue key;
                auto status = keysValues.Fetch(key);

                switch (status) {
                    case NUdf::EFetchStatus::Ok: {
                        TVector<TCell> fromCells(TableInfo->KeyColumns.size());
                        FillKeyTupleValue(key, ParseResult.KeyIndices, ParseResult.KeyTypes, fromCells, TypeEnv);

                        TVector<TCell> toCells(ParseResult.KeyIndices.size());
                        FillKeyTupleValue(key, ParseResult.KeyIndices, ParseResult.KeyTypes, toCells, TypeEnv);

                        auto range = TTableRange(fromCells, true, toCells, true);

                        TSmallVec<TRawTypeValue> from, to;
                        ConvertTableKeys(ComputeCtx.Database->GetScheme(), TableInfo, range.From, from, nullptr);
                        ConvertTableKeys(ComputeCtx.Database->GetScheme(), TableInfo, range.To, to, nullptr);

                        NTable::TKeyRange keyRange;
                        keyRange.MinKey = from;
                        keyRange.MaxKey = to;
                        keyRange.MinInclusive = range.InclusiveFrom;
                        keyRange.MaxInclusive = range.InclusiveTo;

                        ComputeCtx.ReadTable(ParseResult.TableId, range);
                        Iterator = ComputeCtx.Database->IterateRange(LocalTid, keyRange, ColumnTags, ComputeCtx.GetReadVersion());
                        break;
                    }

                    case NUdf::EFetchStatus::Finish:
                        Finished = true;
                        return TUnboxedValue::MakeFinish();

                    case NUdf::EFetchStatus::Yield:
                        return TUnboxedValue::MakeYield();
                }
            }

            TUnboxedValue result;
            TKqpTableStats tableStats;
            auto fetched = TryFetchRow(*Iterator, result, ctx, tableStats, ComputeCtx, SystemColumnTags, ParseResult.SkipNullKeys);

            if (Iterator->Stats.InvisibleRowSkips) {
                ComputeCtx.BreakSetLocks();
            }

            ShardTableStats += tableStats;
            TaskTableStats += tableStats;

            ui64 deletedRowSkips = std::exchange(Iterator->Stats.DeletedRowSkips, 0);
            ui64 invisibleRowSkips = std::exchange(Iterator->Stats.InvisibleRowSkips, 0);

            ShardTableStats.SelectRangeDeletedRowSkips += deletedRowSkips;
            ShardTableStats.InvisibleRowSkips += invisibleRowSkips;

            TaskTableStats.SelectRangeDeletedRowSkips += deletedRowSkips;
            TaskTableStats.InvisibleRowSkips += invisibleRowSkips;

            if (fetched) {
                return result;
            }

            if (Iterator->Last() == NTable::EReady::Page) {
                ComputeCtx.SetTabletNotReady();
                return TUnboxedValue::MakeYield();
            }

            Iterator = nullptr;
        }

        return TUnboxedValue::MakeFinish();
    }

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(LookupKeysNode);
    }

private:
    TKqpDatashardComputeContext& ComputeCtx;
    const TTypeEnvironment& TypeEnv;
    TParseLookupTableResult ParseResult;
    IComputationNode* LookupKeysNode;
    ui64 LocalTid;
    TSmallVec<TTag> ColumnTags;
    TSmallVec<TTag> SystemColumnTags;
    TKqpTableStats& ShardTableStats;
    TKqpTableStats& TaskTableStats;
    const NTable::TScheme::TTableInfo* TableInfo;
    mutable TAutoPtr<NTable::TTableIt> Iterator;
    mutable bool Finished = false;
};

IComputationNode* WrapKqpLookupTableInternal(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    auto parseResult = ParseLookupTable(callable);
    auto lookupKeysNode = LocateNode(ctx.NodeLocator, *parseResult.LookupKeys.GetNode());

    auto keyColumns = computeCtx.GetKeyColumnsMap(parseResult.TableId);
    ValidateLookupKeys(parseResult.LookupKeys.GetStaticType(), keyColumns);

    auto localTid = computeCtx.GetLocalTableId(parseResult.TableId);
    auto tableInfo = computeCtx.Database->GetScheme().GetTableInfo(localTid);
    MKQL_ENSURE_S(tableInfo);

    if (tableInfo->KeyColumns.size() == parseResult.KeyIndices.size()) {
        return new TKqpLookupRowsWrapper(ctx.Mutables, computeCtx, ctx.Env, parseResult, lookupKeysNode);
    } else {
        return new TKqpLookupTableWrapper(ctx.Mutables, computeCtx, ctx.Env, parseResult, lookupKeysNode);
    }
}

} // namespace

IComputationNode* WrapKqpLookupTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    return WrapKqpLookupTableInternal(callable, ctx, computeCtx);
}

} // namespace NMiniKQL
} // namespace NKikimr
