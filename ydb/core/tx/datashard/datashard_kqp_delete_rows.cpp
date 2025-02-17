#include "datashard_kqp_compute.h"

#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/engine/mkql_engine_flat_host.h>
#include <ydb/core/kqp/runtime/kqp_runtime_impl.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/generic/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

namespace {

class TKqpDeleteRowsWrapper : public TMutableComputationNode<TKqpDeleteRowsWrapper> {
    using TBase = TMutableComputationNode<TKqpDeleteRowsWrapper>;

public:
    class TRowResult : public TComputationValue<TRowResult> {
        using TBase = TComputationValue<TRowResult>;

    public:
        TRowResult(TMemoryUsageInfo* memInfo, const TKqpDeleteRowsWrapper& owner,
            NUdf::TUnboxedValue&& row)
            : TBase(memInfo)
            , Owner(owner)
            , Row(std::move(row)) {}

    private:
        void Apply(NUdf::IApplyContext& applyContext) const override {
            auto& engineCtx = *CheckedCast<TKqpDatashardApplyContext*>(&applyContext);

            TVector<TCell> keyTuple(Owner.KeyIndices.size());
            FillKeyTupleValue(Row, Owner.KeyIndices, Owner.RowTypes, keyTuple, *engineCtx.Env);

            if (engineCtx.Host->IsPathErased(Owner.TableId)) {
                return;
            }

            if (!engineCtx.Host->IsMyKey(Owner.TableId, keyTuple)) {
                return;
            }
            Y_ABORT_UNLESS(engineCtx.ShardTableStats);
            Y_ABORT_UNLESS(engineCtx.TaskTableStats);

            ui64 nEraseRow = engineCtx.ShardTableStats->NEraseRow;

            engineCtx.Host->EraseRow(Owner.TableId, keyTuple);

            if (i64 delta = engineCtx.ShardTableStats->NEraseRow - nEraseRow; delta > 0) {
                engineCtx.TaskTableStats->NEraseRow += delta;
            }
        };

    private:
        const TKqpDeleteRowsWrapper& Owner;
        NUdf::TUnboxedValue Row;
    };

    class TRowsResult : public TComputationValue<TRowsResult> {
        using TBase = TComputationValue<TRowsResult>;

    public:
        TRowsResult(TMemoryUsageInfo* memInfo, const TKqpDeleteRowsWrapper& owner,
            NUdf::TUnboxedValue&& rows)
            : TBase(memInfo)
            , Owner(owner)
            , Rows(std::move(rows)) {}

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            NUdf::TUnboxedValue row;
            auto status = Rows.Fetch(row);

            if (status == NUdf::EFetchStatus::Ok) {
                result = NUdf::TUnboxedValuePod(new TRowResult(GetMemInfo(), Owner, std::move(row)));
            }

            return status;
        }

    private:
        const TKqpDeleteRowsWrapper& Owner;
        NUdf::TUnboxedValue Rows;
    };

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TRowsResult>(*this, RowsNode->GetValue(ctx));
    }

public:
    TKqpDeleteRowsWrapper(TComputationMutables& mutables, const TTableId& tableId, IComputationNode* rowsNode,
            TVector<NScheme::TTypeInfo> rowTypes, TVector<ui32> keyIndices)
        : TBase(mutables)
        , TableId(tableId)
        , RowsNode(rowsNode)
        , RowTypes(std::move(rowTypes))
        , KeyIndices(std::move(keyIndices))
    {}

private:
    void RegisterDependencies() const final {
        DependsOn(RowsNode);
    }

private:
    TTableId TableId;
    IComputationNode* RowsNode;
    const TVector<NScheme::TTypeInfo> RowTypes;
    const TVector<ui32> KeyIndices;
};

} // namespace

IComputationNode* WrapKqpDeleteRows(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    MKQL_ENSURE_S(callable.GetInputsCount() == 2);

    auto tableNode = callable.GetInput(0);
    auto rowsNode = callable.GetInput(1);

    auto tableId = NKqp::ParseTableId(tableNode);
    auto tableInfo = computeCtx.GetTable(tableId);
    MKQL_ENSURE(tableInfo, "Table not found: " << tableId.PathId.ToString());

    auto rowType = AS_TYPE(TStructType, AS_TYPE(TStreamType, rowsNode.GetStaticType())->GetItemType());
    MKQL_ENSURE_S(tableInfo->KeyColumnIds.size() == rowType->GetMembersCount(), "Table key column count mismatch"
        << ", expected: " << tableInfo->KeyColumnIds.size()
        << ", actual: " << rowType->GetMembersCount());

    THashMap<TString, ui32> inputIndex;
    TVector<NScheme::TTypeInfo> rowTypes(rowType->GetMembersCount());
    for (ui32 i = 0; i < rowType->GetMembersCount(); ++i) {
        const auto& name = rowType->GetMemberName(i);
        MKQL_ENSURE_S(inputIndex.emplace(TString(name), i).second);

        const NScheme::TTypeInfo typeInfo = NKqp::UnwrapTypeInfoFromStruct(*rowType, i);
        rowTypes[i] = typeInfo;
    }

    TVector<ui32> keyIndices(tableInfo->KeyColumnIds.size());
    for (ui32 i = 0; i < keyIndices.size(); i++) {
        auto& columnInfo = computeCtx.GetKeyColumnInfo(*tableInfo, i);

        auto it = inputIndex.find(columnInfo.Name);

        MKQL_ENSURE_S(rowTypes[it->second] == columnInfo.Type, "Key type mismatch"
            << ", column: " << columnInfo.Name
            << ", expected: " << NScheme::TypeName(columnInfo.Type)
            << ", actual: " << NScheme::TypeName(rowTypes[it->second]));

        keyIndices[i] = it->second;
    }

    return new TKqpDeleteRowsWrapper(ctx.Mutables, tableId,
        LocateNode(ctx.NodeLocator, *rowsNode.GetNode()), std::move(rowTypes), std::move(keyIndices));
}

} // namespace NMiniKQL
} // namespace NKikimr
