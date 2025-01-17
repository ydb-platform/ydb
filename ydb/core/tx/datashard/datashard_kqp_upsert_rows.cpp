#include "datashard_kqp_compute.h"
#include "datashard_user_table.h"

#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/engine/mkql_engine_flat_host.h>
#include <ydb/core/kqp/runtime/kqp_runtime_impl.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>

#include <util/generic/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

namespace {

struct TUpsertColumn {
    ui32 ColumnId;
    ui32 RowIndex;
};

class TKqpUpsertRowsWrapper : public TMutableComputationNode<TKqpUpsertRowsWrapper> {
    using TBase = TMutableComputationNode<TKqpUpsertRowsWrapper>;

public:
    class TRowResult : public TComputationValue<TRowResult> {
        using TBase = TComputationValue<TRowResult>;

    public:
        TRowResult(TMemoryUsageInfo* memInfo, const TKqpUpsertRowsWrapper& owner,
            NUdf::TUnboxedValue&& row)
            : TBase(memInfo)
            , Owner(owner)
            , Row(std::move(row)) {}

    private:
        void Apply(NUdf::IApplyContext& applyContext) const override {
            auto& dsApplyCtx = *CheckedCast<TKqpDatashardApplyContext*>(&applyContext);

            TVector<TCell> keyTuple(Owner.KeyIndices.size());
            FillKeyTupleValue(Row, Owner.KeyIndices, Owner.RowTypes, keyTuple, *dsApplyCtx.Env);

            if (dsApplyCtx.Host->IsPathErased(Owner.TableId)) {
                return;
            }

            if (!dsApplyCtx.Host->IsMyKey(Owner.TableId, keyTuple)) {
                return;
            }

            TVector<IEngineFlatHost::TUpdateCommand> commands;
            commands.reserve(Owner.UpsertColumns.size());

            for (auto& upsertColumn : Owner.UpsertColumns) {
                IEngineFlatHost::TUpdateCommand command;
                command.Column = upsertColumn.ColumnId;
                command.Operation = TKeyDesc::EColumnOperation::Set;
                auto rowIndex = upsertColumn.RowIndex;

                NScheme::TTypeInfo type = Owner.RowTypes[rowIndex];
                i32 typmod = Owner.RowTypeMods[rowIndex];
                NUdf::TUnboxedValue value = Row.GetElement(rowIndex);

                if (value) {
                    if (type.GetTypeId() != NScheme::NTypeIds::Pg) {
                        auto slot = NUdf::GetDataSlot(type.GetTypeId());
                        MKQL_ENSURE(IsValidValue(slot, value),
                            "Malformed value for type: " << NUdf::GetDataTypeInfo(slot).Name << ", " << value);
                    } else {
                        Y_UNUSED(
                            NYql::NCommon::PgValueToNativeBinary(value, NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc()))
                        );
                    }
                }

                // NOTE: We have to copy values here as some values inlined in TUnboxedValue
                // cannot be inlined in TCell.
                TMaybe<TString> error;
                command.Value = MakeCell(type, value, *dsApplyCtx.Env, true, typmod, &error);
                MKQL_ENSURE(!error, "Incorrect value: " << *error);

                commands.emplace_back(std::move(command));
            }
            Y_ABORT_UNLESS(dsApplyCtx.ShardTableStats);
            Y_ABORT_UNLESS(dsApplyCtx.TaskTableStats);

            ui64 nUpdateRow = dsApplyCtx.ShardTableStats->NUpdateRow;
            ui64 updateRowBytes = dsApplyCtx.ShardTableStats->UpdateRowBytes;

            dsApplyCtx.Host->UpdateRow(Owner.TableId, keyTuple, commands);

            if (i64 delta = dsApplyCtx.ShardTableStats->NUpdateRow - nUpdateRow; delta > 0) {
                dsApplyCtx.TaskTableStats->NUpdateRow += delta;
                dsApplyCtx.TaskTableStats->UpdateRowBytes += dsApplyCtx.ShardTableStats->UpdateRowBytes - updateRowBytes;
            }
        };

    private:
        const TKqpUpsertRowsWrapper& Owner;
        NUdf::TUnboxedValue Row;
    };

    class TRowsResult : public TComputationValue<TRowsResult> {
        using TBase = TComputationValue<TRowsResult>;

    public:
        TRowsResult(TMemoryUsageInfo* memInfo, const TKqpUpsertRowsWrapper& owner,
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
        const TKqpUpsertRowsWrapper& Owner;
        NUdf::TUnboxedValue Rows;
    };

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TRowsResult>(*this, RowsNode->GetValue(ctx));
    }

public:
    TKqpUpsertRowsWrapper(TComputationMutables& mutables, const TTableId& tableId, IComputationNode* rowsNode,
            TVector<NScheme::TTypeInfo>&& rowTypes, TVector<i32>&& rowTypeMods,
            TVector<ui32>&& keyIndices, TVector<TUpsertColumn>&& upsertColumns)
        : TBase(mutables)
        , TableId(tableId)
        , RowsNode(rowsNode)
        , RowTypes(std::move(rowTypes))
        , RowTypeMods(std::move(rowTypeMods))
        , KeyIndices(std::move(keyIndices))
        , UpsertColumns(std::move(upsertColumns))
    {}

private:
    void RegisterDependencies() const final {
        DependsOn(RowsNode);
    }

private:
    TTableId TableId;
    IComputationNode* RowsNode;
    TVector<NScheme::TTypeInfo> RowTypes;
    TVector<i32> RowTypeMods;
    TVector<ui32> KeyIndices;
    TVector<TUpsertColumn> UpsertColumns;
};

} // namespace

IComputationNode* WrapKqpUpsertRows(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    MKQL_ENSURE_S(callable.GetInputsCount() >= 3);

    auto tableNode = callable.GetInput(0);
    auto rowsNode = callable.GetInput(1);
    auto upsertColumnsNode = callable.GetInput(2);
    bool isUpdate = false;
    if (callable.GetInputsCount() >= 4) {
        auto isUpdateNode = callable.GetInput(3);
        isUpdate = AS_VALUE(TDataLiteral, isUpdateNode)->AsValue().Get<bool>();
    }
    auto tableId = NKqp::ParseTableId(tableNode);
    auto tableInfo = computeCtx.GetTable(tableId);
    MKQL_ENSURE(tableInfo, "Table not found: " << tableId.PathId.ToString());

    auto rowType = AS_TYPE(TStructType, AS_TYPE(TStreamType, rowsNode.GetStaticType())->GetItemType());

    MKQL_ENSURE_S(tableInfo->KeyColumnIds.size() <= rowType->GetMembersCount(),
        "not enough columns in the runtime node");

    THashMap<TStringBuf, ui32> inputIndex; // column name -> struct field index
    THashMap<TStringBuf, ui32> columnIds; // column name -> user table column id (all columns)

    auto memberCount = rowType->GetMembersCount();
    TVector<NScheme::TTypeInfo> rowTypes(memberCount); // struct field index -> type info
    TVector<i32> rowTypeMods; // struct field index -> binary type mod
    rowTypeMods.resize(memberCount, -1);

    for (const auto& [columnId, columnInfo] : tableInfo->Columns) {
        columnIds.emplace(columnInfo.Name, columnId);
    }

    for (ui32 i = 0; i < rowTypes.size(); ++i) {
        const auto& name = rowType->GetMemberName(i);
        MKQL_ENSURE_S(inputIndex.emplace(name, i).second);
        const NScheme::TTypeInfo typeInfo = NKqp::UnwrapTypeInfoFromStruct(*rowType, i);
        rowTypes[i] = typeInfo;

        if (typeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
            auto itColumnId = columnIds.find(name);
            MKQL_ENSURE_S(itColumnId != columnIds.end());
            auto itColumnInfo = tableInfo->Columns.find(itColumnId->second);
            MKQL_ENSURE_S(itColumnInfo != tableInfo->Columns.end());
            const auto& typeMod = itColumnInfo->second.TypeMod;
            if (!typeMod.empty()) {
                auto result = NPg::BinaryTypeModFromTextTypeMod(typeMod, typeInfo.GetPgTypeDesc());
                MKQL_ENSURE_S(!result.Error, "invalid type mod");
                rowTypeMods[i] = result.Typmod;
            }
        } 
    }

    TVector<ui32> keyIndices(tableInfo->KeyColumnIds.size());
    for (ui32 i = 0; i < keyIndices.size(); i++) {
        auto& columnInfo = computeCtx.GetKeyColumnInfo(*tableInfo, i);

        auto it = inputIndex.find(columnInfo.Name);
        MKQL_ENSURE_S(it != inputIndex.end());
        const NScheme::TTypeInfo typeInfo = NKqp::UnwrapTypeInfoFromStruct(*rowType, it->second);
        MKQL_ENSURE_S(typeInfo == columnInfo.Type, "row key type mismatch with table key type");
        keyIndices[i] = it->second;
    }

    for (const auto& [_, column] : tableInfo->Columns) {
        if (column.NotNull && !isUpdate) {
            auto it = inputIndex.find(column.Name);
            MKQL_ENSURE(it != inputIndex.end(),
                "Not null column " << column.Name << " has to be specified in upsert");

            if (it != inputIndex.end()) {
                auto columnType = rowType->GetMemberType(it->second);
                MKQL_ENSURE(columnType->GetKind() != NMiniKQL::TType::EKind::Optional,
                    "Not null column " << column.Name << " can't be optional");
            }
        }
    }

    auto upsertColumnsDict = AS_VALUE(TDictLiteral, upsertColumnsNode);
    TVector<TUpsertColumn> upsertColumns(upsertColumnsDict->GetItemsCount());
    for (ui32 i = 0; i < upsertColumns.size(); ++i) {
        auto item = upsertColumnsDict->GetItem(i);

        auto& upsertColumn = upsertColumns[i];
        upsertColumn.ColumnId = AS_VALUE(TDataLiteral, item.first)->AsValue().Get<ui32>();
        upsertColumn.RowIndex = AS_VALUE(TDataLiteral, item.second)->AsValue().Get<ui32>();

        auto tableColumn = tableInfo->Columns.FindPtr(upsertColumn.ColumnId);
        MKQL_ENSURE_S(tableColumn);

        MKQL_ENSURE_S(rowTypes[upsertColumn.RowIndex] == tableColumn->Type,
            "upsert column type missmatch, column: " << tableColumn->Name);
    }

    return new TKqpUpsertRowsWrapper(ctx.Mutables, tableId,
        LocateNode(ctx.NodeLocator, *rowsNode.GetNode()),
        std::move(rowTypes), std::move(rowTypeMods),
        std::move(keyIndices), std::move(upsertColumns));
}

} // namespace NMiniKQL
} // namespace NKikimr
