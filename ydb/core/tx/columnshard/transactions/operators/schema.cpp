#include "schema.h"
#include <ydb/core/tx/columnshard/subscriber/abstract/subscriber/subscriber.h>
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <ydb/core/tx/columnshard/transactions/transactions/tx_finish_async.h>
#include <util/string/join.h>

namespace NKikimr::NColumnShard {

class TWaitEraseTablesTxSubscriber: public NSubscriber::ISubscriber {
private:
    THashSet<ui64> WaitTables;
    const ui64 TxId;
public:
    virtual std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::TablesErased };
    }

    virtual bool DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard& shard) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::TablesErased);
        auto* evErased = static_cast<const NSubscriber::TEventTablesErased*>(ev.get());
        bool result = false;
        for (auto&& i : evErased->GetPathIds()) {
            result |= WaitTables.erase(i);
        }
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "on_event")("remained", JoinSeq(",", WaitTables));
        if (WaitTables.empty()) {
            shard.Execute(new TTxFinishAsyncTransaction(shard, TxId));
        }
        return result;
    }

    virtual bool IsFinished() const override {
        return WaitTables.empty();
    }

    TWaitEraseTablesTxSubscriber(const THashSet<ui64>& waitTables, const ui64 txId)
        : WaitTables(waitTables)
        , TxId(txId) {

    }
};

NKikimr::NColumnShard::TTxController::TProposeResult TSchemaTransactionOperator::DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
        {
            auto validationStatus = ValidateTables(SchemaTxBody.GetInitShard().GetTables());
            if (validationStatus.IsFail()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            WaitPathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetInitShard().GetTables());
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
        {
            auto validationStatus = ValidateTables(SchemaTxBody.GetEnsureTables().GetTables());
            if (validationStatus.IsFail()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            WaitPathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetEnsureTables().GetTables());
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }

    auto seqNo = SeqNoFromProto(SchemaTxBody.GetSeqNo());
    auto lastSeqNo = owner.LastSchemaSeqNo;

    // Check if proposal is outdated
    if (seqNo < lastSeqNo) {
        auto errorMessage = TStringBuilder()
            << "Ignoring outdated schema tx proposal at tablet "
            << owner.TabletID()
            << " txId " << GetTxId()
            << " ssId " << owner.CurrentSchemeShardId
            << " seqNo " << seqNo
            << " lastSeqNo " << lastSeqNo;
        return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_CHANGED, errorMessage);
    }

    owner.UpdateSchemaSeqNo(seqNo, txc);
    return TProposeResult();
}

NKikimr::TConclusionStatus TSchemaTransactionOperator::ValidateTableSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) const {
    namespace NTypeIds = NScheme::NTypeIds;
    static const THashSet<NScheme::TTypeId> pkSupportedTypes = {
        NTypeIds::Timestamp,
        NTypeIds::Date32,
        NTypeIds::Datetime64,
        NTypeIds::Timestamp64,
        NTypeIds::Interval64,
        NTypeIds::Int8,
        NTypeIds::Int16,
        NTypeIds::Int32,
        NTypeIds::Int64,
        NTypeIds::Uint8,
        NTypeIds::Uint16,
        NTypeIds::Uint32,
        NTypeIds::Uint64,
        NTypeIds::Date,
        NTypeIds::Datetime,
        //NTypeIds::Interval,
        //NTypeIds::Float,
        //NTypeIds::Double,
        NTypeIds::String,
        NTypeIds::Utf8,
        NTypeIds::Decimal
    };
    if (!schema.HasEngine() ||
        schema.GetEngine() != NKikimrSchemeOp::EColumnTableEngine::COLUMN_ENGINE_REPLACING_TIMESERIES) {
        return TConclusionStatus::Fail("Invalid scheme engine: " + (schema.HasEngine() ? NKikimrSchemeOp::EColumnTableEngine_Name(schema.GetEngine()) : TString("No")));
    }

    if (!schema.KeyColumnNamesSize()) {
        return TConclusionStatus::Fail("There is no key columns");
    }

    THashSet<TString> keyColumns(schema.GetKeyColumnNames().begin(), schema.GetKeyColumnNames().end());
    TVector<TString> columnErrors;
    for (const NKikimrSchemeOp::TOlapColumnDescription& column : schema.GetColumns()) {
        TString name = column.GetName();
        void* typeDescr = nullptr;
        if (column.GetTypeId() == NTypeIds::Pg && column.HasTypeInfo()) {
            typeDescr = NPg::TypeDescFromPgTypeId(column.GetTypeInfo().GetPgTypeId());
        }

        NScheme::TTypeInfo schemeType(column.GetTypeId(), typeDescr);
        if (keyColumns.contains(name) && !pkSupportedTypes.contains(column.GetTypeId())) {
            columnErrors.emplace_back("key column " + name + " has unsupported type " + column.GetTypeName());
        }
        auto arrowType = NArrow::GetArrowType(schemeType);
        if (!arrowType.ok()) {
            columnErrors.emplace_back("column " + name + ": " + arrowType.status().ToString());
        }
        keyColumns.erase(name);
    }
    if (!columnErrors.empty()) {
        return TConclusionStatus::Fail("Column errors: " + JoinSeq("; ", columnErrors));
    }

    if (!keyColumns.empty()) {
        return TConclusionStatus::Fail("Key columns not in scheme: " + JoinSeq(", ", keyColumns));
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TSchemaTransactionOperator::ValidateTables(::google::protobuf::RepeatedPtrField<::NKikimrTxColumnShard::TCreateTable> tables) const {
    for (auto& table : tables) {
        if (table.HasSchemaPreset()) {
            const auto validationStatus = ValidateTablePreset(table.GetSchemaPreset());
            if (validationStatus.IsFail()) {
                return validationStatus;
            }
        }
        if (table.HasSchema()) {
            const auto validationStatus = ValidateTableSchema(table.GetSchema());
            if (validationStatus.IsFail()) {
                return validationStatus;
            }
        }
    } return TConclusionStatus::Success();
}

void TSchemaTransactionOperator::DoOnTabletInit(TColumnShard& owner) {
    AFL_VERIFY(WaitPathIdsToErase.empty());
    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
        {
            for (auto&& i : SchemaTxBody.GetEnsureTables().GetTables()) {
                AFL_VERIFY(!owner.TablesManager.HasTable(i.GetPathId()));
                if (owner.TablesManager.HasTable(i.GetPathId(), true)) {
                    WaitPathIdsToErase.emplace(i.GetPathId());
                }
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (WaitPathIdsToErase.size()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "wait_remove_path_id")("pathes", JoinSeq(",", WaitPathIdsToErase))("tx_id", GetTxId());
        owner.Subscribers->RegisterSubscriber(std::make_shared<TWaitEraseTablesTxSubscriber>(WaitPathIdsToErase, GetTxId()));
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "remove_pathes_cleaned")("tx_id", GetTxId());
        owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()));
    }
}

void TSchemaTransactionOperator::DoStartProposeOnComplete(TColumnShard& owner, const TActorContext& /*ctx*/) {
    AFL_VERIFY(WaitPathIdsToErase.size());
    owner.Subscribers->RegisterSubscriber(std::make_shared<TWaitEraseTablesTxSubscriber>(WaitPathIdsToErase, GetTxId()));
}

}
