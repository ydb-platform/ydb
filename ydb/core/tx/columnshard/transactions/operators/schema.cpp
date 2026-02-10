#include "schema.h"
#include <ydb/core/tx/columnshard/subscriber/abstract/subscriber/subscriber.h>
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/tx_completed/event.h>
#include <ydb/core/tx/columnshard/transactions/transactions/tx_finish_async.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <util/string/join.h>
#include <util/stream/output.h>

namespace NKikimr::NColumnShard {

class TWaitOnProposeTxSubscriberBase : public NSubscriber::ISubscriber {
    const ui64 TxId;
protected:
    TWaitOnProposeTxSubscriberBase(const ui64 txId)
        : TxId(txId) {    }

    void OnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard& shard) {
        const NActors::TLogContextGuard g = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "on_subscriber_event")("tx_id", TxId)("event", ev->DebugString());
        AFL_VERIFY(!IsFinished());
        DoOnEvent(ev);
        if (IsFinished()) {
            shard.Execute(new TTxFinishAsyncTransaction(shard, TxId));
        }
    }

protected:
    virtual void DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) = 0;
};


class TWaitEraseTablesTxSubscriber: public TWaitOnProposeTxSubscriberBase {
private:
    THashSet<TInternalPathId> WaitTables;
public:
    TWaitEraseTablesTxSubscriber(const ui64 txId, THashSet<TInternalPathId>&& waitTables)
        : TWaitOnProposeTxSubscriberBase(txId)
        , WaitTables(std::move(waitTables)) {
    }

    virtual std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::TablesErased };
    }

    virtual void DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::TablesErased);
        auto* evErased = static_cast<const NSubscriber::TEventTablesErased*>(ev.get());
        for (auto&& i : evErased->GetPathIds()) {
            WaitTables.erase(i);
        }
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("remained", JoinSeq(",", WaitTables));
    }

    virtual bool IsFinished() const override {
        return WaitTables.empty();
    }
};

class TWaitTxs: public TWaitOnProposeTxSubscriberBase {
    THashSet<ui64> TxIdsToWait;
public:
    TWaitTxs(const ui64 txId, const THashSet<ui64>&& txIdsToWait)
        : TWaitOnProposeTxSubscriberBase(txId)
        , TxIdsToWait(std::move(txIdsToWait)) {
    }
    std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::TxCompleted };
    }

    bool IsFinished() const override {
        return TxIdsToWait.empty();
    }

    virtual void DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::TxCompleted);
        const auto* evCompleted = static_cast<const NSubscriber::TEventTxCompleted*>(ev.get());
        AFL_VERIFY(TxIdsToWait.erase(evCompleted->GetTxId()));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("completed", evCompleted->GetTxId())("remained", JoinSeq(",", TxIdsToWait));
    }
};



TTxController::TProposeResult TSchemaTransactionOperator::DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(!WaitOnPropose);
    auto seqNo = SeqNoFromProto(SchemaTxBody.GetSeqNo());
    auto lastSeqNo = owner.LastSchemaSeqNo;

    // Check if proposal is outdated
    if (seqNo < lastSeqNo) {
        auto errorMessage = TStringBuilder() << "Ignoring outdated schema tx proposal at tablet " << owner.TabletID() << " txId " << GetTxId()
                                             << " ssId " << owner.CurrentSchemeShardId << " seqNo " << seqNo << " lastSeqNo " << lastSeqNo;
        return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_CHANGED, errorMessage);
    }

    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
        {
            if (owner.InitShardCounter.Add(1) != 1) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "repeated_initialization")("tx_id", GetTxId())(
                    "counter", owner.InitShardCounter.Val());
            }
            auto validationStatus = ValidateTables(SchemaTxBody.GetInitShard().GetTables());
            if (validationStatus.IsFail()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            auto pathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetInitShard().GetTables());
            if (!pathIdsToErase.empty()) {
                WaitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(GetTxId(), std::move(pathIdsToErase));
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
        {
            const auto& tables = SchemaTxBody.GetEnsureTables().GetTables();
            auto validationStatus = ValidateTables(tables);
            if (validationStatus.IsFail()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            auto waitPathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetEnsureTables().GetTables());
            if (!waitPathIdsToErase.empty()) {
                WaitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(GetTxId(), std::move(waitPathIdsToErase));
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable:
        {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetSrcPathId());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetDstPathId());
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("propose_execute", "move_table")("src", srcSchemeShardLocalPathId)("dst", dstSchemeShardLocalPathId);
            if (!owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "No such table");
            }
            if (owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Rename to existing table");
            }
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();  //TODO #8650 Get transaction for moving pathId only
            if (!txIdsToWait.empty()) {
                AFL_VERIFY(!txIdsToWait.contains(GetTxId()))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIdsToWait));
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
            owner.TablesManager.MoveTablePropose(srcSchemeShardLocalPathId);
            break;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kCopyTable:
        {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetSrcPathId());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetDstPathId());
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("propose_execute", "copy_table")("src", srcSchemeShardLocalPathId)("dst", dstSchemeShardLocalPathId);
            if (!owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "No such table");
            }
            if (owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Copy to existing table");
            }
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();
            if (!txIdsToWait.empty()) {
                AFL_VERIFY(!txIdsToWait.contains(GetTxId()))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIdsToWait));
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
            owner.TablesManager.CopyTablePropose(srcSchemeShardLocalPathId);
            break;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (WaitOnPropose) {
        owner.Subscribers->RegisterSubscriber(WaitOnPropose);
    }

    owner.UpdateSchemaSeqNo(seqNo, txc);
    return TProposeResult();
}

NKikimr::TConclusionStatus TSchemaTransactionOperator::ValidateTableSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) const {
    namespace NTypeIds = NScheme::NTypeIds;
    static const THashSet<NScheme::TTypeId> pkSupportedTypes = {
        NTypeIds::Bool,
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

    if (!schema.KeyColumnNamesSize()) {
        return TConclusionStatus::Fail("There is no key columns");
    }

    THashSet<TString> keyColumns(schema.GetKeyColumnNames().begin(), schema.GetKeyColumnNames().end());
    TVector<TString> columnErrors;
    for (const NKikimrSchemeOp::TOlapColumnDescription& column : schema.GetColumns()) {
        TString name = column.GetName();
        NScheme::TTypeId typeId = column.GetTypeId();
        NScheme::TTypeInfo schemeType;
        if (column.HasTypeInfo()) {
            schemeType = NScheme::TypeInfoFromProto(typeId, column.GetTypeInfo());
        } else {
            schemeType = typeId;
        }

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
   AFL_VERIFY(!WaitOnPropose);
    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
        {
            THashSet<TInternalPathId> waitPathIdsToErase;
            for (auto&& i : SchemaTxBody.GetEnsureTables().GetTables()) {
                const auto& schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(i);
                if (const auto internalPathId = owner.TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false)) {
                    if (owner.TablesManager.HasTable(*internalPathId, true)) {
                        waitPathIdsToErase.emplace(*internalPathId);
                    }
                }
            }
            if (!waitPathIdsToErase.empty()) {
                WaitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(GetTxId(), std::move(waitPathIdsToErase));
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable:
        {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetSrcPathId());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetDstPathId());

            AFL_VERIFY(owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false));
            AFL_VERIFY(!owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false));
            owner.TablesManager.MoveTablePropose(srcSchemeShardLocalPathId);
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();
            AFL_VERIFY(txIdsToWait.erase(GetTxId()));
            if (!txIdsToWait.empty()) {
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kCopyTable:
        {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetSrcPathId());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetDstPathId());
            AFL_VERIFY(owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false));
            AFL_VERIFY(!owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false));
            owner.TablesManager.CopyTablePropose(srcSchemeShardLocalPathId);
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();
            AFL_VERIFY(txIdsToWait.erase(GetTxId()));
            if (!txIdsToWait.empty()) {
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (WaitOnPropose) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "wait_on_propose")("tx_id", GetTxId());
        owner.Subscribers->RegisterSubscriber(WaitOnPropose);
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "remove_pathes_cleaned")("tx_id", GetTxId());
        owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()));
    }
}

void TSchemaTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) {
}

} //namespace NKikimr::NColumnShard