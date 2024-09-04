#include "schema.h"
#include <ydb/core/tx/columnshard/subscriber/abstract/subscriber/subscriber.h>
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/indexation_completed/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/transaction_completed/event.h>
#include <ydb/core/tx/columnshard/transactions/transactions/tx_finish_async.h>
#include <util/string/join.h>
#include <util/stream/output.h>

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

class TWaitTransactions: public NSubscriber::ISubscriber {
    THashSet<ui64> TxIdsToWait;
    std::function<void()> OnFinish;
private:
    void Finish() {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "waiting_transactions_finished");
        OnFinish();
    }
public:
    TWaitTransactions(THashSet<ui64>&& txIdsToWait, std::function<void()> onFinish)
        : TxIdsToWait(std::move(txIdsToWait))
        , OnFinish(onFinish)
    {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "waiting_transactions")("tx_ids", JoinSeq(",", TxIdsToWait));
        if(IsFinished()) {
            Finish();
        }
    }
    std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::TransactionCompleted };
    }
    bool IsFinished() const override {
        return TxIdsToWait.empty();
    }
    virtual bool DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard&) override {
        AFL_VERIFY(!IsFinished());
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::TransactionCompleted);
        const auto* evCompleted = static_cast<const NSubscriber::TEventTransactionCompleted*>(ev.get());
        if (TxIdsToWait.erase(evCompleted->GetTxId())) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "on_tx_completed")("completed", evCompleted->GetTxId())("remained", JoinSeq(",", TxIdsToWait));
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_tx_completed")("completed", evCompleted->GetTxId())("remained", JoinSeq(",", TxIdsToWait));
        }
        if(IsFinished()) {
            Finish();
        }
        return true;
    }
};

class TWaitIndexation: public NSubscriber::ISubscriber {
    std::optional<ui64> PathIdToWait;
    std::function<void()> OnFinish;
     void Finish() {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "waiting_indexation_finished");
        OnFinish();
    }   
public:
    TWaitIndexation(const std::optional<ui64> pathIdToWait, std::function<void()> onFinish)
        : PathIdToWait(pathIdToWait)
        , OnFinish(onFinish)
    {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "waiting_indexation")("path_id", PathIdToWait ? std::to_string(*PathIdToWait) : "none");
        if(IsFinished()) {
            Finish();
        }

    }
    std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::IndexationCompleted };
    }
    bool IsFinished() const override {
        return !PathIdToWait.has_value();
    }
    virtual bool DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard&) override {
        AFL_VERIFY(!IsFinished());
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::IndexationCompleted);
        const auto* evCompleted = static_cast<const NSubscriber::TEventIndexationCompleted*>(ev.get());
        const auto pathId = evCompleted->GetPathId();
        if (pathId == PathIdToWait) {
            PathIdToWait.reset();
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "on_indexation_completed")("path_id", pathId)("finished", true);
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_write_completed")("path_id", pathId)("finished", false);
        }
        if(IsFinished()) {
            Finish();
        }
        return true;
    }
};


TTxController::TProposeResult TSchemaTransactionOperator::DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(!WaitOnPropose);
    std::shared_ptr<NSubscriber::ISubscriber> waitOnPropose;

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
            auto validationStatus = ValidateTables(SchemaTxBody.GetInitShard().GetTables());
            if (validationStatus.IsFail()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            auto pathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetInitShard().GetTables());
            if (!pathIdsToErase.empty()) {
                waitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(pathIdsToErase, GetTxId());
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
                waitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(waitPathIdsToErase, GetTxId());
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable:
        {
            const auto srcPathId = SchemaTxBody.GetMoveTable().GetSrcPathId();
            const auto dstPathId = SchemaTxBody.GetMoveTable().GetDstPathId();
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("propose_execute", "move_table")("src", srcPathId)("dst", dstPathId);
            if (!owner.TablesManager.HasTable(srcPathId)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "No such table");
            }
            if (!owner.TablesManager.GetTable(srcPathId).GetTieringUsage().empty()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Tiering is on");
            }
            if (owner.TablesManager.HasTable(dstPathId)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Rename to existing table");
            }
            auto txIds = owner.GetProgressTxController().GetTxs();  //TODO GetTxsByPathId(srcPathId) #8650
            AFL_VERIFY(!txIds.contains(GetTxId()))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIds));
            waitOnPropose = std::make_shared<TWaitTransactions>(
                std::move(txIds),
                [srcPathId, this, &owner](){
                    auto txIds = owner.GetProgressTxController().GetTxs();
                    AFL_VERIFY(txIds.empty() || (txIds.size() == 1 && txIds.contains(GetTxId())))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIds));
                    THashSet<TWriteId> writeIds{TWriteId{199}};
                    auto hasDataToIndex = owner.InsertTable->HasCommittedByPathId(srcPathId);
                    owner.Subscribers->RegisterSubscriber(std::make_shared<TWaitIndexation>(
                        hasDataToIndex ? std::optional{srcPathId} : std::nullopt,
                        [this, &owner]() {
                            owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()));
                        }
                    ));
                }             
            );
            owner.TablesManager.StartMovingTable(srcPathId, dstPathId);
            break;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (waitOnPropose && !waitOnPropose->IsFinished()) {
        WaitOnPropose = std::move(waitOnPropose);
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
   AFL_VERIFY(!WaitOnPropose);
    std::shared_ptr<NSubscriber::ISubscriber> waitOnPropose;
    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
        {
            THashSet<ui64> waitPathIdsToErase;
            for (auto&& i : SchemaTxBody.GetEnsureTables().GetTables()) {
                AFL_VERIFY(!owner.TablesManager.HasTable(i.GetPathId()));
                if (owner.TablesManager.HasTable(i.GetPathId(), true)) {
                    waitPathIdsToErase.emplace(i.GetPathId());
                }
            }
            if (!waitPathIdsToErase.empty()) {
                waitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(waitPathIdsToErase, GetTxId());
            }
        }
        break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable:
        {
            const auto srcPathId = SchemaTxBody.GetMoveTable().GetSrcPathId();
            const auto dstPathId = SchemaTxBody.GetMoveTable().GetDstPathId();

            AFL_VERIFY(owner.TablesManager.HasTable(srcPathId));
            AFL_VERIFY(!owner.TablesManager.HasTable(dstPathId));
            // TODO
            // WaitOnPropose = std::make_shared<TWaitTransactions>(
            //     std::move(txIds),
            //     [&](){
            //         auto txIds = owner.GetProgressTxController().GetTxs();
            //         AFL_VERIFY(txIds.size() == 1 && txIds.contains(GetTxId()))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIds));
            //         THashSet<TWriteId> writeIds;
            //         for(const auto& [writeId, data]: owner.InsertTable->GetInserted()) {
            //             if (data.PathId == srcPathId && data.PlanStep != 0) {
            //                 writeIds.insert(writeId);
            //             }
            //         }
            //         owner.Subscribers->RegisterSubscriber(std::make_shared<TWaitWrites>(
            //             std::move(writeIds),
            //             [&]() {
            //                 owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()));
            //             }
            //         ));
            //     }             
            // );
            
        }
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (waitOnPropose && !waitOnPropose->IsFinished()) {
        WaitOnPropose = waitOnPropose;
    }
    if (WaitOnPropose) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "wait_on_propose")("tx_id", GetTxId());
        owner.Subscribers->RegisterSubscriber(WaitOnPropose);
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "remove_pathes_cleaned")("tx_id", GetTxId());
        owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()));
    }
}

void TSchemaTransactionOperator::DoStartProposeOnComplete(TColumnShard& owner, const TActorContext& /*ctx*/) {
    AFL_VERIFY(!!WaitOnPropose);
    owner.Subscribers->RegisterSubscriber(WaitOnPropose);
}

}
