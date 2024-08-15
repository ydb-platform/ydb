#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/dq.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxProposeTransaction: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
    std::optional<TTxController::TTxInfo> TxInfo;

public:
    TTxProposeTransaction(TColumnShard* self, TEvColumnShard::TEvProposeTransaction::TPtr& ev)
        : TBase(self)
        , Ev(ev) {
        AFL_VERIFY(!!Ev);
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        txc.DB.NoMoreReadsForTx();
        NIceDb::TNiceDb db(txc.DB);

        Self->Counters.GetTabletCounters()->IncCounter(COUNTER_PREPARE_REQUEST);

        auto& record = Proto(Ev->Get());
        const auto txKind = record.GetTxKind();
        const ui64 txId = record.GetTxId();
        const auto& txBody = record.GetTxBody();
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("tablet_id", Self->TabletID())("tx_id", txId)("this", (ui64)this);

        if (txKind == NKikimrTxColumnShard::TX_KIND_TTL) {
            auto proposeResult = ProposeTtlDeprecated(txBody);
            auto reply = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
                Self->TabletID(), txKind, txId, proposeResult.GetStatus(), proposeResult.GetStatusMessage());
            ctx.Send(Ev->Sender, reply.release());
            return true;
        }

        if (!Self->ProcessingParams && record.HasProcessingParams()) {
            Self->ProcessingParams.emplace().CopyFrom(record.GetProcessingParams());
            Schema::SaveSpecialProtoValue(db, Schema::EValueIds::ProcessingParams, *Self->ProcessingParams);
        }

        if (record.HasSchemeShardId()) {
            if (Self->CurrentSchemeShardId == 0) {
                Self->CurrentSchemeShardId = record.GetSchemeShardId();
                Schema::SaveSpecialValue(db, Schema::EValueIds::CurrentSchemeShardId, Self->CurrentSchemeShardId);
            } else {
                Y_ABORT_UNLESS(Self->CurrentSchemeShardId == record.GetSchemeShardId());
            }
        }
        std::optional<TMessageSeqNo> msgSeqNo;
        if (Ev->Get()->Record.HasSeqNo()) {
            TMessageSeqNo seqNo;
            seqNo.DeserializeFromProto(Ev->Get()->Record.GetSeqNo()).Validate();
            msgSeqNo = seqNo;
        } else if (txKind == NKikimrTxColumnShard::TX_KIND_SCHEMA) {
            // deprecated. alive while all branches in SS not updated in new flow
            NKikimrTxColumnShard::TSchemaTxBody schemaTxBody;
            if (schemaTxBody.ParseFromString(txBody)) {
                msgSeqNo = SeqNoFromProto(schemaTxBody.GetSeqNo());
            }
        }
        TxInfo.emplace(txKind, txId, Ev->Get()->GetSource(), Ev->Cookie, msgSeqNo);
        TxOperator = Self->GetProgressTxController().StartProposeOnExecute(*TxInfo, txBody, txc);
        return true;
    }

    virtual void Complete(const TActorContext& ctx) override {
        auto& record = Proto(Ev->Get());
        if (record.GetTxKind() == NKikimrTxColumnShard::TX_KIND_TTL) {
            return;
        }
        AFL_VERIFY(!!TxOperator);
        AFL_VERIFY(!!TxInfo);
        const ui64 txId = record.GetTxId();
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("tablet_id", Self->TabletID())("request_tx", TxInfo->DebugString())(
            "this", (ui64)this)("op_tx", TxOperator->GetTxInfo().DebugString());

        if (TxOperator->IsFail()) {
            TxOperator->SendReply(*Self, ctx);
        } else {
            auto internalOp = Self->GetProgressTxController().GetVerifiedTxOperator(TxOperator->GetTxId());
            NActors::TLogContextGuard lGuardTx = NActors::TLogContextBuilder::Build()("int_op_tx", internalOp->GetTxInfo().DebugString());
            if (!TxOperator->CheckTxInfoForReply(*TxInfo)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "deprecated tx operator");
                return;
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "actual tx operator");
            }
            if (TxOperator->IsAsync()) {
                Self->GetProgressTxController().StartProposeOnComplete(txId, ctx);
            } else {
                Self->GetProgressTxController().FinishProposeOnComplete(txId, ctx);
            }
        }

        Self->TryRegisterMediatorTimeCast();
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE;
    }

private:
    TEvColumnShard::TEvProposeTransaction::TPtr Ev;
    std::shared_ptr<TTxController::ITransactionOperator> TxOperator;

    TTxController::TProposeResult ProposeTtlDeprecated(const TString& txBody) {
        /// @note There's no tx guaranties now. For now TX_KIND_TTL is used to trigger TTL in tests only.
        /// In future we could trigger TTL outside of tablet. Then we need real tx with complete notification.
        // TODO: make real tx: save and progress with tablets restart support

        NKikimrTxColumnShard::TTtlTxBody ttlBody;
        if (!ttlBody.ParseFromString(txBody)) {
            return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TTL tx cannot be parsed");
        }

        // If no paths trigger schema defined TTL
        THashMap<ui64, NOlap::TTiering> pathTtls;
        if (!ttlBody.GetPathIds().empty()) {
            auto unixTime = TInstant::Seconds(ttlBody.GetUnixTimeSeconds());
            if (!unixTime) {
                return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TTL tx wrong timestamp");
            }

            TString columnName = ttlBody.GetTtlColumnName();
            if (columnName.empty()) {
                return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TTL tx wrong TTL column ''");
            }

            if (!Self->TablesManager.HasPrimaryIndex()) {
                return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "No primary index for TTL");
            }

            auto schema = Self->TablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema()->GetSchema();
            auto ttlColumn = schema->GetFieldByName(columnName);
            if (!ttlColumn) {
                return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TTL tx wrong TTL column '" + columnName + "'");
            }

            const TInstant now = TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now();
            for (ui64 pathId : ttlBody.GetPathIds()) {
                NOlap::TTiering tiering;
                AFL_VERIFY(tiering.Add(NOlap::TTierInfo::MakeTtl(now - unixTime, columnName)));
                pathTtls.emplace(pathId, std::move(tiering));
            }
        }
        if (!Self->SetupTtl(pathTtls)) {
            return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TTL not started");
        }
        Self->TablesManager.MutablePrimaryIndex().OnTieringModified(Self->Tiers, Self->TablesManager.GetTtl(), {});

        return TTxController::TProposeResult();
    }
};

void TColumnShard::Handle(TEvColumnShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxProposeTransaction(this, ev), ctx);
}

}   // namespace NKikimr::NColumnShard
