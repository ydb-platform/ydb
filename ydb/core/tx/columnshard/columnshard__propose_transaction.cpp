#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxProposeTransaction: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
    TEvColumnShard::TEvProposeTransaction::TPtr Ev;
    std::shared_ptr<TTxController::ITransactionOperator> TxOperator;
    std::optional<TTxController::TTxInfo> TxInfo;

public:
    TTxProposeTransaction(TColumnShard* self, TEvColumnShard::TEvProposeTransaction::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {
        AFL_VERIFY(!!Ev);
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& /* ctx */) override {
        txc.DB.NoMoreReadsForTx();
        NIceDb::TNiceDb db(txc.DB);

        Self->Counters.GetTabletCounters()->IncCounter(COUNTER_PREPARE_REQUEST);

        auto& record = Proto(Ev->Get());
        const auto txKind = record.GetTxKind();
        const ui64 txId = record.GetTxId();
        const auto& txBody = record.GetTxBody();
        NActors::TLogContextGuard lGuard =
            NActors::TLogContextBuilder::Build()("tablet_id", Self->TabletID())("tx_id", txId)("this", (ui64)this);

        if (!Self->ProcessingParams && record.HasProcessingParams()) {
            Self->ProcessingParams.emplace().CopyFrom(record.GetProcessingParams());
            Schema::SaveSpecialProtoValue(db, Schema::EValueIds::ProcessingParams, *Self->ProcessingParams);
        }

        if (record.HasSchemeShardId()) {
            if (Self->CurrentSchemeShardId == 0) {
                Self->CurrentSchemeShardId = record.GetSchemeShardId();
                Schema::SaveSpecialValue(db, Schema::EValueIds::CurrentSchemeShardId, Self->CurrentSchemeShardId);
            } else {
                AFL_VERIFY(Self->CurrentSchemeShardId == record.GetSchemeShardId());
            }
            if (txKind == NKikimrTxColumnShard::TX_KIND_SCHEMA) {
                if (record.HasSubDomainPathId()) {
                    ui64 subDomainPathId = record.GetSubDomainPathId();
                    YDB_LOG_DEBUG("",
                        {"event", "propose"},
                        {"subdomain_id", subDomainPathId});
                    Self->SpaceWatcher->PersistSubDomainPathId(subDomainPathId, txc);
                    Self->SpaceWatcher->StartWatchingSubDomainPathId();
                } else {
                    Self->SpaceWatcher->StartFindSubDomainPathId();
                }
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
        TxInfo.emplace(txKind, txId, Ev->Get()->GetSource(), Self->GetProgressTxController().GetAllowedStep(), Ev->Cookie, msgSeqNo);
        TxOperator = Self->GetProgressTxController().StartProposeOnExecute(*TxInfo, txBody, txc);
        return true;
    }

    virtual void Complete(const TActorContext& ctx) override {
        auto& record = Proto(Ev->Get());
        AFL_VERIFY(!!TxOperator);
        AFL_VERIFY(!!TxInfo);
        const ui64 txId = record.GetTxId();
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("tablet_id", Self->TabletID())(
            "request_tx", TxInfo->DebugString())("this", (ui64)this)("op_tx", TxOperator->GetTxInfo().DebugString());

        Self->TryRegisterMediatorTimeCast();

        if (TxOperator->IsFail()) {
            TxOperator->SendReply(*Self, ctx);
            return;
        }
        auto internalOp = Self->GetProgressTxController().GetTxOperatorOptional(txId);
        if (!internalOp) {
            YDB_LOG_WARN("",
                {"event", "removed tx operator"});
            return;
        }
        NActors::TLogContextGuard lGuardTx =
            NActors::TLogContextBuilder::Build()("int_op_tx", internalOp->GetTxInfo().DebugString())("int_this", (ui64)internalOp.get());
        if (!internalOp->CheckTxInfoForReply(*TxInfo)) {
            YDB_LOG_WARN("",
                {"event", "deprecated tx operator"});
            return;
        }

        YDB_LOG_DEBUG("",
            {"event", "actual tx operator"});
        if (internalOp->IsAsync()) {
            Self->GetProgressTxController().StartProposeOnComplete(*internalOp, ctx);
        } else {
            Self->GetProgressTxController().FinishProposeOnComplete(*internalOp, ctx);
        }
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE;
    }

private:
};

void TColumnShard::Handle(TEvColumnShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxProposeTransaction(this, ev), ctx);
}

}   // namespace NKikimr::NColumnShard
