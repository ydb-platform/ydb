#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/dq.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxProposeTransaction : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
public:
    TTxProposeTransaction(TColumnShard* self, TEvColumnShard::TEvProposeTransaction::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    virtual bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        Y_ABORT_UNLESS(Ev);

        txc.DB.NoMoreReadsForTx();
        NIceDb::TNiceDb db(txc.DB);

        Self->IncCounter(COUNTER_PREPARE_REQUEST);

        auto& record = Proto(Ev->Get());
        const auto txKind = record.GetTxKind();
        const ui64 txId = record.GetTxId();
        const auto& txBody = record.GetTxBody();

        if (txKind == NKikimrTxColumnShard::TX_KIND_TTL) {
            auto proposeResult = ProposeTtlDeprecated(txBody);
            Result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(Self->TabletID(), txKind, txId, proposeResult.GetStatus(), proposeResult.GetStatusMessage());
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
        auto result = Self->GetProgressTxController().ProposeTransaction(TTxController::TBasicTxInfo(txKind, txId), txBody, Ev->Get()->GetSource(), Ev->Cookie, txc);
        const auto& proposeResult = result.GetProposeResult();
        if (result.IsError()) {
            const auto& txInfo = result.GetBaseTxInfoVerified();
            Result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(Self->TabletID(), txInfo.TxKind, txInfo.TxId, proposeResult.GetStatus(), proposeResult.GetStatusMessage());
            Self->IncCounter(COUNTER_PREPARE_ERROR);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("message", proposeResult.GetStatusMessage())("tablet_id", Self->TabletID())("tx_id", txInfo.TxId);
        } else {
            const auto& txInfo = result.GetFullTxInfoVerified();
            AFL_VERIFY(proposeResult.GetStatus() == NKikimrTxColumnShard::EResultStatus::PREPARED)("tx_id", txInfo.TxId)("details", proposeResult.DebugString());
            Result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(Self->TabletID(), txInfo.TxKind, txInfo.TxId, proposeResult.GetStatus(), proposeResult.GetStatusMessage());
            Result->Record.SetMinStep(txInfo.MinStep);
            Result->Record.SetMaxStep(txInfo.MaxStep);
            if (Self->ProcessingParams) {
                Result->Record.MutableDomainCoordinators()->CopyFrom(Self->ProcessingParams->GetCoordinators());
            }
            Self->IncCounter(COUNTER_PREPARE_SUCCESS);
        }
        return true;
    }

    virtual void Complete(const TActorContext& ctx) override {
        Y_ABORT_UNLESS(Ev);
        Y_ABORT_UNLESS(Result);

        auto& record = Proto(Ev->Get());
        const ui64 txId = record.GetTxId();

        Self->GetProgressTxController().CompleteTransaction(txId, ctx);
        ctx.Send(Ev->Get()->GetSource(), Result.release());
        Self->TryRegisterMediatorTimeCast();
    }

    TTxType GetTxType() const override { return TXTYPE_PROPOSE; }

private:
    TEvColumnShard::TEvProposeTransaction::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> Result;

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

}
