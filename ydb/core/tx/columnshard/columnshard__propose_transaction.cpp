#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/dq.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxProposeTransaction : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxProposeTransaction(TColumnShard* self, TEvColumnShard::TEvProposeTransaction::TPtr& ev)
        : TBase(self)
        , Ev(ev)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_PROPOSE; }

private:
    TEvColumnShard::TEvProposeTransaction::TPtr Ev;
    const ui32 TabletTxNo;
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> Result;

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxProposeTransaction[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }

    void ConstructResult(TTxController::TProposeResult& proposeResult, const TTxController::TBasicTxInfo& txInfo);
    TTxController::TProposeResult ProposeTtlDeprecated(const TString& txBody);
};


bool TTxProposeTransaction::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Y_ABORT_UNLESS(Ev);
    LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());

    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    Self->IncCounter(COUNTER_PREPARE_REQUEST);

    auto& record = Proto(Ev->Get());
    auto txKind = record.GetTxKind();
    ui64 txId = record.GetTxId();
    auto& txBody = record.GetTxBody();

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

    TTxController::TBasicTxInfo fakeTxInfo;
    fakeTxInfo.TxId = txId;
    fakeTxInfo.TxKind = txKind;

    auto txOperator = TTxController::ITransactionOperatior::TFactory::MakeHolder(txKind, fakeTxInfo);
    if (!txOperator || !txOperator->Parse(txBody)) {
        TTxController::TProposeResult proposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, TStringBuilder() << "Error processing commit TxId# " << txId
                                                << (txOperator ? ". Parsing error " : ". Unknown operator for txKind"));
        ConstructResult(proposeResult, fakeTxInfo);
        return true;
    }

    auto txInfoPtr = Self->ProgressTxController->GetTxInfo(txId);
    if (!!txInfoPtr) {
        if (txInfoPtr->Source != Ev->Get()->GetSource() || txInfoPtr->Cookie != Ev->Cookie) {
            TTxController::TProposeResult proposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, TStringBuilder() << "Another commit TxId# " << txId << " has already been proposed");
            ConstructResult(proposeResult, fakeTxInfo);
        }
        TTxController::TProposeResult proposeResult;
        ConstructResult(proposeResult, *txInfoPtr);
    } else {
        auto proposeResult = txOperator->Propose(*Self, txc, false);
        if (!!proposeResult) {
            const auto& txInfo = txOperator->TxWithDeadline() ? Self->ProgressTxController->RegisterTxWithDeadline(txId, txKind, txBody, Ev->Get()->GetSource(), Ev->Cookie, txc)
                                                              : Self->ProgressTxController->RegisterTx(txId, txKind, txBody, Ev->Get()->GetSource(), Ev->Cookie, txc);

            ConstructResult(proposeResult, txInfo);
        } else {
            ConstructResult(proposeResult, fakeTxInfo);
        }
    }
    AFL_VERIFY(!!Result);
    return true;
}

TTxController::TProposeResult TTxProposeTransaction::ProposeTtlDeprecated(const TString& txBody) {
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
            return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR,  "TTL tx wrong TTL column '" + columnName + "'");
        }

        const TInstant now = TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now();
        for (ui64 pathId : ttlBody.GetPathIds()) {
            NOlap::TTiering tiering;
            tiering.Ttl = NOlap::TTierInfo::MakeTtl(now - unixTime, columnName);
            pathTtls.emplace(pathId, std::move(tiering));
        }
    }
    if (!Self->SetupTtl(pathTtls, true)) {
        return TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TTL not started");
    }

    return TTxController::TProposeResult();
}

void TTxProposeTransaction::ConstructResult(TTxController::TProposeResult& proposeResult, const TTxController::TBasicTxInfo& txInfo) {
    Result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(Self->TabletID(), txInfo.TxKind, txInfo.TxId, proposeResult.GetStatus(), proposeResult.GetStatusMessage());
    if (proposeResult.GetStatus() == NKikimrTxColumnShard::EResultStatus::PREPARED) {
        Self->IncCounter(COUNTER_PREPARE_SUCCESS);
        Result->Record.SetMinStep(txInfo.MinStep);
        Result->Record.SetMaxStep(txInfo.MaxStep);
        if (Self->ProcessingParams) {
            Result->Record.MutableDomainCoordinators()->CopyFrom(Self->ProcessingParams->GetCoordinators());
        }
    } else if (proposeResult.GetStatus() == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        Self->IncCounter(COUNTER_PREPARE_SUCCESS);
    } else {
        Self->IncCounter(COUNTER_PREPARE_ERROR);
        LOG_S_INFO(TxPrefix() << "error txId " << txInfo.TxId << " " << proposeResult.GetStatusMessage() << TxSuffix());
    }
}

void TTxProposeTransaction::Complete(const TActorContext& ctx) {
    Y_ABORT_UNLESS(Ev);
    Y_ABORT_UNLESS(Result);
    ctx.Send(Ev->Get()->GetSource(), Result.release());
    Self->TryRegisterMediatorTimeCast();
}


void TColumnShard::Handle(TEvColumnShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx) {
    auto& record = Proto(ev->Get());
    auto txKind = record.GetTxKind();
    ui64 txId = record.GetTxId();
    LOG_S_DEBUG("ProposeTransaction " << NKikimrTxColumnShard::ETransactionKind_Name(txKind)
        << " txId " << txId << " at tablet " << TabletID());

    Execute(new TTxProposeTransaction(this, ev), ctx);
}

}
