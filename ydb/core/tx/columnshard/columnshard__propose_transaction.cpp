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
};


bool TTxProposeTransaction::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());

    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    Self->IncCounter(COUNTER_PREPARE_REQUEST);

    auto& record = Proto(Ev->Get());
    auto txKind = record.GetTxKind();
    //ui64 ssId = record.GetSchemeShardId();
    ui64 txId = record.GetTxId();
    auto& txBody = record.GetTxBody();
    auto status = NKikimrTxColumnShard::EResultStatus::ERROR;
    TString statusMessage;

    ui64 minStep = 0;
    ui64 maxStep = Max<ui64>();

    switch (txKind) {
        case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
            TColumnShard::TAlterMeta meta;
            if (!meta.Body.ParseFromString(txBody)) {
                statusMessage = TStringBuilder()
                    << "Schema TxId# " << txId << " cannot be parsed";
                status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                break;
            }

            NOlap::ISnapshotSchema::TPtr currentSchema;
            if (Self->TablesManager.HasPrimaryIndex()) {
                currentSchema = Self->TablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema();
            }

            // Invalid body generated at a newer SchemeShard
            if (!meta.Validate(currentSchema)) {
                statusMessage = TStringBuilder()
                    << "Schema TxId# " << txId << " cannot be proposed";
                status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                break;
            }

            Y_VERIFY(record.HasSchemeShardId());
            if (Self->CurrentSchemeShardId == 0) {
                Self->CurrentSchemeShardId = record.GetSchemeShardId();
                Schema::SaveSpecialValue(db, Schema::EValueIds::CurrentSchemeShardId, Self->CurrentSchemeShardId);
            } else {
                Y_VERIFY(Self->CurrentSchemeShardId == record.GetSchemeShardId());
            }

            auto seqNo = SeqNoFromProto(meta.Body.GetSeqNo());
            auto lastSeqNo = Self->LastSchemaSeqNo;

            // Check if proposal is outdated
            if (seqNo < lastSeqNo) {
                status = NKikimrTxColumnShard::SCHEMA_CHANGED;
                statusMessage = TStringBuilder()
                    << "Ignoring outdated schema tx proposal at tablet "
                    << Self->TabletID()
                    << " txId " << txId
                    << " ssId " << Self->CurrentSchemeShardId
                    << " seqNo " << seqNo
                    << " lastSeqNo " << lastSeqNo;
                LOG_S_INFO(TxPrefix() << statusMessage << TxSuffix());
                break;
            }

            Self->UpdateSchemaSeqNo(seqNo, txc);

            // FIXME: current tests don't provide processing params!
            // Y_VERIFY_DEBUG(record.HasProcessingParams());
            if (!Self->ProcessingParams && record.HasProcessingParams()) {
                Self->ProcessingParams.emplace().CopyFrom(record.GetProcessingParams());
                Schema::SaveSpecialProtoValue(db, Schema::EValueIds::ProcessingParams, *Self->ProcessingParams);
            }

            // Always persist the latest metadata, this may include an updated seqno
            Self->ProgressTxController.RegisterTx(txId, txKind, txBody, Ev->Get()->GetSource(), Ev->Cookie, txc);

            if (!Self->AltersInFlight.contains(txId)) {
                Self->AltersInFlight.emplace(txId, std::move(meta));
            } else {
                auto& existing = Self->AltersInFlight.at(txId);
                existing.Body = std::move(meta.Body);
            }

            LOG_S_DEBUG(TxPrefix() << "schema txId " << txId << TxSuffix());

            status = NKikimrTxColumnShard::EResultStatus::PREPARED;
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT: {
            if (Self->CommitsInFlight.contains(txId)) {
                LOG_S_DEBUG(TxPrefix() << "CommitTx (retry) TxId " << txId << TxSuffix());

                auto txInfoPtr = Self->ProgressTxController.GetTxInfo(txId);
                Y_VERIFY(txInfoPtr);

                if (txInfoPtr->Source != Ev->Get()->GetSource() || txInfoPtr->Cookie != Ev->Cookie) {
                    statusMessage = TStringBuilder()
                        << "Another commit TxId# " << txId << " has already been proposed";
                    break;
                }

                maxStep = txInfoPtr->MaxStep;
                minStep = txInfoPtr->MinStep;
                status = NKikimrTxColumnShard::EResultStatus::PREPARED;
                break;
            }

            NKikimrTxColumnShard::TCommitTxBody body;
            if (!body.ParseFromString(txBody)) {
                statusMessage = TStringBuilder()
                    << "Commit TxId# " << txId << " cannot be parsed";
                break;
            }

            if (body.GetWriteIds().empty()) {
                statusMessage = TStringBuilder()
                    << "Commit TxId# " << txId << " has an empty list of write ids";
                break;
            }

            if (body.GetTxInitiator() == 0) {
                // When initiator is 0, this means it's a local write id
                // Check that all write ids actually exist
                bool failed = false;
                for (ui64 writeId : body.GetWriteIds()) {
                    if (!Self->LongTxWrites.contains(TWriteId{writeId})) {
                        statusMessage = TStringBuilder()
                            << "Commit TxId# " << txId << " references WriteId# " << writeId
                            << " that no longer exists";
                        failed = true;
                        break;
                    }
                    auto& lw = Self->LongTxWrites[TWriteId{writeId}];
                    if (lw.PreparedTxId != 0) {
                        statusMessage = TStringBuilder()
                            << "Commit TxId# " << txId << " references WriteId# " << writeId
                            << " that is already locked by TxId# " << lw.PreparedTxId;
                        failed = true;
                        break;
                    }
                }
                if (failed) {
                    break;
                }
            }

            TColumnShard::TCommitMeta meta;
            for (ui64 wId : body.GetWriteIds()) {
                TWriteId writeId{wId};
                meta.AddWriteId(writeId);
                Self->AddLongTxWrite(writeId, txId);
            }

            const auto& txInfo = Self->ProgressTxController.RegisterTxWithDeadline(txId, txKind, txBody, Ev->Get()->GetSource(), Ev->Cookie, txc);
            minStep = txInfo.MinStep;
            maxStep = txInfo.MaxStep;

            Self->CommitsInFlight.emplace(txId, std::move(meta));

            LOG_S_DEBUG(TxPrefix() << "CommitTx txId " << txId << TxSuffix());

            status = NKikimrTxColumnShard::EResultStatus::PREPARED;
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_TTL: {
            /// @note There's no tx guaranties now. For now TX_KIND_TTL is used to trigger TTL in tests only.
            /// In future we could trigger TTL outside of tablet. Then we need real tx with complete notification.
            // TODO: make real tx: save and progress with tablets restart support

            NKikimrTxColumnShard::TTtlTxBody ttlBody;
            if (!ttlBody.ParseFromString(txBody)) {
                statusMessage = "TTL tx cannot be parsed";
                status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                break;
            }

            // If no paths trigger schema defined TTL
            THashMap<ui64, NOlap::TTiering> pathTtls;
            if (!ttlBody.GetPathIds().empty()) {
                auto unixTime = TInstant::Seconds(ttlBody.GetUnixTimeSeconds());
                if (!unixTime) {
                    statusMessage = "TTL tx wrong timestamp";
                    status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                    break;
                }

                TString columnName = ttlBody.GetTtlColumnName();
                if (columnName.empty()) {
                    statusMessage = "TTL tx wrong TTL column ''";
                    status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                    break;
                }

                if (!Self->TablesManager.HasPrimaryIndex()) {
                    statusMessage = "No primary index for TTL";
                    status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                    break;
                }

                auto schema = Self->TablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema()->GetSchema();
                auto ttlColumn = schema->GetFieldByName(columnName);
                if (!ttlColumn) {
                    statusMessage = "TTL tx wrong TTL column '" + columnName + "'";
                    status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                    break;
                }

                if (statusMessage.empty()) {
                    const TInstant now = TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now();
                    for (ui64 pathId : ttlBody.GetPathIds()) {
                        NOlap::TTiering tiering;
                        tiering.Ttl = NOlap::TTierInfo::MakeTtl(now - unixTime, columnName);
                        pathTtls.emplace(pathId, std::move(tiering));
                    }
                }
            }

            if (statusMessage.empty()) {
                if (auto event = Self->SetupTtl(pathTtls, true)) {
                    if (event->NeedDataReadWrite()) {
                        ctx.Send(Self->EvictionActor, event.release());
                    } else {
                        ctx.Send(Self->SelfId(), event->TxEvent.release());
                    }
                    status = NKikimrTxColumnShard::EResultStatus::SUCCESS;
                } else {
                    statusMessage = "TTL not started";
                }
            }

            break;
        }
        default: {
            statusMessage = TStringBuilder()
                << "Unsupported TxKind# " << ui32(txKind) << " TxId# " << txId;
        }
    }

    Result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(Self->TabletID(), txKind, txId, status, statusMessage);

    if (status == NKikimrTxColumnShard::EResultStatus::PREPARED) {
        Self->IncCounter(COUNTER_PREPARE_SUCCESS);
        Result->Record.SetMinStep(minStep);
        Result->Record.SetMaxStep(maxStep);
        if (Self->ProcessingParams) {
            Result->Record.MutableDomainCoordinators()->CopyFrom(Self->ProcessingParams->GetCoordinators());
        }
    } else if (status == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        Self->IncCounter(COUNTER_PREPARE_SUCCESS);
    } else {
        Self->IncCounter(COUNTER_PREPARE_ERROR);
        LOG_S_INFO(TxPrefix() << "error txId " << txId << " " << statusMessage << TxSuffix());
    }
    return true;
}

void TTxProposeTransaction::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Result);
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());

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
