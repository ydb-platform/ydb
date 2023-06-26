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
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_PROPOSE; }

private:
    TEvColumnShard::TEvProposeTransaction::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> Result;
};


bool TTxProposeTransaction::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxProposeTransaction.Execute at tablet " << Self->TabletID());

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
                LOG_S_INFO(statusMessage);
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
            auto& txInfo = Self->BasicTxInfo[txId];
            txInfo.TxId = txId;
            txInfo.TxKind = txKind;
            txInfo.Source = Ev->Get()->GetSource();
            txInfo.Cookie = Ev->Cookie;
            Schema::SaveTxInfo(db, txInfo.TxId, txInfo.TxKind, txBody, txInfo.MaxStep, txInfo.Source, txInfo.Cookie);

            if (!Self->AltersInFlight.contains(txId)) {
                Self->AltersInFlight.emplace(txId, std::move(meta));
            } else {
                auto& existing = Self->AltersInFlight.at(txId);
                existing.Body = std::move(meta.Body);
            }

            LOG_S_DEBUG("TTxProposeTransaction schema txId " << txId << " at tablet " << Self->TabletID());

            status = NKikimrTxColumnShard::EResultStatus::PREPARED;
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT: {
            if (Self->CommitsInFlight.contains(txId)) {
                LOG_S_DEBUG("TTxProposeTransaction CommitTx (retry) TxId " << txId << " at tablet " << Self->TabletID());

                Y_VERIFY(Self->BasicTxInfo.contains(txId));
                const auto& txInfo = Self->BasicTxInfo[txId];

                if (txInfo.Source != Ev->Get()->GetSource() || txInfo.Cookie != Ev->Cookie) {
                    statusMessage = TStringBuilder()
                        << "Another commit TxId# " << txId << " has already been proposed";
                    break;
                }

                maxStep = txInfo.MaxStep;
                minStep = maxStep - Self->MaxCommitTxDelay.MilliSeconds(); // TODO: improve this code
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

            minStep = Self->GetAllowedStep();
            maxStep = minStep + Self->MaxCommitTxDelay.MilliSeconds();

            TColumnShard::TCommitMeta meta;
            meta.MetaShard = body.GetTxInitiator();
            for (ui64 wId : body.GetWriteIds()) {
                TWriteId writeId{wId};
                meta.AddWriteId(writeId);
                if (meta.MetaShard == 0) {
                    Self->AddLongTxWrite(writeId, txId);
                }
            }

            auto& txInfo = Self->BasicTxInfo[txId];
            txInfo.TxId = txId;
            txInfo.TxKind = txKind;
            txInfo.MaxStep = maxStep;
            txInfo.Source = Ev->Get()->GetSource();
            txInfo.Cookie = Ev->Cookie;
            Schema::SaveTxInfo(db, txInfo.TxId, txInfo.TxKind, txBody, txInfo.MaxStep, txInfo.Source, txInfo.Cookie);

            Self->CommitsInFlight.emplace(txId, std::move(meta));

            Self->DeadlineQueue.emplace(txInfo.MaxStep, txId);

            LOG_S_DEBUG("TTxProposeTransaction CommitTx txId " << txId << " at tablet " << Self->TabletID());

            status = NKikimrTxColumnShard::EResultStatus::PREPARED;
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_DATA: {
            NKikimrTxDataShard::TDataTransaction dataTransaction;
            Y_VERIFY(dataTransaction.ParseFromString(record.GetTxBody()));

            LOG_S_DEBUG("TTxProposeTransaction immediate data tx txId " << txId
                << " '" << dataTransaction.DebugString()
                << "' at tablet " << Self->TabletID());

            bool isImmediate = record.GetFlags() & NKikimrTxColumnShard::ETransactionFlag::TX_FLAG_IMMEDIATE;
            if (isImmediate) {
                for (auto&& task : dataTransaction.GetKqpTransaction().GetTasks()) {
                    for (auto&& o : task.GetOutputs()) {
                        for (auto&& c : o.GetChannels()) {
                            TActorId actorId(c.GetDstEndpoint().GetActorId().GetRawX1(),
                                             c.GetDstEndpoint().GetActorId().GetRawX2());
                            NYql::NDqProto::TEvComputeChannelData evProto;
                            evProto.MutableChannelData()->SetChannelId(c.GetId());
                            evProto.MutableChannelData()->SetFinished(true);
                            evProto.SetNoAck(true);
                            evProto.SetSeqNo(1);
                            auto ev = std::make_unique<NYql::NDq::TEvDqCompute::TEvChannelData>();
                            ev->Record = evProto;
                            ctx.Send(actorId, ev.release());
                        }
                    }
                }
                status = NKikimrTxColumnShard::EResultStatus::SUCCESS;
            } else {
#if 0 // TODO
                minStep = Self->GetAllowedStep();
                maxStep = minStep + Self->MaxCommitTxDelay.MilliSeconds();
                auto& txInfo = Self->BasicTxInfo[txId];
                txInfo.TxId = txId;
                txInfo.TxKind = txKind;
                txInfo.Source = Ev->Get()->GetSource();
                txInfo.Cookie = Ev->Cookie;
                Schema::SaveTxInfo(db, txInfo.TxId, txInfo.TxKind, txBody, txInfo.MaxStep, txInfo.Source, txInfo.Cookie);
#endif
                statusMessage = TStringBuilder() << "Planned data tx is not supported at ColumnShard txId "
                    << txId << " '" << dataTransaction.DebugString() << "'";
                //status = NKikimrTxColumnShard::EResultStatus::PREPARED;
            }
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

                auto schema = Self->TablesManager.GetIndexInfo().ArrowSchema();
                auto ttlColumn = schema->GetFieldByName(columnName);
                if (!ttlColumn) {
                    statusMessage = "TTL tx wrong TTL column '" + columnName + "'";
                    status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
                    break;
                }

                if (statusMessage.empty()) {
                    for (ui64 pathId : ttlBody.GetPathIds()) {
                        NOlap::TTiering tiering;
                        tiering.Ttl = NOlap::TTierInfo::MakeTtl(unixTime, columnName);
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
        LOG_S_INFO("TTxProposeTransaction error txId " << txId << " at tablet " << Self->TabletID()
            << " " << statusMessage);
    }
    return true;
}

void TTxProposeTransaction::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Result);
    LOG_S_DEBUG("TTxProposeTransaction.Complete at tablet " << Self->TabletID());

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
