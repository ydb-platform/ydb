#include "columnshard_impl.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/data_events/backup_events.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

constexpr auto DefaultFreeSpace = 8 * 1024 * 1024;
constexpr auto DefaultGeneration = 0;
constexpr auto DefaultMaxChunks = 1;

enum class BackupActorState : ui8 {
    Invalid,
    Init,
    Progress,
    Done
};

std::string ToString(BackupActorState s) {
    switch (s) {
        case BackupActorState::Invalid: {
            return "Invalid";
        }
        case BackupActorState::Init: {
            return "Init";
        }
        case BackupActorState::Progress: {
            return "Progress";
        }
        case BackupActorState::Done: {
            return "Done";
        }
    }
}

class TBackupWriteController : public NColumnShard::IWriteController, public NColumnShard::TMonitoringObjectsCounter<TBackupWriteController, true> {
private:
    const TActorId actorID;

    NBlobCache::TUnifiedBlobId BlobId;

    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupWriteController.DoOnReadyResult", "start");

        NOlap::TWritingBuffer bufferStub;

        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(bufferStub));
        ctx.Send(actorID, result.release());
    }
    void DoOnStartSending() override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupWriteController.DoOnStartSending", "start");
    }

public:
    TBackupWriteController(const TActorId& actorID, 
        // @TODO action with blob
        const std::shared_ptr<NOlap::IBlobsWritingAction>& action, 
        std::shared_ptr<arrow::RecordBatch> arrowBatch
    ) : actorID(actorID) {

        NArrow::TBatchSplitttingContext splitCtx(NColumnShard::TLimits::GetMaxBlobSize());

        NArrow::TSerializedBatch batch = NArrow::TSerializedBatch::Build(arrowBatch, splitCtx);


        NOlap::TWritingBlob currentBlob;

        // @TODO stub
        NOlap::TWriteAggregation aggreagtion(nullptr);

        NOlap::TWideSerializedBatch wideBatch(std::move(batch), aggreagtion);

        currentBlob.AddData(wideBatch);

        auto& task = AddWriteTask(NOlap::TBlobWriteInfo::BuildWriteTask(currentBlob.GetBlobData(), action));

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupWriteController.ctor", "create")("task", task.GetBlobId());

        currentBlob.InitBlobId(task.GetBlobId());
    }

    NBlobCache::TUnifiedBlobId GetBlobID() const noexcept { return BlobId; }
};

class TBackupActor : public TActorBootstrapped<TBackupActor> {
    std::shared_ptr<NOlap::NBlobOperations::NTier::TOperator> InsertOperator;

    const TActorId SenderActorId;
    const TActorIdentity CSActorId;
    const ui64 TxId;
    const ui64 PlanStep;
    const ui64 TableId;

    BackupActorState State = BackupActorState::Invalid;

    NKikimrSSA::TProgram ProgramProto = NKikimrSSA::TProgram();

    // @TODO think about columns
    const std::vector<TString> replyColumns{"key", "field"};

    std::optional<NActors::TActorId> ScanActorId;

public:
    TBackupActor(std::shared_ptr<NOlap::NBlobOperations::NTier::TOperator> insertOperator, 
                const TActorId senderActorId, 
                const TActorIdentity csActorId, 
                const ui64 txId, 
                const int planStep,
                const ui64 tableId)
        : InsertOperator(insertOperator)
        , SenderActorId(senderActorId)
        , CSActorId(csActorId)
        , TxId(txId)
        , PlanStep(planStep)
        , TableId(tableId) {
    }

    void Bootstrap(const TActorContext& ctx) {
        ProcessState(ctx, BackupActorState::Init);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqpCompute::TEvScanInitActor, Handle);
            HFunc(NKqp::TEvKqpCompute::TEvScanError, Handle);
            HFunc(NKqp::TEvKqpCompute::TEvScanData, Handle);
            HFunc(TEvPrivate::TEvWriteBlobsResult, Handle);
            default:
                break;
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev, const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "creaTEvScanInitActorte");

        AFL_VERIFY(State == BackupActorState::Init);
        AFL_VERIFY(ev);

        auto& msg = ev->Get()->Record;
        ScanActorId = ActorIdFromProto(msg.GetScanActorId());

        ProcessState(ctx, BackupActorState::Progress);
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanError::TPtr&, const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "TEvScanError");

        auto result = std::make_unique<NEvents::TBackupEvents::TEvBackupShardProposeResult>();
        ctx.Send(SenderActorId, result.release());
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev, const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "TEvScanData");
        AFL_VERIFY(State == BackupActorState::Progress);

        auto batch = ev->Get()->ArrowBatch;
        if (batch) {
            NArrow::TStatusValidator::Validate(batch->ValidateFull());
            LoadBatchToStorage(ctx, batch);
        } else {
            AFL_VERIFY(ev->Get()->Finished);
        }

        if (ev->Get()->Finished) {
            AFL_VERIFY(ev->Get()->StatsOnFinished);
            // ResultStats = ev->Get()->StatsOnFinished->GetMetrics();

            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "finished");
            return;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "not finished, send ack");

        ProcessState(ctx, BackupActorState::Progress);
    }
    
    void Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& , const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "TEvWriteBlobsResult");

        ProcessState(ctx, BackupActorState::Done);

        SendBackupShardProposeResult(ctx);
    }

private:
    void ProcessState(const TActorContext& ctx, const BackupActorState newState) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("BackupActor.ProcessState", "change state")
            ("from", ToString(State))("to", ToString(newState));

        State = newState;

        switch (State) {
            case BackupActorState::Invalid: {
                break;
            }
            case BackupActorState::Init: {
                SendScanEvent(ctx);
                break;
            }
            case BackupActorState::Progress: {
                SendScanDataAck(ctx);
                break;
            }
            case BackupActorState::Done: {
                SendBackupShardProposeResult(ctx);
                break;
            }
        }
    }

    void PrepareReplyColumns() {
        for (auto&& command : *ProgramProto.MutableCommand()) {
            if (command.HasProjection()) {
                NKikimrSSA::TProgram::TProjection proj;
                for (auto&& i : replyColumns) {
                    proj.AddColumns()->SetName(i);
                }
                *command.MutableProjection() = proj;
                return;
            }
        }
        {
            auto* command = ProgramProto.AddCommand();
            NKikimrSSA::TProgram::TProjection proj;
            for (auto&& i : replyColumns) {
                proj.AddColumns()->SetName(i);
            }
            *command->MutableProjection() = proj;
        }
    }

    std::unique_ptr<TEvDataShard::TEvKqpScan> BuildScanEvent() const {
        auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(TableId);
        ev->Record.MutableSnapshot()->SetStep(PlanStep);
        ev->Record.MutableSnapshot()->SetTxId(TxId);

        ev->Record.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_FULL);
        ev->Record.SetTxId(TxId);

        // // ev->Record.SetReverse(Reverse);
        // // ev->Record.SetItemsLimit(Limit);

        ev->Record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);

        NKikimrSSA::TOlapProgram olapProgram;
        {
            TString programBytes;
            TStringOutput stream(programBytes);
            ProgramProto.SerializeToArcadiaStream(&stream);
            olapProgram.SetProgram(programBytes);
        }
        {
            TString programBytes;
            TStringOutput stream(programBytes);
            olapProgram.SerializeToArcadiaStream(&stream);
            ev->Record.SetOlapProgram(programBytes);
        }
        ev->Record.SetOlapProgramType(NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS);

        return ev;
    }

    void SendScanEvent(const TActorContext& ctx) {
        PrepareReplyColumns();
        auto ev = BuildScanEvent();
        ctx.Send(CSActorId, ev.release());
    }

    void SendScanDataAck(const TActorContext& ctx) {
        AFL_VERIFY(State != BackupActorState::Done);
        AFL_VERIFY(ScanActorId);

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("ScanActorId", ScanActorId->ToString());

        ctx.Send(*ScanActorId,
                 new NKqp::TEvKqpCompute::TEvScanDataAck(DefaultFreeSpace, DefaultGeneration, DefaultMaxChunks));
    }

    void SendBackupShardProposeResult(const TActorContext& ctx) {
        auto ProposeResult = std::make_unique<NEvents::TBackupEvents::TEvBackupShardProposeResult>();
        ctx.Send(SenderActorId, ProposeResult.release());
    }

    void LoadBatchToStorage(const TActorContext& ctx, std::shared_ptr<arrow::RecordBatch> arrowBatch) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("BackupActor.LoadBatchToStorage", "start");
        
        auto action = InsertOperator->StartWritingAction("BACKUP:WRITING");
        auto writeController = std::make_shared<TBackupWriteController>(SelfId(), action, arrowBatch);
        ctx.Register(CreateWriteActor(TableId, writeController, TInstant::Max()));

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("SendBackupShardProposeResult", "actor registred");
    }
};

IActor* CreatBackupActor(std::shared_ptr<NOlap::NBlobOperations::NTier::TOperator> insertOperator, 
                        const TActorId senderActorId, 
                        const TActorIdentity csActorId, 
                        const ui64 txId,
                        const int planStep, 
                        const ui64 tableId) {
    return new TBackupActor(insertOperator, senderActorId, csActorId, txId, planStep, tableId);
}

}   // namespace NKikimr::NColumnShard