#include "columnshard_impl.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/backup_actor_state.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/data_events/backup_events.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

constexpr auto DefaultFreeSpace = 8 * 1024 * 1024;
constexpr auto DefaultGeneration = 0;
constexpr auto DefaultMaxChunks = 1;

class TBackupWriteController : public IWriteController, public TMonitoringObjectsCounter<TBackupWriteController, true> {
private:
    const TActorId actorID;

    void DoOnReadyResult(const NActors::TActorContext& ctx, const TBlobPutResult::TPtr& putResult) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupWriteController.DoOnReadyResult", "start");

        NOlap::TWritingBuffer bufferStub;

        auto result = std::make_unique<TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(bufferStub));
        ctx.Send(actorID, result.release());
    }
    void DoOnStartSending() override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupWriteController.DoOnStartSending", "start");
    }

public:
    TBackupWriteController(const TActorId& actorID,
                           const std::shared_ptr<NOlap::IBlobsWritingAction>& action,
                           std::shared_ptr<arrow::RecordBatch> arrowBatch)
        : actorID(actorID) {
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
};

class TBackupActor : public TActorBootstrapped<TBackupActor> {
    std::shared_ptr<NOlap::NBlobOperations::NTier::TOperator> InsertOperator;

    const TActorId SenderActorId;
    const TActorIdentity CSActorId;
    const ui64 TableId;
    const NOlap::TSnapshot Snapshot;
    const std::vector<TString> ColumnsNames;

    EBackupActorState State = EBackupActorState::Invalid;

    NKikimrSSA::TProgram ProgramProto = NKikimrSSA::TProgram();

    std::optional<NActors::TActorId> ScanActorId;

public:
    TBackupActor(std::shared_ptr<NOlap::NBlobOperations::NTier::TOperator> insertOperator, const TActorId senderActorId,
                 const TActorIdentity csActorId, const ui64 tableId, const NOlap::TSnapshot snapshot,
                 const std::vector<TString>& columnsNames)
        : InsertOperator(insertOperator)
        , SenderActorId(senderActorId)
        , CSActorId(csActorId)
        , TableId(tableId)
        , Snapshot(snapshot)
        , ColumnsNames(columnsNames) {
    }

    void Bootstrap(const TActorContext& ctx) {
        ProcessState(ctx, EBackupActorState::Init);
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

        AFL_VERIFY(State == EBackupActorState::Init);
        AFL_VERIFY(ev);

        auto& msg = ev->Get()->Record;
        ScanActorId = ActorIdFromProto(msg.GetScanActorId());

        ProcessState(ctx, EBackupActorState::Progress);
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanError::TPtr&, const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "TEvScanError");

        // @TODO discuss about error
        auto result = std::make_unique<NEvents::TBackupEvents::TEvBackupShardResult>();
        ctx.Send(SenderActorId, result.release());
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev, const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "TEvScanData");
        AFL_VERIFY(State == EBackupActorState::Progress);

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

        ProcessState(ctx, EBackupActorState::Progress);
    }

    void Handle(TEvPrivate::TEvWriteBlobsResult::TPtr&, const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TBackupActor.Handle", "TEvWriteBlobsResult");

        ProcessState(ctx, EBackupActorState::Done);

        SendBackupShardResult(ctx);
    }

private:
    void ProcessState(const TActorContext& ctx, const EBackupActorState newState) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)
        ("BackupActor.ProcessState", "change state")("from", ToString(State))("to", ToString(newState));

        State = newState;

        switch (State) {
            case EBackupActorState::Invalid: {
                break;
            }
            case EBackupActorState::Init: {
                SendScanEvent(ctx);
                break;
            }
            case EBackupActorState::Progress: {
                SendScanDataAck(ctx);
                break;
            }
            case EBackupActorState::Done: {
                SendBackupShardResult(ctx);
                break;
            }
        }
    }

    void PrepareReplyColumns() {
        for (auto&& command : *ProgramProto.MutableCommand()) {
            if (command.HasProjection()) {
                NKikimrSSA::TProgram::TProjection proj;
                for (auto& name : ColumnsNames) {
                    proj.AddColumns()->SetName(name);
                }
                *command.MutableProjection() = proj;
                return;
            }
        }
        {
            auto* command = ProgramProto.AddCommand();
            NKikimrSSA::TProgram::TProjection proj;
            for (auto& name : ColumnsNames) {
                proj.AddColumns()->SetName(name);
            }
            *command->MutableProjection() = proj;
        }
    }

    std::unique_ptr<TEvDataShard::TEvKqpScan> BuildScanEvent() const {
        auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(TableId);
        ev->Record.MutableSnapshot()->SetStep(Snapshot.GetPlanStep());
        ev->Record.MutableSnapshot()->SetTxId(Snapshot.GetTxId());

        ev->Record.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_FULL);
        ev->Record.SetTxId(Snapshot.GetTxId());

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
        AFL_VERIFY(State != EBackupActorState::Done);
        AFL_VERIFY(ScanActorId);

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("ScanActorId", ScanActorId->ToString());

        ctx.Send(*ScanActorId,
                 new NKqp::TEvKqpCompute::TEvScanDataAck(DefaultFreeSpace, DefaultGeneration, DefaultMaxChunks));
    }

    void SendBackupShardResult(const TActorContext& ctx) {
        auto ProposeResult = std::make_unique<NEvents::TBackupEvents::TEvBackupShardResult>();
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
                         const TActorId senderActorId, const TActorIdentity csActorId, const ui64 tableId,
                         const NOlap::TSnapshot snapshot, const std::vector<TString>& columnsNames) {
    return new TBackupActor(insertOperator, senderActorId, csActorId, tableId, snapshot, columnsNames);
}

}   // namespace NKikimr::NColumnShard