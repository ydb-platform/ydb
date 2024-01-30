#include "columnshard_impl.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/data_events/backup_events.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

class BackupActor : public TActorBootstrapped<BackupActor> {
    const TActorId SenderActorId;
    const TActorIdentity CSActorId;
    const ui64 txId;
    const ui64 planStep;
    const ui64 TableId;

    std::optional<NKikimrSSA::TProgram> ProgramProto = NKikimrSSA::TProgram();

public:
    BackupActor(const TActorId senderActorId, const TActorIdentity csActorId, const ui64 txId, const int planStep,
                const ui64 tableId)
        : SenderActorId(senderActorId)
        , CSActorId(csActorId)
        , txId(txId)
        , planStep(planStep)
        , TableId(tableId) {
    }

    void Bootstrap(const TActorContext& ctx) {
        Cerr << "call BackupActor::Bootstrap, selfID()=" << SelfId().ToString() << ", cs=" << CSActorId.ToString()
             << Endl;

        auto ev = BuildEvent();
        ctx.Send(CSActorId, ev.release());

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqpCompute::TEvScanInitActor, Handle);
            HFunc(NKqp::TEvKqpCompute::TEvScanError, Handle);
            default:
                break;
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr&, const TActorContext& ctx) {
        Cerr << "\ncall BackupActor::Handle with TEvScanInitActor" << Endl;

        auto result = std::make_unique<NEvents::TBackupEvents::TEvBackupShardProposeResult>();
        ctx.Send(SenderActorId, result.release());
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanError::TPtr&, const TActorContext& ctx) {
        Cerr << "\ncall BackupActor::Handle with TEvScanError" << Endl;

        auto result = std::make_unique<NEvents::TBackupEvents::TEvBackupShardProposeResult>();
        ctx.Send(SenderActorId, result.release());
    }

private:
    std::unique_ptr<TEvDataShard::TEvKqpScan> BuildEvent() const {
        auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(TableId);
        ev->Record.MutableSnapshot()->SetStep(planStep);
        ev->Record.MutableSnapshot()->SetTxId(txId);

        ev->Record.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_FULL);
        ev->Record.SetTxId(txId);

        // // ev->Record.SetReverse(Reverse);
        // // ev->Record.SetItemsLimit(Limit);

        ev->Record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);

        NKikimrSSA::TOlapProgram olapProgram;
        {
            TString programBytes;
            TStringOutput stream(programBytes);
            ProgramProto->SerializeToArcadiaStream(&stream);
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
};

IActor* CreatBackupActor(const TActorId senderActorId, const TActorIdentity csActorId, const ui64 txId,
                         const int planStep, const ui64 tableId) {
    return new BackupActor(senderActorId, csActorId, txId, planStep, tableId);
}

}   // namespace NKikimr::NColumnShard