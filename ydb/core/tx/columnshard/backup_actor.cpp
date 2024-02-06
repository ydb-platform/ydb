#include "columnshard_impl.h"

#include <ydb/core/tx/data_events/backup_events.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

class BackupActor : public TActorBootstrapped<BackupActor> {
    [[maybe_unused]]
    // const ui64 tabletId;
    const TActorId tabletId;

    const TPathId targetPathId;
    const ui64 planStep{};
    const ui64 txId{};

    std::optional<NKikimrSSA::TProgram> ProgramProto = NKikimrSSA::TProgram();

public:
    BackupActor(const TActorId tabletId, const TPathId& targetPathId, const ui64 planStep, const ui64 txId)
        : tabletId(tabletId)
        , targetPathId(targetPathId)
        , planStep(planStep)
        , txId(txId) {
    }

    void Bootstrap([[maybe_unused]] const TActorContext& ctx) {
        Become(&TThis::StateWork);
    }

    void Handle([[maybe_unused]] NEvents::TBackupEvents::TEvBackupShardPropose::TPtr& ev, [[maybe_unused]] const TActorContext& ctx) {
        ACFL_WARN("event", "TEvProposeTransaction");
    }

    STFUNC(StateWork) {
        NActors::TLogContextGuard logGuard =
            NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("txId", txId);

        switch (ev->GetTypeRewrite()) { HFunc(NEvents::TBackupEvents::TEvBackupShardPropose, Handle); }
    }

    void SendEvent() {
        // auto ev = BuildEvent();
        // Send(ev.release());
    }

private:
    std::unique_ptr<TEvDataShard::TEvKqpScan> BuildEvent() const {
        auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(targetPathId.LocalPathId);
        ev->Record.MutableSnapshot()->SetStep(planStep);
        ev->Record.MutableSnapshot()->SetTxId(txId);

        ev->Record.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_FULL);
        ev->Record.SetTxId(txId);

        // ev->Record.SetReverse(Reverse);
        // ev->Record.SetItemsLimit(Limit);

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
        ev->Record.SetOlapProgramType(
            NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS
        );

        return ev;
    }
};

IActor* CreatBackupActor(const TActorId tabletId, const TPathId& targetPathId, const ui64 planStep, const ui64 txId) {
    return new BackupActor(tabletId, targetPathId, planStep, txId);
}

}   // namespace NKikimr::NColumnShard