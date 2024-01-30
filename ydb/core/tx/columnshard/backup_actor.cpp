#include "columnshard_impl.h"

#include <ydb/core/tx/data_events/backup_events.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

class BackupActor : public TActorBootstrapped<BackupActor> {
    [[maybe_unused]] const TPathId targetPathId;
    [[maybe_unused]] const ui32 shardNum;

public:
    BackupActor([[maybe_unused]] const TPathId& targetPathId, const ui32 shardNum)
        : targetPathId(targetPathId)
        , shardNum(shardNum) {
    }

    void Bootstrap([[maybe_unused]] const TActorContext& ctx) {
        Become(&TThis::StateWork);
    }

    void Handle(NEvents::TBackupEvents::TEvBackupShardPropose::TPtr& ev, const TActorContext& ctx) {
        ACFL_WARN("event", "TEvProposeTransaction");
        // const auto txKind = NKikimrTxColumnShard::ETransactionKind::TX_KIND_COMMIT;

        // auto& record = Proto(ev->Get());
        // const auto txKind = record.GetTxKind();
        // const auto txKind = NKikimrTxColumnShard::ETransactionKind::TX_KIND_COMMIT;

        // const ui64 txId = record.GetTxId();

        constexpr auto status = NKikimrBackupEvents::TEvBackupShardProposeResult::STATUS_PREPARED;
        auto result = MakeHolder<NEvents::TBackupEvents::TEvBackupShardProposeResult>(status);

        ctx.Send(ev->Get()->GetSource(), result.Release());
    }

    STFUNC(StateWork) {
        NActors::TLogContextGuard logGuard =
            NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("shard_num", shardNum);

        switch (ev->GetTypeRewrite()) { HFunc(NEvents::TBackupEvents::TEvBackupShardPropose, Handle); }
    }
};

IActor* CreateWriteActor(const TPathId& targetPathId, const ui32 shardNum) {
    return new BackupActor(targetPathId, shardNum);
}

}   // namespace NKikimr::NColumnShard