#pragma once

#include "datashard_ut_common.h"

namespace NKikimr::NDataShard::NTest {

inline NKikimrTxDataShard::TEvDatabaseFreezeResult SendDatabaseFreeze(
        TTestActorRuntime& runtime, ui64 shard, ui64 schemeShard, const TPathId& domain,
        ui64 epoch, NKikimrTxDataShard::TDatabaseFreezeState::EStage stage)
{
    const auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvDatabaseFreeze>();
    request->Record.SetEpoch(epoch);
    request->Record.SetSchemeShardId(schemeShard);
    request->Record.SetDomainOwnerId(domain.OwnerId);
    request->Record.SetDomainLocalPathId(domain.LocalPathId);
    request->Record.SetStage(stage);
    runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());
    return runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDatabaseFreezeResult>(sender)->Get()->Record;
}

inline void WaitDatabaseFreezeStage(TTestActorRuntime& runtime, ui64 shard,
        ui64 schemeShard, const TPathId& domain, ui64 epoch,
        NKikimrTxDataShard::TDatabaseFreezeState::EStage stage)
{
    for (ui32 attempt = 0; attempt < 300; ++attempt) {
        const auto result = SendDatabaseFreeze(runtime, shard, schemeShard, domain, epoch, stage);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), result.ShortDebugString());
        if (result.GetReady()) {
            return;
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }
    UNIT_FAIL("Timed out waiting for database freeze stage " << ui32(stage) << " on shard " << shard);
}

inline void FreezeDatabaseShard(TTestActorRuntime& runtime, ui64 shard,
        ui64 schemeShard, const TPathId& domain, ui64 epoch)
{
    using TStage = NKikimrTxDataShard::TDatabaseFreezeState;
    for (const auto stage : {TStage::FENCE_WRITES, TStage::DRAIN_INDEXES, TStage::DRAIN_CDC}) {
        WaitDatabaseFreezeStage(runtime, shard, schemeShard, domain, epoch, stage);
    }
}

} // namespace NKikimr::NDataShard::NTest
