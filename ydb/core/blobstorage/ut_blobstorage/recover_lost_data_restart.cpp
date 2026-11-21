#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_proxyobtain.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_recovery.h>

namespace {

enum class ERestartPoint {
    FullSync,
    OsirisWriteReply,
    OsirisDone,
    RecoveryCommitted,
    GuidPublicationBeforeLost,
    GuidPublicationWithLost,
    GuidPublicationWithFinal,
};

void TestRestart(ERestartPoint point) {
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const ui32 groupId = env.GetGroups().front();
    const auto info = env.GetGroupInfo(groupId);
    const TString data(1024, 'R');
    const TLogoBlobID blob(1, 1, 1, 0, data.size(), 0);
    TBlobStorageGroupInfo::TOrderNums subgroup;
    info->GetTopology().PickSubgroup(blob.Hash(), subgroup);
    const ui32 recoveringOrder = subgroup.front();
    const TVDiskID recoveringDisk = info->GetVDiskId(recoveringOrder);
    const TActorId recoveringActor = info->GetActorId(recoveringOrder);
    const ui32 recoveringNode = recoveringActor.NodeId();

    // Lose the part before this disk can publish its ingress to any peer.
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        return nodeId != recoveringNode || (ev->GetTypeRewrite() != TEvBlobStorage::EvVSync &&
            ev->GetTypeRewrite() != TEvBlobStorage::EvVSyncFull);
    };
    env.PutBlob(groupId, blob, data);
    env.Sim(TDuration::Seconds(10));
    env.Cleanup();
    for (auto& [key, state] : env.PDiskMockStates) {
        if (key.first == recoveringNode) {
            state.Reset();
        }
    }
    env.Initialize();

    auto waitUntil = [&](auto predicate, const char* phase) {
        for (ui32 i = 0; i != 600 && !predicate(); ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(predicate(), phase);
    };
    const bool prepareLost = point == ERestartPoint::GuidPublicationWithLost;
    const bool prepareFinal = point == ERestartPoint::GuidPublicationWithFinal;
    ui64 dbBirthLsnBeforePublication = 0;
    if (prepareLost || prepareFinal) {
        bool prepared = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId != recoveringNode) {
                return true;
            }
            const ui32 type = ev->GetTypeRewrite();
            // Obtain a quorum consisting entirely of healthy peers. This goes straight to Lost,
            // leaving our own peer-GUID entry Empty for the next startup's publication repair.
            if (type == TEvBlobStorage::EvVDiskGuidObtained &&
                    ev->Get<TEvVDiskGuidObtained>()->VDiskId == recoveringDisk) {
                return false;
            }
            if (prepareLost && type == TEvBlobStorage::EvSyncerFullSyncedWithPeer) {
                prepared = true;
                return false;
            }
            if (prepareFinal && type == TEvBlobStorage::EvSyncerLostDataRecovered) {
                dbBirthLsnBeforePublication = ev->Get<TEvSyncerLostDataRecovered>()->LocalSyncerState.DbBirthLsn;
                prepared = true;
                return false;
            }
            return true;
        };
        waitUntil([&] { return prepared; }, "Recovery must commit the local state before GUID republication");
        if (prepareFinal) {
            UNIT_ASSERT_C(dbBirthLsnBeforePublication, "Completed lost-data recovery must have a nonzero DbBirthLsn");
        }
    }

    bool stopped = false;
    bool restarted = false;
    ui32 osirisCallsAfterRestart = 0;
    ui32 osirisWrites = 0;
    std::optional<ui64> dbBirthLsnAfterRestart;
    std::optional<NSyncer::EDecision> decisionAfterRestart;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (nodeId != recoveringNode) {
            return true;
        }
        const ui32 type = ev->GetTypeRewrite();
        if (restarted) {
            if (type == TEvBlobStorage::EvVDiskGuidRecovered) {
                decisionAfterRestart = ev->Get<TEvVDiskGuidRecovered>()->Outcome.Decision;
            } else if (type == TEvBlobStorage::EvCallOsiris) {
                ++osirisCallsAfterRestart;
            } else if (type == TEvBlobStorage::EvSyncGuidRecoveryDone) {
                dbBirthLsnAfterRestart = ev->Get<TEvSyncGuidRecoveryDone>()->DbBirthLsn;
            }
            return true;
        }
        if (type == TEvBlobStorage::EvAnubisOsirisPut) {
            ++osirisWrites;
        }
        const bool stopAfterPublication = point == ERestartPoint::GuidPublicationBeforeLost || prepareLost || prepareFinal;
        if (stopAfterPublication && type == TEvBlobStorage::EvVDiskGuidObtained) {
            // Delay two peers' survey replies, so the first quorum includes the wiped disk's own Empty reply.
            // No reply contents are modified; all peers remain available for GUID publication and data recovery.
            const ui32 order = info->GetOrderNumber(ev->Get<TEvVDiskGuidObtained>()->VDiskId);
            if (order == (recoveringOrder + 1) % 8 || order == (recoveringOrder + 2) % 8) {
                return false;
            }
        }
        const bool stop =
            (point == ERestartPoint::FullSync && type == TEvBlobStorage::EvSyncerFullSyncedWithPeer) ||
            (point == ERestartPoint::OsirisWriteReply && type == TEvBlobStorage::EvAnubisOsirisPutResult) ||
            (point == ERestartPoint::OsirisDone && type == TEvBlobStorage::EvOsirisDone) ||
            (point == ERestartPoint::RecoveryCommitted && type == TEvBlobStorage::EvSyncerLostDataRecovered) ||
            (stopAfterPublication && type == TEvBlobStorage::EvSyncerGuidFirstRunDone);
        if (stop) {
            stopped = true;
            return false;
        }
        return true;
    };
    if (prepareLost || prepareFinal) {
        env.RestartNode(recoveringNode);
    }
    waitUntil([&] { return stopped; }, "The recovery must reach the requested restart point");
    if (point == ERestartPoint::OsirisWriteReply || point == ERestartPoint::OsirisDone ||
            point == ERestartPoint::RecoveryCommitted) {
        UNIT_ASSERT_C(osirisWrites, "Osiris must actually resurrect the lost part's ingress");
    }

    // This restart preserves all committed PDisk data from the interrupted recovery.
    restarted = true;
    env.RestartNode(recoveringNode);
    waitUntil([&] { return decisionAfterRestart.has_value(); }, "GUID recovery must finish after restart");
    const bool expectGood = point == ERestartPoint::RecoveryCommitted || prepareFinal;
    const auto expectedDecision = expectGood
        ? NSyncer::EDecision::Good : NSyncer::EDecision::LostData;
    Cerr << "decisionAfterRestart# " << NSyncer::EDecisionToStr(*decisionAfterRestart)
        << " expected# " << NSyncer::EDecisionToStr(expectedDecision) << Endl;
    env.WaitForVDiskRepl(recoveringActor, recoveringDisk);
    if (prepareFinal) {
        UNIT_ASSERT(dbBirthLsnAfterRestart);
        UNIT_ASSERT_VALUES_EQUAL(*dbBirthLsnAfterRestart, dbBirthLsnBeforePublication);
    }
    env.Runtime->FilterFunction = {};

    // Check the wiped disk itself, rather than allowing the group proxy to read surviving peers.
    const TActorId queue = env.CreateQueueActor(recoveringDisk, NKikimrBlobStorage::GetFastRead, 0);
    const TActorId edge = env.Runtime->AllocateEdgeActor(recoveringNode);
    auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(recoveringDisk, TInstant::Max(),
        NKikimrBlobStorage::FastRead);
    query->AddExtremeQuery(TLogoBlobID(blob, 1), 0, 0);
    env.Runtime->Send(new IEventHandle(queue, edge, query.release()), recoveringNode);
    auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.ResultSize(), 1);
    Cerr << "osirisCallsAfterRestart# " << osirisCallsAfterRestart << " recoveredPartStatus# "
        << NKikimrProto::EReplyStatus_Name(result->Get()->Record.GetResult(0).GetStatus()) << Endl;
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(*decisionAfterRestart), static_cast<int>(expectedDecision));
    UNIT_ASSERT_VALUES_EQUAL(osirisCallsAfterRestart, expectGood ? 0 : 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetResult(0).GetStatus(), NKikimrProto::OK);
}

} // namespace

Y_UNIT_TEST_SUITE(RecoverLostDataRestart) {
    Y_UNIT_TEST(DuringFullSync) { TestRestart(ERestartPoint::FullSync); }
    Y_UNIT_TEST(DuringOsiris) { TestRestart(ERestartPoint::OsirisWriteReply); }
    Y_UNIT_TEST(AfterOsirisBeforeFinalCommit) { TestRestart(ERestartPoint::OsirisDone); }
    Y_UNIT_TEST(AfterFinalCommitBeforeReplication) { TestRestart(ERestartPoint::RecoveryCommitted); }
    Y_UNIT_TEST(DuringGuidPublicationBeforeLostCommit) { TestRestart(ERestartPoint::GuidPublicationBeforeLost); }
    Y_UNIT_TEST(DuringGuidPublicationWithPersistedLost) { TestRestart(ERestartPoint::GuidPublicationWithLost); }
    Y_UNIT_TEST(GuidPublicationPreservesDbBirthLsn) { TestRestart(ERestartPoint::GuidPublicationWithFinal); }
}
