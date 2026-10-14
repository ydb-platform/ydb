#include "guid_firstrun.h"
#include "blobstorage_syncer_committer.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TSyncerGuidFirstRunTests) {
    Y_UNIT_TEST(GenerationChangeWaitsForLocalCommit) {
        for (bool singleDc : {false, true}) {
            TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());

            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1u, 3u, 3u);
            info->EnableSingleDcMode = singleDc;
            if (singleDc) {
                info->SurvivingDc = 1;
            }
            const TVDiskID self(info->GroupID, info->GroupGeneration, 1, 0, 0);
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
                self, nullptr, NPDisk::DEVICE_TYPE_SSD);
            const auto committer = runtime.AllocateEdgeActor();
            const auto notify = runtime.AllocateEdgeActor();
            const TVDiskEternalGuid guid(123);
            const auto actor = runtime.Register(CreateVDiskGuidFirstRunActor(vctx, info, committer, notify,
                NSyncer::EFirstRunStep::ACTION_WriteFinalLocally, guid));

            auto pending = runtime.GrabEdgeEventRethrow<TEvSyncerCommit>(committer);
            UNIT_ASSERT_VALUES_EQUAL(pending->Get()->LocalGuidInfo.GetState(), NKikimrBlobStorage::TLocalGuidInfo::Final);
            UNIT_ASSERT_VALUES_EQUAL(pending->Get()->LocalGuidInfo.GetGuid(), ui64(guid));

            // A generation update must neither complete nor replace a local
            // write whose durable acknowledgement is still outstanding.
            const TVDiskID nextSelf(info->GroupID, info->GroupGeneration + 1, self);
            auto nextInfo = MakeIntrusive<TBlobStorageGroupInfo>(info, nextSelf, TActorId());
            runtime.Send(actor, notify, new TEvVGenerationChange(nextSelf, nextInfo));
            // SimulateSleep supplies a scheduled sentinel, so a correctly empty
            // event queue does not make the runtime wait for its dispatch timeout.
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            const auto assertNoEvent = [&](const TActorId& edge, ui32 eventType) {
                auto events = runtime.CaptureMailboxEvents(edge.Hint(), edge.NodeId());
                for (const auto& event : events) {
                    UNIT_ASSERT_C(event->GetTypeRewrite() != eventType,
                        "Generation change advanced GUID recovery before the durable commit acknowledgement");
                }
                runtime.PushMailboxEventsFront(edge.Hint(), edge.NodeId(), events);
            };
            assertNoEvent(notify, TEvSyncerGuidFirstRunDone::EventType);
            assertNoEvent(committer, TEvSyncerCommit::EventType);

            runtime.Send(actor, committer, new TEvSyncerCommitDone(pending->Get()->Cookie));
            auto done = runtime.GrabEdgeEventRethrow<TEvSyncerGuidFirstRunDone>(notify);
            UNIT_ASSERT_VALUES_EQUAL(done->Get()->Guid, guid);
        }
    }
}

} // NKikimr
