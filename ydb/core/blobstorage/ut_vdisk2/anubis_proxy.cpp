#include "defs.h"

#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubisproxy.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>

#include <util/generic/scope.h>

using namespace NKikimr;

namespace {

    // The proxy sends the query to the backpressure queue of its target VDisk and must hand the queue's answer
    // back to the requester; only an answer from another generation of the target is a RACE.
    void CheckAnswer(bool otherGeneration, NKikimrProto::EReplyStatus expectedStatus, bool queueReportsState = false) {
        TTestActorSystem runtime(1);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };

        const ui32 nodeId = 1;
        TIntrusivePtr<TBlobStorageGroupInfo> info =
            MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block, 1u, 8u);
        const TVDiskID selfId = info->GetVDiskId(0);
        const TVDiskID targetId = info->GetVDiskId(1);
        TIntrusivePtr<TVDiskContext> vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(),
            MakeIntrusive<::NMonitoring::TDynamicCounters>(), selfId, nullptr, NPDisk::DEVICE_TYPE_SSD);

        const TActorId proxy = runtime.Register(CreateAnubisProxy(vctx, info, TVDiskIdShort(targetId), 0), nodeId);
        if (queueReportsState) {
            // The queue reports its state to the proxy, its parent, as it does when it connects to the target.
            runtime.Send(new IEventHandle(proxy, TActorId(), new TEvProxyQueueState(targetId,
                NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead, true, false, false, nullptr)), nodeId);
        }
        const TActorId edge = runtime.AllocateEdgeActor(nodeId);
        runtime.Send(new IEventHandle(proxy, edge, new TEvAnubisVGet({TLogoBlobID(1, 1, 1, 0, 100, 0)})), nodeId);

        // Answer the proxy the way the target's queue would.
        TVDiskID answerId = targetId;
        if (otherGeneration) {
            ++answerId.GroupGeneration;
        }
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result = std::make_unique<TEvBlobStorage::TEvVGetResult>();
        result->Record.SetStatus(NKikimrProto::OK);
        VDiskIDFromVDiskID(answerId, result->Record.MutableVDiskID());
        runtime.Send(new IEventHandle(proxy, TActorId(), result.release()), nodeId);

        std::unique_ptr<TEventHandle<TEvAnubisVGetResult>> ev = runtime.WaitForEdgeActorEvent<TEvAnubisVGetResult>(edge);
        UNIT_ASSERT(ev->Get()->VDiskIdShort == TVDiskIdShort(targetId));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Ev->Get()->Record.GetStatus(), expectedStatus);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(AnubisProxy) {

    Y_UNIT_TEST(AnswerReachesRequester) {
        CheckAnswer(false, NKikimrProto::OK);
    }

    Y_UNIT_TEST(AnswerFromOtherGenerationIsRace) {
        CheckAnswer(true, NKikimrProto::RACE);
    }

    Y_UNIT_TEST(QueueStateReportIsAccepted) {
        CheckAnswer(false, NKikimrProto::OK, true);
    }

}
