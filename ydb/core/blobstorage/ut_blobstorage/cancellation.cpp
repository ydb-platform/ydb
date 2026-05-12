#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include <util/stream/null.h>

#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(Cancellation) {
    using TTestCtx = TTestCtxBase;

    ui64 GetCancellationCounter(TTestCtx& ctx, const TString& name) {
        ui64 value = 0;
        const TString groupName = Sprintf("%09" PRIu32, ctx.GroupId);
        for (ui32 nodeId = 1; nodeId <= ctx.NodeCount; ++nodeId) {
            auto* appData = ctx.Env->Runtime->GetNode(nodeId)->AppData.get();
            value += GetServiceCounters(appData->Counters, "dsproxy")
                ->GetSubgroup("blobstorageproxy", groupName)
                ->GetSubgroup("subsystem", "cancellation")
                ->GetCounter(name)->Val();
        }
        return value;
    }

    void SendCancellablePut(TTestCtx& ctx, TMessageRelevanceOwner owner, TInstant deadline = TInstant::Max()) {
        TString data = MakeData(10);
        TLogoBlobID blobId(1, 1, 1, 1, data.size(), 1);
        auto* ev = new TEvBlobStorage::TEvPut(
            TEvBlobStorage::TEvPut::TParameters{
                .BlobId = blobId,
                .Buffer = TRope(data),
                .Deadline = deadline,
                .ExternalRelevanceWatcher = owner,
            }
        );
        ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
            SendToBSProxy(ctx.Edge, ctx.GroupId, ev);
        });
    }

    Y_UNIT_TEST(CancelPut) {
        TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
            .MaxPutTimeoutDSProxy = TDuration::Minutes(10),
        });

        ctx.Initialize();
        ctx.AllocateEdgeActorOnSpecificNode(ctx.NodeCount);

        TMessageRelevanceOwner owner = std::make_shared<TMessageRelevanceTracker>();

        ctx.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPut::EventType &&
                    ev->Sender.NodeId() != ev->Recipient.NodeId()) {
                UNIT_FAIL("Unexpected TEvVPut event# " << ev->Sender << "->" <<
                        ev->Recipient << " " << ev->ToString());
            }
            return true;
        };

        const ui64 cancelledEvents = GetCancellationCounter(ctx, "CancelledEvents");
        const ui64 timeoutedCancelledEvents = GetCancellationCounter(ctx, "TimeoutedCancelledEvents");

        SendCancellablePut(ctx, owner);
        owner.reset();

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false,
                TAppData::TimeProvider->Now() + TDuration::Seconds(65));
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "CancelledEvents"), cancelledEvents + 1);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "TimeoutedCancelledEvents"), timeoutedCancelledEvents);
    }

    Y_UNIT_TEST(CancelPutByDeadline) {
        TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
            .MaxPutTimeoutDSProxy = TDuration::Minutes(10),
        });

        ctx.Initialize();
        ctx.AllocateEdgeActorOnSpecificNode(ctx.NodeCount);

        TMessageRelevanceOwner owner = std::make_shared<TMessageRelevanceTracker>();
        bool deadlineInjected = false;

        ctx.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!deadlineInjected && ev->GetTypeRewrite() == TEvBlobStorage::TEvVPut::EventType) {
                deadlineInjected = true;
                owner.reset();
                ctx.Env->Runtime->Send(new IEventHandle(TEvBlobStorage::EvDeadline, 0, ev->Sender,
                    MakeBlobStorageProxyID(ctx.GroupId), nullptr, 0), ev->Sender.NodeId());
                return false;
            }
            return true;
        };

        const ui64 cancelledEvents = GetCancellationCounter(ctx, "CancelledEvents");
        const ui64 timeoutedCancelledEvents = GetCancellationCounter(ctx, "TimeoutedCancelledEvents");

        SendCancellablePut(ctx, owner, TAppData::TimeProvider->Now() + TDuration::Minutes(10));

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false,
                TAppData::TimeProvider->Now() + TDuration::Seconds(5));
        UNIT_ASSERT(deadlineInjected);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::DEADLINE);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "CancelledEvents"), cancelledEvents);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "TimeoutedCancelledEvents"), timeoutedCancelledEvents + 1);
    }
}
