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
                ->GetCounter(name, true)->Val();
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

        SendCancellablePut(ctx, owner);
        owner.reset();

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false,
                TAppData::TimeProvider->Now() + TDuration::Seconds(65));
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "CancelledEvents"), cancelledEvents + 1);
    }
    Y_UNIT_TEST(ExternalCancellationTerminatesInTime) {
        TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
            .MaxPutTimeoutDSProxy = TDuration::Minutes(10),
        });

        ctx.Initialize();
        ctx.AllocateEdgeActorOnSpecificNode(ctx.NodeCount);

        TMessageRelevanceOwner owner = std::make_shared<TMessageRelevanceTracker>();
        bool cancelledInBackpressure = false;

        ctx.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPut::EventType) {
                if (!cancelledInBackpressure) {
                    cancelledInBackpressure = true;
                    owner.reset();
                }
                if (ev->Sender.NodeId() != ev->Recipient.NodeId()) {
                    UNIT_FAIL("Externally cancelled TEvVPut was sent to VDisk, event# " << ev->Sender << "->" <<
                            ev->Recipient << " " << ev->ToString());
                }
            }
            return true;
        };

        const ui64 cancelledEvents = GetCancellationCounter(ctx, "CancelledEvents");

        SendCancellablePut(ctx, owner);

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false,
                TAppData::TimeProvider->Now() + TDuration::Seconds(5));
        UNIT_ASSERT(cancelledInBackpressure);
        UNIT_ASSERT_C(res, "Externally cancelled request did not terminate in time");
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "CancelledEvents"), cancelledEvents + 1);
    }

    Y_UNIT_TEST(ExternalCancellationAfterAllSubrequestsSentTerminatesInTime) {
        TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
            .MaxPutTimeoutDSProxy = TDuration::Minutes(10),
        });

        ctx.Initialize();
        ctx.AllocateEdgeActorOnSpecificNode(ctx.NodeCount);

        TMessageRelevanceOwner owner = std::make_shared<TMessageRelevanceTracker>();
        const ui32 expectedSubrequests = erasure.TotalPartCount();
        ui32 subrequestsSent = 0;
        bool allSubrequestsSent = false;
        bool releaseResults = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> detainedResults;

        ctx.Env->Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) -> bool {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::TEvVPut::EventType:
                    if (ev->Sender.NodeId() != ev->Recipient.NodeId() && ++subrequestsSent == expectedSubrequests) {
                        allSubrequestsSent = true;
                        owner.reset();
                    }
                    return true;

                case TEvBlobStorage::TEvVPutResult::EventType:
                    if (!releaseResults) {
                        detainedResults.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                    return true;

                default:
                    return true;
            }
        };

        const ui64 cancelledEvents = GetCancellationCounter(ctx, "CancelledEvents");

        SendCancellablePut(ctx, owner);

        while (!allSubrequestsSent || detainedResults.empty()) {
            ctx.Env->Sim(TDuration::MilliSeconds(100));
        }

        releaseResults = true;
        for (auto& [nodeId, ev] : detainedResults) {
            ctx.Env->Runtime->Send(ev.release(), nodeId);
        }
        detainedResults.clear();

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false,
                TAppData::TimeProvider->Now() + TDuration::Seconds(5));
        UNIT_ASSERT_C(res, "Request cancelled after all subrequests were sent did not terminate in time");
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetCancellationCounter(ctx, "CancelledEvents"), cancelledEvents + 1);
    }
}
