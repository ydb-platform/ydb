#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include <util/stream/null.h>

#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(Cancellation) {
    using TTestCtx = TTestCtxBase;

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

        {
            TString data = MakeData(10);
            TLogoBlobID blobId(1, 1, 1, 1, data.size(), 1);
            TEvBlobStorage::TEvPut* ev = new TEvBlobStorage::TEvPut(
                TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = blobId,
                    .Buffer = TRope(data),
                    .Deadline = TInstant::Max(),
                    .ExternalRelevanceWatcher = owner,
                }
            );
            owner.reset();
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, ev);
            });
        }

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false,
                TAppData::TimeProvider->Now() + TDuration::Seconds(65));
        UNIT_ASSERT(res);
    }
}
