#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/generic/hash_set.h>
#include <util/stream/null.h>

#include "ut_helpers.h"

#define Ctest Cnull

Y_UNIT_TEST_SUITE(Acceleration) {

    struct TEvDelayedMessageWrapper : public TEventLocal<TEvDelayedMessageWrapper, TEvBlobStorage::EvDelayedMessageWrapper> {
    public:
        std::unique_ptr<IEventHandle> Event;

        TEvDelayedMessageWrapper(std::unique_ptr<IEventHandle>& ev)
            : Event(ev.release())
        {}
    };

    struct TestCtx {
        std::unique_ptr<TEnvironmentSetup> Env;
        ui32 NodeCount;
        ui32 GroupId;
        TActorId Edge;
    };

    TestCtx SetupEnv(const TBlobStorageGroupType& erasure, float slowDiskThreshold, TDuration diskDelay) {
        TestCtx ctx{};

        ui32 nodeCount = erasure.BlobSubgroupSize() + 1;
        ctx.NodeCount = nodeCount;

        std::function<TNodeLocation(ui32)> locationGenerator;
        if (erasure.BlobSubgroupSize() == 9) {
            locationGenerator = [=](ui32 nodeId) -> TNodeLocation { 
                if (nodeId == nodeCount) {
                    return TNodeLocation{"4", "1", "1", "1"};
                }
                return TNodeLocation{
                    std::to_string((nodeId - 1) / 3),
                    "1",
                    std::to_string((nodeId - 1) % 3),
                    "0"
                };
            };
        } else {
            locationGenerator = [=](ui32 nodeId) -> TNodeLocation { 
                if (nodeId == nodeCount) {
                    return TNodeLocation{"2", "1", "1", "1"};
                }
                return TNodeLocation{
                    "1",
                    "1",
                    std::to_string(nodeId),
                    "0"
                };
            };
        }

        ctx.Env.reset(new TEnvironmentSetup{{
            .NodeCount = nodeCount,
            .Erasure = erasure,
            .LocationGenerator = locationGenerator,
            .SlowDiskThreshold = slowDiskThreshold,
        }});


        ctx.Env->CreateBoxAndPool(1, 1);
        ctx.Env->Sim(TDuration::Minutes(1));

        NKikimrBlobStorage::TBaseConfig base = ctx.Env->FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
        ctx.GroupId = base.GetGroup(0).GetGroupId();

        ctx.Edge = ctx.Env->Runtime->AllocateEdgeActor(ctx.NodeCount);

        ctx.Env->Runtime->FilterFunction = [&, nodeCount](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDelayedMessageWrapper::EventType) {
                std::unique_ptr<IEventHandle> delayedMsg(std::move(ev));
                ev.reset(delayedMsg->Get<TEvDelayedMessageWrapper>()->Event.release());
                return true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVStatusResult::EventType && ev->Sender.NodeId() < nodeCount) {
                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                    TActivationContext::Schedule(diskDelay, new IEventHandle(
                            ev->Sender,
                            ev->Recipient,
                            new TEvDelayedMessageWrapper(ev))
                    );
                });
                return false;
            }
            return true;
        };

        for (ui32 i = 0; i < 1000; ++i) {
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
            });
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
        return std::move(ctx);
    }

    void TestAcceleratePut(const TBlobStorageGroupType& erasure, ui32 slowDisksNum,
            NKikimrBlobStorage::EPutHandleClass handleClass, float slowDiskThreshold = 2,
            TDuration waitTime = TDuration::Seconds(3),
            TDuration fastDiskDelay = TDuration::Seconds(1),
            TDuration slowDiskDelay = TDuration::Seconds(4)) {
        for (ui32 fastDisksNum = 0; fastDisksNum < erasure.BlobSubgroupSize() - 2; ++fastDisksNum) {
            TestCtx ctx = SetupEnv(erasure, slowDiskThreshold, fastDiskDelay);

            Ctest << "fastDisksNum# " << fastDisksNum << Endl;

            TString data = "Test";
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), 1);

            THashSet<TVDiskID> fastDisks;
            THashSet<TVDiskID> slowDisks;

            ctx.Env->Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvDelayedMessageWrapper::EventType) {
                    std::unique_ptr<IEventHandle> delayedMsg(std::move(ev));
                    Ctest << TAppData::TimeProvider->Now() << " Unwrap message: " <<
                            delayedMsg->Get<TEvDelayedMessageWrapper>()->Event->Get<TEvBlobStorage::TEvVPutResult>()->ToString() << Endl;
                    ev.reset(delayedMsg->Get<TEvDelayedMessageWrapper>()->Event.release());
                    return true;
                }
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType && ev->Sender.NodeId() < ctx.NodeCount) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetBlobID());
                    Ctest << TAppData::TimeProvider->Now() << " TEvVPutResult: vdiskId# " << vdiskId.ToString() <<
                            " partId# " << partId.ToString() << ", ";
                    TDuration delay;
                    if (fastDisks.size() < fastDisksNum || fastDisks.count(vdiskId)) {
                        fastDisks.insert(vdiskId);
                        delay = fastDiskDelay;
                    } else if (!slowDisks.count(vdiskId) && slowDisks.size() < slowDisksNum) {
                        slowDisks.insert(vdiskId);
                        delay = slowDiskDelay;
                    } else {
                        fastDisks.insert(vdiskId);
                        delay = fastDiskDelay;
                    }
                    Ctest << "delay message for " << delay.ToString() << Endl;
                    ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                        TActivationContext::Schedule(delay, new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                new TEvDelayedMessageWrapper(ev))
                        );
                    });

                    return false;
                }
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPut::EventType && ev->Recipient.NodeId() == ctx.NodeCount) {
                    Ctest << "Send TEvVPut: " << ev->Sender.ToString() << " " << ev->Recipient.ToString() << ev->Get<TEvBlobStorage::TEvVPut>()->ToString() << Endl;
                }
                return true;
            };

            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()), handleClass);
            });

            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                    ctx.Edge, false, TAppData::TimeProvider->Now() + waitTime);
            UNIT_ASSERT_C(res, "fastDisksNum# " << fastDisksNum);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
    }

    void TestAccelerateGet(const TBlobStorageGroupType& erasure, ui32 slowDisksNum,
            NKikimrBlobStorage::EGetHandleClass handleClass, float slowDiskThreshold = 2,
            TDuration waitTime = TDuration::Seconds(3),
            TDuration fastDiskDelay = TDuration::Seconds(1),
            TDuration slowDiskDelay = TDuration::Seconds(4)) {
        for (ui32 fastDisksNum = 0; fastDisksNum < erasure.BlobSubgroupSize() - 2; ++fastDisksNum) {
            TestCtx ctx = SetupEnv(erasure, slowDiskThreshold, fastDiskDelay);

            Ctest << "fastDisksNum# " << fastDisksNum << Endl;
            TString data = MakeData(1024);
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), 1);

            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
            });
            
            ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());

            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), TInstant::Max(), handleClass));
            });

            THashSet<TVDiskID> slowDisks;
            THashSet<TVDiskID> fastDisks;

            ctx.Env->Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvDelayedMessageWrapper::EventType) {
                    std::unique_ptr<IEventHandle> delayedMsg(std::move(ev));
                    Ctest << TAppData::TimeProvider->Now() << " Unwrap message " <<
                            delayedMsg->Get<TEvDelayedMessageWrapper>()->Event->Get<TEvBlobStorage::TEvVGetResult>()->ToString() << Endl;
                    ev.reset(delayedMsg->Get<TEvDelayedMessageWrapper>()->Event.release());
                    return true;
                }
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType && ev->Sender.NodeId() < ctx.NodeCount) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(
                            ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetResult(0).GetBlobID());
                    Ctest << TAppData::TimeProvider->Now() << " TEvVGetResult: vdiskId# " << vdiskId.ToString() <<
                            " partId# " << partId.ToString() << ", ";
                            
                    TDuration delay;
                    if (fastDisks.size() < fastDisksNum || fastDisks.count(vdiskId)) {
                        fastDisks.insert(vdiskId);
                        delay = fastDiskDelay;
                    } else if (!slowDisks.count(vdiskId) && slowDisks.size() < slowDisksNum) {
                        slowDisks.insert(vdiskId);
                        delay = slowDiskDelay;
                    } else {
                        delay = fastDiskDelay;
                    }
                    Ctest << "delay message for " << delay.ToString() << Endl;
                    ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                        TActivationContext::Schedule(delay, new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                new TEvDelayedMessageWrapper(ev))
                        );
                    });

                    return false;
                }
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGet::EventType && ev->Recipient.NodeId() == ctx.NodeCount) {
                    Ctest << "Send TEvVGet: " << ev->Sender.ToString() << " " << ev->Recipient.ToString() << ev->Get<TEvBlobStorage::TEvVGet>()->ToString() << Endl;
                }
                return true;
            };

            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(ctx.Edge, false, TAppData::TimeProvider->Now() + waitTime);
            UNIT_ASSERT_C(res, "fastDisksNum# " << fastDisksNum);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
            Ctest << "TEvGetResult# " << res->Get()->ToString() << Endl;
        }
    }

    #define TEST_ACCELERATE(erasure, method, handleClass, slowDisks)                                                    \
    Y_UNIT_TEST(Test##erasure##method##handleClass##slowDisks##Slow) {                                                  \
        TestAccelerate##method(TBlobStorageGroupType::Erasure##erasure, slowDisks, NKikimrBlobStorage::handleClass,     \
            10, TDuration::Seconds(6), TDuration::Seconds(2), TDuration::Seconds(4));                                   \
    }

    TEST_ACCELERATE(Mirror3dc, Put, AsyncBlob, 1);
//    TEST_ACCELERATE(Mirror3of4, Put, AsyncBlob, 1);
    TEST_ACCELERATE(4Plus2Block, Put, AsyncBlob, 1);

//    TEST_ACCELERATE(Mirror3dc, Put, AsyncBlob, 2);
//    TEST_ACCELERATE(Mirror3of4, Put, AsyncBlob, 2);
    TEST_ACCELERATE(4Plus2Block, Put, AsyncBlob, 2);

    TEST_ACCELERATE(Mirror3dc, Get, AsyncRead, 1);
//    TEST_ACCELERATE(Mirror3of4, Get, AsyncRead, 1);
    TEST_ACCELERATE(4Plus2Block, Get, AsyncRead, 1);

//    TEST_ACCELERATE(Mirror3dc, Get, AsyncRead, 2);
//    TEST_ACCELERATE(Mirror3of4, Get, AsyncRead, 2);
    TEST_ACCELERATE(4Plus2Block, Get, AsyncRead, 2);

    #define TEST_ACCELERATE_THRESHOLD(erasure, method, handleClass)                                             \
    Y_UNIT_TEST(Threshold##erasure##method##handleClass) {                                                      \
        TestAccelerate##method(TBlobStorageGroupType::Erasure##erasure, 1, NKikimrBlobStorage::handleClass);    \
    }

    TEST_ACCELERATE_THRESHOLD(Mirror3dc, Put, AsyncBlob);
    TEST_ACCELERATE_THRESHOLD(4Plus2Block, Put, AsyncBlob);

    TEST_ACCELERATE_THRESHOLD(Mirror3dc, Get, AsyncRead);
    TEST_ACCELERATE_THRESHOLD(4Plus2Block, Get, AsyncRead);

    #undef TEST_ACCELERATE
}
