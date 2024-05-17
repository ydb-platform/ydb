#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/generic/hash_set.h>
#include <util/stream/null.h>

#define Ctest Cerr

Y_UNIT_TEST_SUITE(Acceleration) {

    void SetupEnv(const TBlobStorageGroupType& erasure, std::unique_ptr<TEnvironmentSetup>& env,
            ui32& nodeCount, ui32& groupId) {
        nodeCount = erasure.BlobSubgroupSize();

        env.reset(new TEnvironmentSetup{{
            .NodeCount = nodeCount,
            .Erasure = erasure,
        }});


        env->CreateBoxAndPool(1, 1);
        env->Sim(TDuration::Minutes(1));

        NKikimrBlobStorage::TBaseConfig base = env->FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
        groupId = base.GetGroup(0).GetGroupId();

        TActorId edge = env->Runtime->AllocateEdgeActor(1);

        env->Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
        });
        auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, true, TInstant::Max());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    void TestAcceleratePut(const TBlobStorageGroupType& erasure, ui32 slowDisks,
            NKikimrBlobStorage::EPutHandleClass handleClass) {
        std::unique_ptr<TEnvironmentSetup> env;
        ui32 nodeCount;
        ui32 groupId;

        constexpr TDuration delay = TDuration::Seconds(1);

        SetupEnv(erasure, env, nodeCount, groupId);

        TActorId edge = env->Runtime->AllocateEdgeActor(1);
        TString data = "Test";
        TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), 1);

        env->Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()), handleClass);
        });

        THashSet<TVDiskID> delayedDisks;

        env->Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType) {
                TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetVDiskID());
                TLogoBlobID partId = LogoBlobIDFromLogoBlobID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetBlobID());
                Ctest << TAppData::TimeProvider->Now() << " TEvVPutResult: vdiskId# " << vdiskId.ToString() <<
                        " partId# " << partId.ToString() << ", ";
                if (delayedDisks.size() >= slowDisks || delayedDisks.count(vdiskId)) {
                    Ctest << "pass message" << Endl;
                    return true;
                } else {
                    Ctest << "delay message for " << delay.ToString() << Endl;
                    delayedDisks.insert(vdiskId);
                    env->Runtime->WrapInActorContext(edge, [&] {
                        TActivationContext::Schedule(delay, ev.release());
                    });

                    return false;
                }
            }
            return true;
        };

        auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, TAppData::TimeProvider->Now() + delay / 2);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    void TestAccelerateGet(const TBlobStorageGroupType& erasure, ui32 slowDisks,
            NKikimrBlobStorage::EGetHandleClass handleClass) {
        std::unique_ptr<TEnvironmentSetup> env;
        ui32 nodeCount;
        ui32 groupId;

        constexpr TDuration delay = TDuration::Seconds(1);

        SetupEnv(erasure, env, nodeCount, groupId);

        TActorId edge = env->Runtime->AllocateEdgeActor(1);
        TString data = "Test";
        TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), 1);

        env->Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
        });
        
        env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, TInstant::Max());

        env->Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), TInstant::Max(), handleClass));
        });

        THashSet<TVDiskID> delayedDisks;

        env->Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType) {
                TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetVDiskID());
                TLogoBlobID partId = LogoBlobIDFromLogoBlobID(
                        ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetResult(0).GetBlobID());
                Ctest << TAppData::TimeProvider->Now() << " TEvVGetResult: vdiskId# " << vdiskId.ToString() <<
                        " partId# " << partId.ToString() << ", ";
                if (delayedDisks.size() >= slowDisks || delayedDisks.count(vdiskId)) {
                    Ctest << "pass message" << Endl;
                    return true;
                } else {
                    Ctest << "delay message for " << delay.ToString() << Endl;
                    delayedDisks.insert(vdiskId);
                    env->Runtime->WrapInActorContext(edge, [&] {
                        TActivationContext::Schedule(delay, ev.release());
                    });

                    return false;
                }
            }
            return true;
        };

        auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false, TAppData::TimeProvider->Now() + delay / 2);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    #define TEST_ACCELERATE(erasure, method, handleClass, slowDisks)                                                    \
    Y_UNIT_TEST(Test##erasure##method##handleClass##slowDisks##Slow) {                                                  \
        TestAccelerate##method(TBlobStorageGroupType::Erasure##erasure, slowDisks, NKikimrBlobStorage::handleClass);    \
    }

    TEST_ACCELERATE(Mirror3dc, Put, AsyncBlob, 1);
    TEST_ACCELERATE(Mirror3of4, Put, AsyncBlob, 1);
    TEST_ACCELERATE(4Plus2Block, Put, AsyncBlob, 1);

    TEST_ACCELERATE(Mirror3dc, Put, AsyncBlob, 2);
    TEST_ACCELERATE(Mirror3of4, Put, AsyncBlob, 2);
    TEST_ACCELERATE(4Plus2Block, Put, AsyncBlob, 2);

    TEST_ACCELERATE(Mirror3dc, Get, AsyncRead, 1);
    TEST_ACCELERATE(Mirror3of4, Get, AsyncRead, 1);
    TEST_ACCELERATE(4Plus2Block, Get, AsyncRead, 1);

    TEST_ACCELERATE(Mirror3dc, Get, AsyncRead, 2);
    TEST_ACCELERATE(Mirror3of4, Get, AsyncRead, 2);
    TEST_ACCELERATE(4Plus2Block, Get, AsyncRead, 2);

    #undef TEST_ACCELERATE
}
