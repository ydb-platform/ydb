#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/generic/hash_set.h>
#include <util/stream/null.h>

#include "ut_helpers.h"

#define Ctest Cnull

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

    void TestAcceleratePut(const TBlobStorageGroupType& erasure, ui32 slowDisksNum,
            NKikimrBlobStorage::EPutHandleClass handleClass) {
        for (ui32 fastDisksNum = 0; fastDisksNum < erasure.BlobSubgroupSize() - 2; ++fastDisksNum) {
            std::unique_ptr<TEnvironmentSetup> env;
            ui32 nodeCount;
            ui32 groupId;
            SetupEnv(erasure, env, nodeCount, groupId);

            constexpr TDuration delay = TDuration::Seconds(2);
            constexpr TDuration waitFor = TDuration::Seconds(1);

            Ctest << "fastDisksNum# " << fastDisksNum << Endl;

            TActorId edge = env->Runtime->AllocateEdgeActor(1);
            TString data = "Test";
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), 1);

            env->Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()), handleClass);
            });

            THashSet<TVDiskID> fastDisks;
            THashSet<TVDiskID> slowDisks;

            env->Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetBlobID());
                    Ctest << TAppData::TimeProvider->Now() << " TEvVPutResult: vdiskId# " << vdiskId.ToString() <<
                            " partId# " << partId.ToString() << ", ";
                    if (fastDisks.size() < fastDisksNum || fastDisks.count(vdiskId)) {
                        fastDisks.insert(vdiskId);
                        Ctest << "pass message" << Endl;
                        return true;
                    } else if (!slowDisks.count(vdiskId) && slowDisks.size() >= slowDisksNum) {
                        Ctest << "pass message" << Endl;
                        return true;
                    } else {
                        Ctest << "delay message for " << delay.ToString() << Endl;
                        slowDisks.insert(vdiskId);
                        env->Runtime->WrapInActorContext(edge, [&] {
                            TActivationContext::Schedule(delay, ev.release());
                        });

                        return false;
                    }
                }
                return true;
            };

            auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, TAppData::TimeProvider->Now() + waitFor);
            UNIT_ASSERT_C(res, "fastDisksNum# " << fastDisksNum);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
    }

    void TestAccelerateGet(const TBlobStorageGroupType& erasure, ui32 slowDisksNum,
            NKikimrBlobStorage::EGetHandleClass handleClass) {
        for (ui32 fastDisksNum = 0; fastDisksNum < erasure.BlobSubgroupSize() - 2; ++fastDisksNum) {
            std::unique_ptr<TEnvironmentSetup> env;
            ui32 nodeCount;
            ui32 groupId;
            SetupEnv(erasure, env, nodeCount, groupId);

            constexpr TDuration delay = TDuration::Seconds(2);
            constexpr TDuration waitFor = TDuration::Seconds(1);

            Ctest << "fastDisksNum# " << fastDisksNum << Endl;

            TActorId edge = env->Runtime->AllocateEdgeActor(1);
            TString data = MakeData(1024);
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), 1);

            env->Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
            });
            
            env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, TInstant::Max());

            env->Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), TInstant::Max(), handleClass));
            });

            THashSet<TVDiskID> slowDisks;
            THashSet<TVDiskID> fastDisks;

            env->Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(
                            ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetResult(0).GetBlobID());
                    Ctest << TAppData::TimeProvider->Now() << " TEvVGetResult: " << vdiskId.ToString() <<
                            " partId# " << partId.ToString() << ", ";
                    if (fastDisks.size() < fastDisksNum || fastDisks.count(vdiskId)) {
                        fastDisks.insert(vdiskId);
                        Ctest << "pass message" << Endl;
                        return true;
                    } else if (!slowDisks.count(vdiskId) && slowDisks.size() >= slowDisksNum) {
                        Ctest << "pass message" << Endl;
                        return true;
                    } else {
                        Ctest << "delay message for " << delay.ToString() << Endl;
                        slowDisks.insert(vdiskId);
                        env->Runtime->WrapInActorContext(edge, [&] {
                            TActivationContext::Schedule(delay, ev.release());
                        });

                        return false;
                    }
                }
                return true;
            };

            auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false, TAppData::TimeProvider->Now() + waitFor);
            UNIT_ASSERT_C(res, "fastDisksNum# " << fastDisksNum);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
            Ctest << "TEvGetResult# " << res->Get()->ToString() << Endl;
        }
    }

    #define TEST_ACCELERATE(erasure, method, handleClass, slowDisks)                                                    \
    Y_UNIT_TEST(Test##erasure##method##handleClass##slowDisks##Slow) {                                                  \
        TestAccelerate##method(TBlobStorageGroupType::Erasure##erasure, slowDisks, NKikimrBlobStorage::handleClass);    \
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

    #undef TEST_ACCELERATE
}
