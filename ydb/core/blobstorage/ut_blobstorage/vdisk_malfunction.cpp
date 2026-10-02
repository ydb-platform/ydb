#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <util/stream/null.h>

#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(VDiskMalfunction) {
    Y_UNIT_TEST(StuckInternalQueues) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);

        ui32 groupId = env.GetGroupInfo(groups.front())->GroupID.GetRawId();
        TActorId edge = env.Runtime->AllocateEdgeActor(1);

        ui32 initialPuts = 100;

        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Type == TEvVDiskRequestCompleted::EventType) {
                return false;
            }
            return true;
        };

        auto sendPut = [&](ui64 cookie) {
            env.Runtime->WrapInActorContext(edge, [&] {
                TString data = MakeData(100000);
                TLogoBlobID blobId(1, 1, 1, 1, data.size(), cookie);
                TEvBlobStorage::TEvPut* ev = new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max());
                SendToBSProxy(edge, groupId, ev);
            });
        };

        for (ui32 i = 0; i < initialPuts; ++i) {
            sendPut(i);
        }

        for (ui32 i = 0; i < initialPuts; ++i) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                    edge, false, TInstant::Max());
        }

        env.Sim(TDuration::Minutes(10));
        env.Runtime->FilterFunction = {};

        sendPut(initialPuts);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                edge, false, TAppData::TimeProvider->Now() + TDuration::Seconds(10));
        UNIT_ASSERT(res);
    }
}
