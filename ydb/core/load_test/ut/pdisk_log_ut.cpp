#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/load_test/service_actor.h>

#include <library/cpp/protobuf/util/pb_io.h>

Y_UNIT_TEST_SUITE(PDiskLogLoadTest) {
    Y_UNIT_TEST(DelayedFirstYardInitResult) {
        TEnvironmentSetup env({.NodeCount = 1});
        auto& runtime = *env.Runtime;
        constexpr ui32 nodeId = 1;
        constexpr ui32 pdiskId = 99;
        constexpr ui64 pdiskGuid = 12345;
        TIntrusivePtr<TPDiskMockState> state = new TPDiskMockState(nodeId, pdiskId, pdiskGuid, 1ULL << 30);
        const auto pdisk = runtime.Register(CreatePDiskMockActor(state), nodeId);
        runtime.RegisterService(MakeBlobStoragePDiskID(nodeId, pdiskId), pdisk);

        TStringInput config(R"(
            PDiskId: 99
            PDiskGuid: 12345
            DurationSeconds: 1
            DelayBeforeMeasurementsSeconds: 0
            IsWardenlessTest: true
            Workers: {
                VDiskId: {GroupID: 1 GroupGeneration: 1 Ring: 0 Domain: 0 VDisk: 0}
                MaxInFlight: 1
                SizeIntervalMin: 128
                SizeIntervalMax: 128
                BurstInterval: 2147483647
                BurstSize: 1024
                StorageDuration: 1048576
            }
            Workers: {
                VDiskId: {GroupID: 2 GroupGeneration: 1 Ring: 0 Domain: 0 VDisk: 0}
                MaxInFlight: 1
                SizeIntervalMin: 256
                SizeIntervalMax: 256
                BurstInterval: 2147483647
                BurstSize: 1024
                StorageDuration: 1048576
            }
        )");
        const auto command = ParseFromTextFormat<NKikimr::TEvLoadTestRequest::TPDiskLogLoad>(config);
        const auto edge = runtime.AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters();
        const auto load = runtime.Register(CreatePDiskLogWriterLoadTest(command, edge, counters, 0, 42), nodeId);

        bool delayed = false;
        ui32 readsChecked = 0;
        std::map<NPDisk::TOwner, NPDisk::TOwnerRound> ownerRounds;
        runtime.FilterFunction = [&](ui32 eventNodeId, std::unique_ptr<IEventHandle>& event) {
            if (event->Recipient == load && event->GetTypeRewrite() == NPDisk::TEvYardInitResult::EventType) {
                const auto* result = event->Get<NPDisk::TEvYardInitResult>();
                UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
                ownerRounds[result->PDiskParams->Owner] = result->PDiskParams->OwnerRound;
                if (!delayed) {
                    delayed = true;
                    // PDisk may finish the second owner's initialization before the first one's.
                    runtime.Schedule(TDuration::MilliSeconds(10), event.release(), nullptr, eventNodeId);
                    return false;
                }
            } else if (event->Sender == load && event->GetTypeRewrite() == NPDisk::TEvReadLog::EventType) {
                const auto* request = event->Get<NPDisk::TEvReadLog>();
                UNIT_ASSERT_VALUES_EQUAL(request->OwnerRound, ownerRounds.at(request->Owner));
                ++readsChecked;
            }
            return true;
        };

        const auto finished = env.WaitForEdgeActorEvent<TEvLoad::TEvLoadTestFinished>(
            edge, true, runtime.GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT(finished);
        UNIT_ASSERT_C(finished->Get()->Report, finished->Get()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(finished->Get()->Tag, 42);
        UNIT_ASSERT(delayed);
        UNIT_ASSERT_C(readsChecked >= 4, readsChecked);
        runtime.FilterFunction = {};
    }
}
