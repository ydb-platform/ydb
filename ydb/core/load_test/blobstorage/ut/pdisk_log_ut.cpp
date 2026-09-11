#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/load_test/service_actor.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

using namespace NKikimr;
using namespace NActors;

Y_UNIT_TEST_SUITE(PDiskLogLoadTest) {
    Y_UNIT_TEST(DelayedFirstYardInitResult) {
        TTestActorSystem runtime(1);
        const auto timeProvider = TAppData::TimeProvider;
        TAppData::TimeProvider = TTestActorSystem::CreateTimeProvider();
        Y_DEFER {
            runtime.Stop();
            TAppData::TimeProvider = timeProvider;
        };
        runtime.Start();
        constexpr ui32 nodeId = 1;
        constexpr ui32 pdiskId = 99;
        constexpr ui64 pdiskGuid = 12345;
        TIntrusivePtr<TPDiskMockState> state = new TPDiskMockState(nodeId, pdiskId, pdiskGuid, 1ULL << 30);
        const auto pdisk = runtime.Register(CreatePDiskMockActor(state), nodeId);
        runtime.RegisterService(MakeBlobStoragePDiskID(nodeId, pdiskId), pdisk);

        const TString configText(R"(
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
        TStringInput config(configText);
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

        const auto deadline = runtime.GetClock() + TDuration::Seconds(1);
        runtime.Sim([&] { return readsChecked < 2 && runtime.GetClock() < deadline; });
        UNIT_ASSERT(delayed);
        UNIT_ASSERT_VALUES_EQUAL(ownerRounds.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(readsChecked, 2);
        runtime.FilterFunction = {};
    }
}
