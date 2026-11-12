#include <ydb/core/cms/ddisk_usage.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/cms/cluster_info.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NCms {

    Y_UNIT_TEST_SUITE(TDDiskUsageTest) {
        Y_UNIT_TEST(WhiteboardBatchesSlotsAndExpiresStaleSamples) {
            TTestBasicRuntime runtime;
            SetupTabletServices(runtime);
            using namespace NNodeWhiteboard;
            const auto board = MakeNodeWhiteboardServiceId(runtime.GetNodeId(0));
            const auto client = runtime.AllocateEdgeActor();
            const auto oldPublisher = runtime.AllocateEdgeActor();
            const auto publisher = runtime.AllocateEdgeActor();
            auto publish = [&](ui32 slot, TActorId source, double occupancy) {
                auto* update = new TEvWhiteboard::TEvDDiskStateUpdate;
                update->OwnerRound = source == oldPublisher ? 1 : 2;
                update->Lifetime = TDuration::Seconds(slot == 2 ? 90 : 15);
                update->Record.SetPDiskId(1);
                update->Record.SetDDiskSlotId(slot);
                update->Record.SetDDiskOccupancy(occupancy);
                update->Record.SetPersistentBufferOccupancy(0);
                runtime.Send(new IEventHandle(board, source, update));
            };
            auto query = [&](bool include) {
                auto* request = new TEvWhiteboard::TEvPDiskStateRequest;
                request->Record.SetIncludeDDiskState(include);
                runtime.Send(new IEventHandle(board, client, request));
                return runtime.GrabEdgeEventRethrow<TEvWhiteboard::TEvPDiskStateResponse>(client)->Get()->Record;
            };
            publish(1, oldPublisher, 0.1);
            publish(1, publisher, 0.8);
            publish(2, publisher, 0.4);
            UNIT_ASSERT_VALUES_EQUAL(query(false).DDiskStateInfoSize(), 0);
            auto batch = query(true);
            UNIT_ASSERT_VALUES_EQUAL(batch.DDiskStateInfoSize(), 2);
            TClusterInfo cluster;
            for (const auto& info : batch.GetDDiskStateInfo()) {
                cluster.UpdateDDiskState(runtime.GetNodeId(0), info);
            }
            const auto* first = cluster.FindDDiskState(runtime.GetNodeId(0), 1, 1);
            UNIT_ASSERT(first);
            UNIT_ASSERT_VALUES_EQUAL(first->GetDDiskOccupancy(), 0.8);
            UNIT_ASSERT(first->HasPersistentBufferOccupancy());
            UNIT_ASSERT_VALUES_EQUAL(first->GetPersistentBufferOccupancy(), 0);
            UNIT_ASSERT(!cluster.FindDDiskState(runtime.GetNodeId(0), 1, 3));
            cluster.ClearNode(runtime.GetNodeId(0));
            UNIT_ASSERT(!cluster.FindDDiskState(runtime.GetNodeId(0), 1, 1));
            runtime.Send(new IEventHandle(board, oldPublisher, new TEvWhiteboard::TEvDDiskStateDelete(1, 1, 1)));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 2);
            publish(1, oldPublisher, 0.1);
            auto current = query(true);
            for (const auto& info : current.GetDDiskStateInfo()) {
                if (info.GetDDiskSlotId() == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(info.GetDDiskOccupancy(), 0.8);
                }
            }
            runtime.Send(new IEventHandle(board, publisher, new TEvWhiteboard::TEvDDiskStateDelete(1, 1, 2)));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 1);
            // Delayed updates cannot resurrect a deleted incarnation.
            publish(1, oldPublisher, 0.1);
            publish(1, publisher, 0.8);
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 1);
            runtime.AdvanceCurrentTime(TDuration::Seconds(16));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 1);
            runtime.AdvanceCurrentTime(TDuration::Seconds(75));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 0);
            publish(2, oldPublisher, 0.1);
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 0);
            // Expiry still permits a current actor to recover after missed checks.
            publish(2, publisher, 0.5);
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 1);
        }

        Y_UNIT_TEST(SortOccupancyBeforePaging) {
            const TVector<TDDiskOccupancySortKey> keys{
                {1, 1, 1, std::nullopt},
                {2, 1, 1, 0.2},
                {2, 1, 2, 0.2},
                {3, 1, 1, 0.8},
                {4, 1, 1, 0.0},
            };
            for (bool descending : {false, true}) {
                const TVector<ui32> expected = descending
                    ? TVector<ui32>{3, 1, 2, 4, 0} : TVector<ui32>{4, 1, 2, 3, 0};
                UNIT_ASSERT_VALUES_EQUAL(SortAndPageDDiskOccupancy(keys, descending, 0, 0), expected);
                UNIT_ASSERT_VALUES_EQUAL(SortAndPageDDiskOccupancy(keys, descending, 1, 2), (TVector<ui32>{1, 2}));
                UNIT_ASSERT_VALUES_EQUAL(SortAndPageDDiskOccupancy(keys, descending, 4, Max<ui32>()), (TVector<ui32>{0}));
                UNIT_ASSERT(SortAndPageDDiskOccupancy(keys, descending, Max<ui32>(), 2).empty());
                UNIT_ASSERT(SortAndPageDDiskOccupancy({}, descending, 0, 0).empty());
            }
        }
    } // Y_UNIT_TEST_SUITE(TDDiskUsageTest)

} // namespace NKikimr::NCms
