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
            runtime.Send(new IEventHandle(board, oldPublisher, new TEvWhiteboard::TEvDDiskStateDelete(1, 1)));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 2);
            runtime.Send(new IEventHandle(board, publisher, new TEvWhiteboard::TEvDDiskStateDelete(1, 1)));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 1);
            runtime.AdvanceCurrentTime(TDuration::Seconds(16));
            UNIT_ASSERT_VALUES_EQUAL(query(true).DDiskStateInfoSize(), 0);
        }

        Y_UNIT_TEST(SortBothRolesBeforePaging) {
            for (const auto sort : {NKikimrCms::DDISK_DISK_SORT_BY_DDISK_OCCUPANCY,
                                    NKikimrCms::DDISK_DISK_SORT_BY_PERSISTENT_BUFFER_OCCUPANCY}) {
                for (bool descending : {false, true}) {
                    NKikimrCms::TDDiskDiskListResponse response;
                    response.SetTotalCount(5);
                    for (ui32 i = 0; i < 5; ++i) {
                        auto* disk = response.AddDisks();
                        disk->MutableDiskId()->SetNodeId(i + 1);
                        if (i != 0) {
                            // Includes zero, equal values, and missing measurements.
                            const double value = i == 4 ? 0 : i == 3 ? 0.8
                                                                     : 0.2;
                            if (sort == NKikimrCms::DDISK_DISK_SORT_BY_DDISK_OCCUPANCY) {
                                disk->SetDDiskOccupancy(value);
                            } else {
                                disk->SetPersistentBufferOccupancy(value);
                            }
                        }
                    }
                    NKikimrCms::TDDiskDiskListRequest request;
                    request.SetSortBy(sort);
                    request.SetSortDescending(descending);
                    request.SetLimit(0);
                    SortAndPageDDiskOccupancy(response, request);
                    const TVector<ui32> expected = descending
                                                       ? TVector<ui32>{4, 2, 3, 5, 1}
                                                       : TVector<ui32>{5, 2, 3, 4, 1};
                    for (ui32 i = 0; i < expected.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(response.GetDisks(i).GetDiskId().GetNodeId(), expected[i]);
                    }
                    request.SetOffset(1);
                    request.SetLimit(2);
                    SortAndPageDDiskOccupancy(response, request);
                    UNIT_ASSERT_VALUES_EQUAL(response.GetTotalCount(), 5);
                    UNIT_ASSERT_VALUES_EQUAL(response.DisksSize(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(response.GetDisks(0).GetDiskId().GetNodeId(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(response.GetDisks(1).GetDiskId().GetNodeId(), 3);
                    request.SetOffset(Max<ui32>());
                    SortAndPageDDiskOccupancy(response, request);
                    UNIT_ASSERT_VALUES_EQUAL(response.DisksSize(), 0);
                }
            }
        }
    } // Y_UNIT_TEST_SUITE(TDDiskUsageTest)

} // namespace NKikimr::NCms
