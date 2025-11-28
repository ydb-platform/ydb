#include "pqrb/partition_scale_manager_graph_cmp.h"
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NMirror;
using namespace NYdb::NTopic;

Y_UNIT_TEST_SUITE(TPartitionScaleManagerGraphCmpTest) {
    TPartitionGraph MultiplePartitionGraph(ui32 size) {
        Y_ASSERT(size >= 1);
        NKikimrPQ::TPQTabletConfig config;
        // Without parents and childrens
        for (ui32 i = 0; i < size; ++i) {
            auto* p0 = config.AddAllPartitions();
            p0->SetPartitionId(i);
        }
        return MakePartitionGraph(config);
    }

    TPartitionGraph SinglePartitionGraph() {
        return MultiplePartitionGraph(1);
    }

    TPartitionGraph SplittedGraph(ui32 size) {
        Y_ASSERT(size >= 3);
        const ui32 r0 = (size + 2) / 3;
        NKikimrPQ::TPQTabletConfig config;

        for (ui32 i = 0; i < r0; ++i) {
            auto* p0 = config.AddAllPartitions();
            p0->SetPartitionId(i);
            p0->AddChildPartitionIds(1 * r0 + i);
            p0->AddChildPartitionIds(2 * r0 + i);

            auto* p1 = config.AddAllPartitions();
            p1->SetPartitionId(1 * r0 + i);
            p1->AddParentPartitionIds(i);

            auto* p2 = config.AddAllPartitions();
            p2->SetPartitionId(2 * r0 + i);
            p2->AddParentPartitionIds(i);
        }
        return MakePartitionGraph(config);
    }

    TPartitionGraph SingleSplitPartitionGraph(ui32 size, ui32 splitPos) {
        Y_ASSERT(size >= 3);
        Y_ASSERT(splitPos < size - 2);
        NKikimrPQ::TPQTabletConfig config;

        for (ui32 i = 0; i + 2 < size; ++i) {
            auto* p0 = config.AddAllPartitions();
            p0->SetPartitionId(i);
            if (i == splitPos) {
                p0->AddChildPartitionIds(size - 2);
                p0->AddChildPartitionIds(size - 1);
            }
        }

        auto* p1 = config.AddAllPartitions();
        p1->SetPartitionId(size - 2);
        p1->AddParentPartitionIds(splitPos);

        auto* p2 = config.AddAllPartitions();
        p2->SetPartitionId(size - 1);
        p2->AddParentPartitionIds(splitPos);

        return MakePartitionGraph(config);
    }


    std::vector<TPartitionInfo> MultipleRootPartitions(ui32 size) {
        Y_ASSERT(size >= 1);
        std::vector<TPartitionInfo> r;
        for (ui32 i = 0; i < size; ++i) {
            Ydb::Topic::DescribeTopicResult::PartitionInfo p;
            p.set_partition_id(i);
            p.set_active(true);
            r.push_back(p);
        }
        return r;
    }

    std::vector<TPartitionInfo> SingleSplitPartitions(ui32 size, ui32 splitPos = 0) {
        Y_ASSERT(size >= 3);
        Y_ASSERT(splitPos < size - 2);
        std::vector<TPartitionInfo> r;
        for (ui32 i = 0; i + 2 < size; ++i) {
            Ydb::Topic::DescribeTopicResult::PartitionInfo p;
            p.set_partition_id(i != splitPos);
            p.set_active(true);
            if (i == splitPos) {
                p.add_child_partition_ids(size - 2);
                p.add_child_partition_ids(size - 1);
            }
            r.push_back(p);
        }
        Ydb::Topic::DescribeTopicResult::PartitionInfo p0;
        p0.set_partition_id(size - 2);
        p0.set_active(true);
        p0.add_parent_partition_ids(splitPos);
        r.push_back(p0);

        Ydb::Topic::DescribeTopicResult::PartitionInfo p1;
        p1.set_partition_id(size - 1);
        p1.set_active(true);
        p1.add_parent_partition_ids(splitPos);
        r.push_back(p1);

        return r;
    }

    Y_UNIT_TEST(Equal) {
        auto c = ComparePartitionGraphs(MultiplePartitionGraph(5), MultipleRootPartitions(5));
        UNIT_ASSERT(!c.RootPartitionsMismatch);
    }


    Y_UNIT_TEST(ExtraRootPartitionsInTargetTopic) {
        auto c = ComparePartitionGraphs(MultiplePartitionGraph(10), MultipleRootPartitions(5));
        UNIT_ASSERT_C(!c.RootPartitionsMismatch, c.RootPartitionsMismatch->Error);
    }

    Y_UNIT_TEST(ExtraRootPartitionsInSourceTopic) {
        auto c = ComparePartitionGraphs(MultiplePartitionGraph(5), MultipleRootPartitions(10));
        UNIT_ASSERT(c.RootPartitionsMismatch);
        UNIT_ASSERT(!c.RootPartitionsMismatch->Error);
        UNIT_ASSERT_VALUES_EQUAL(c.RootPartitionsMismatch->AlterRootPartitions.size(), 10);

        for (ui32 i = 0; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(c.RootPartitionsMismatch->AlterRootPartitions[i].Id, i, LabeledOutput(i));
            UNIT_ASSERT_EQUAL_C(c.RootPartitionsMismatch->AlterRootPartitions[i].Action, (i < 5) ? EPartitionAction::Modify : EPartitionAction::Create, LabeledOutput(i));
        }
    }

    Y_UNIT_TEST(EqualSplitted) {
        auto c0 = ComparePartitionGraphs(SingleSplitPartitionGraph(15, 6), SingleSplitPartitions(15, 6));
        UNIT_ASSERT(!c0.RootPartitionsMismatch);

        auto c1 = ComparePartitionGraphs(MultiplePartitionGraph(12), SingleSplitPartitions(14, 6));
        UNIT_ASSERT(!c1.RootPartitionsMismatch);
    }

    Y_UNIT_TEST(SplittedTargetTopic) {
        auto c6 = ComparePartitionGraphs(SplittedGraph(6), MultipleRootPartitions(8));
        UNIT_ASSERT(c6.RootPartitionsMismatch);
        UNIT_ASSERT_VALUES_EQUAL(c6.RootPartitionsMismatch->Error, "Need to create new root partition #6, but previous partition #0 already has children");

        auto c9 = ComparePartitionGraphs(SplittedGraph(9), MultipleRootPartitions(8));
        UNIT_ASSERT(!c9.RootPartitionsMismatch);
    }
}
