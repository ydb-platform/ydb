#include <ydb/core/persqueue/pqrb/partition_scale_manager_graph_cmp.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NMirror;
using namespace NYdb::NTopic;

namespace {

TPartitionGraph RootGraph(ui32 size) {
    NKikimrPQ::TPQTabletConfig config;
    for (ui32 i = 0; i < size; ++i) {
        auto* p = config.AddAllPartitions();
        p->SetPartitionId(i);
    }
    return MakePartitionGraph(config);
}

TPartitionGraph SplittedGraph() {
    NKikimrPQ::TPQTabletConfig config;
    auto* p0 = config.AddAllPartitions();
    p0->SetPartitionId(0);
    p0->AddChildPartitionIds(1);
    p0->AddChildPartitionIds(2);

    auto* p1 = config.AddAllPartitions();
    p1->SetPartitionId(1);
    p1->AddParentPartitionIds(0);

    auto* p2 = config.AddAllPartitions();
    p2->SetPartitionId(2);
    p2->AddParentPartitionIds(0);
    return MakePartitionGraph(config);
}

std::vector<TPartitionInfo> RootPartitions(ui32 size) {
    std::vector<TPartitionInfo> result;
    for (ui32 i = 0; i < size; ++i) {
        Ydb::Topic::DescribeTopicResult::PartitionInfo p;
        p.set_partition_id(i);
        p.set_active(true);
        result.push_back(p);
    }
    return result;
}

TPartitionInfo RootPartition(ui32 id) {
    Ydb::Topic::DescribeTopicResult::PartitionInfo p;
    p.set_partition_id(id);
    p.set_active(true);
    return p;
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbGraphCmp) {

Y_UNIT_TEST(EqualRootSetsHaveNoMismatch) {
    auto c = ComparePartitionGraphs(RootGraph(5), RootPartitions(5));
    UNIT_ASSERT(!c.RootPartitionsMismatch);
}

Y_UNIT_TEST(ExtraRootsInTargetAreIgnored) {
    auto c = ComparePartitionGraphs(RootGraph(10), RootPartitions(5));
    UNIT_ASSERT_C(!c.RootPartitionsMismatch, c.RootPartitionsMismatch->Error);
}

Y_UNIT_TEST(ExtraRootsInSourceProduceAlterPlan) {
    auto c = ComparePartitionGraphs(RootGraph(5), RootPartitions(10));
    UNIT_ASSERT(c.RootPartitionsMismatch);
    UNIT_ASSERT(!c.RootPartitionsMismatch->Error);
    UNIT_ASSERT_VALUES_EQUAL(c.RootPartitionsMismatch->AlterRootPartitions.size(), 10u);
    for (ui32 i = 0; i < 10; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(c.RootPartitionsMismatch->AlterRootPartitions[i].Id, i);
        UNIT_ASSERT_EQUAL(
            c.RootPartitionsMismatch->AlterRootPartitions[i].Action,
            (i < 5) ? EPartitionAction::Modify : EPartitionAction::Create
        );
    }
}

Y_UNIT_TEST(GapInMissingRootsIsAnError) {
    auto c = ComparePartitionGraphs(RootGraph(1), std::vector<TPartitionInfo>{
        RootPartition(0),
        RootPartition(1),
        RootPartition(3),
    });
    UNIT_ASSERT(c.RootPartitionsMismatch);
    UNIT_ASSERT(c.RootPartitionsMismatch->Error);
}

Y_UNIT_TEST(CannotCreateRootWhenPreviousAlreadySplit) {
    auto c = ComparePartitionGraphs(SplittedGraph(), RootPartitions(4));
    UNIT_ASSERT(c.RootPartitionsMismatch);
    UNIT_ASSERT(c.RootPartitionsMismatch->Error);
}

Y_UNIT_TEST(MissingSourcePartitionBeforeNewRootIsAnError) {
    auto c = ComparePartitionGraphs(RootGraph(2), std::vector<TPartitionInfo>{RootPartition(2)});
    UNIT_ASSERT(c.RootPartitionsMismatch);
    UNIT_ASSERT(c.RootPartitionsMismatch->Error);
}

Y_UNIT_TEST(NonRootSourcePartitionsAreSkipped) {
    std::vector<TPartitionInfo> source;
    source.push_back(RootPartition(0));

    Ydb::Topic::DescribeTopicResult::PartitionInfo child;
    child.set_partition_id(1);
    child.set_active(true);
    child.add_parent_partition_ids(0);
    source.push_back(child);

    auto c = ComparePartitionGraphs(RootGraph(1), source);
    UNIT_ASSERT(!c.RootPartitionsMismatch);
}

Y_UNIT_TEST(HoleInTargetBeforeNewRootIsAnError) {
    auto c = ComparePartitionGraphs(RootGraph(1), std::vector<TPartitionInfo>{
        RootPartition(0),
        RootPartition(2),
    });
    UNIT_ASSERT(c.RootPartitionsMismatch);
    UNIT_ASSERT(c.RootPartitionsMismatch->Error);
}

Y_UNIT_TEST(AlterPlanCopiesSourceBounds) {
    Ydb::Topic::DescribeTopicResult::PartitionInfo p0;
    p0.set_partition_id(0);
    p0.set_active(true);
    p0.mutable_key_range()->set_from_bound("aa");
    p0.mutable_key_range()->set_to_bound("mm");

    Ydb::Topic::DescribeTopicResult::PartitionInfo p1;
    p1.set_partition_id(1);
    p1.set_active(true);
    p1.mutable_key_range()->set_from_bound("mm");
    p1.mutable_key_range()->set_to_bound("zz");

    auto c = ComparePartitionGraphs(RootGraph(1), std::vector<TPartitionInfo>{p0, p1});
    UNIT_ASSERT(c.RootPartitionsMismatch);
    UNIT_ASSERT(!c.RootPartitionsMismatch->Error);
    UNIT_ASSERT_VALUES_EQUAL(c.RootPartitionsMismatch->AlterRootPartitions.size(), 2u);
    UNIT_ASSERT_EQUAL(c.RootPartitionsMismatch->AlterRootPartitions[0].Action, EPartitionAction::Modify);
    UNIT_ASSERT_EQUAL(c.RootPartitionsMismatch->AlterRootPartitions[1].Action, EPartitionAction::Create);
    UNIT_ASSERT(c.RootPartitionsMismatch->AlterRootPartitions[0].FromBound.has_value());
    UNIT_ASSERT(c.RootPartitionsMismatch->AlterRootPartitions[1].ToBound.has_value());
}

} // Y_UNIT_TEST_SUITE(TPqrbGraphCmp)
