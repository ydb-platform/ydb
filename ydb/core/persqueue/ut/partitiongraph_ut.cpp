
#include <ydb/core/persqueue/utils.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NPQ;

Y_UNIT_TEST_SUITE(TPartitionGraphTest) {
    Y_UNIT_TEST(BuildGraph) {

        // 0 ------------
        // 1 -|
        //    |- 3 -|
        // 2 -|     |- 5
        // 4 -------|
        //
        NKikimrPQ::TPQTabletConfig config;

        // Without parents and childrens
        auto* p0 = config.AddAllPartitions();
        p0->SetPartitionId(0);

        auto* p1 = config.AddAllPartitions();
        p1->SetPartitionId(1);
        p1->AddChildPartitionIds(3);

        auto* p2 = config.AddAllPartitions();
        p2->SetPartitionId(2);
        p2->AddChildPartitionIds(3);

        auto* p3 = config.AddAllPartitions();
        p3->SetPartitionId(3);
        p3->AddChildPartitionIds(5);
        p3->AddParentPartitionIds(1);
        p3->AddParentPartitionIds(2);

        auto* p4 = config.AddAllPartitions();
        p4->SetPartitionId(4);
        p4->AddChildPartitionIds(5);

        auto* p5 = config.AddAllPartitions();
        p5->SetPartitionId(5);
        p5->AddParentPartitionIds(3);
        p5->AddParentPartitionIds(4);

        TPartitionGraph graph = MakePartitionGraph(config);

        const auto n0 = graph.GetPartition(0);
        const auto n1 = graph.GetPartition(1);
        const auto n2 = graph.GetPartition(2);
        const auto n3 = graph.GetPartition(3);
        const auto n4 = graph.GetPartition(4);
        const auto n5 = graph.GetPartition(5);

        UNIT_ASSERT(n0);
        UNIT_ASSERT(n1);
        UNIT_ASSERT(n2);
        UNIT_ASSERT(n3);
        UNIT_ASSERT(n4);
        UNIT_ASSERT(n5);

        UNIT_ASSERT_VALUES_EQUAL(n0->Parents.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(n0->Children.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(n0->HierarhicalParents.size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(n1->Parents.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(n1->Children.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(n1->HierarhicalParents.size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(n5->Parents.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(n5->Children.size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(n5->HierarhicalParents.size(), 4);
        UNIT_ASSERT(std::find(n5->HierarhicalParents.cbegin(),  n5->HierarhicalParents.cend(), n0) == n5->HierarhicalParents.end());
        UNIT_ASSERT(std::find(n5->HierarhicalParents.cbegin(),  n5->HierarhicalParents.cend(), n1) != n5->HierarhicalParents.end());
        UNIT_ASSERT(std::find(n5->HierarhicalParents.cbegin(),  n5->HierarhicalParents.cend(), n2) != n5->HierarhicalParents.end());
        UNIT_ASSERT(std::find(n5->HierarhicalParents.cbegin(),  n5->HierarhicalParents.cend(), n3) != n5->HierarhicalParents.end());
        UNIT_ASSERT(std::find(n5->HierarhicalParents.cbegin(),  n5->HierarhicalParents.cend(), n4) != n5->HierarhicalParents.end());

        {
            std::set<ui32> traversedNodes;
            graph.Travers([&](ui32 id) {
                traversedNodes.insert(id);
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(traversedNodes.size(), 6);
        }

        {
            std::set<ui32> traversedNodes;
            graph.Travers(0,[&](ui32 id) {
                traversedNodes.insert(id);
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(traversedNodes.size(), 0);
        }

        {
            std::set<ui32> traversedNodes;
            graph.Travers(0,[&](ui32 id) {
                traversedNodes.insert(id);
                return true;
            }, true);
            UNIT_ASSERT_VALUES_EQUAL(traversedNodes.size(), 1);
            UNIT_ASSERT(traversedNodes.contains(0));
        }

        {
            std::set<ui32> traversedNodes;
            graph.Travers(2,[&](ui32 id) {
                traversedNodes.insert(id);
                return true;
            }, true);
            UNIT_ASSERT_VALUES_EQUAL(traversedNodes.size(), 3);
            UNIT_ASSERT(traversedNodes.contains(2));
            UNIT_ASSERT(traversedNodes.contains(3));
            UNIT_ASSERT(traversedNodes.contains(5));
        }
    }
}
