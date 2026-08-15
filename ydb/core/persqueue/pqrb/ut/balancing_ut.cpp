#include <ydb/core/persqueue/pqrb/read_balancer__balancing.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbBalancing) {

Y_UNIT_TEST(UninitializedPartitionAcceptsFirstCommit) {
    NBalancing::TPartition partition;
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 0u);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionCookie, 0u);
    UNIT_ASSERT(partition.SetCommittedState(1, 1));
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 1u);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionCookie, 1u);
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancing)

} // namespace NKikimr::NPQ
