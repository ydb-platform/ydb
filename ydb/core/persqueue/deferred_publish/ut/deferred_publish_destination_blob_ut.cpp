#include <ydb/core/persqueue/deferred_publish/destination_blob.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NDeferredPublish {
namespace {

Y_UNIT_TEST_SUITE(TDeferredPublishDestinationBlob) {

Y_UNIT_TEST(RoundTrip) {
    const auto blob = MakeDestinationBlob(7, 42);
    const auto bytes = SerializeDestinationBlob(blob);

    NKikimrPQ::TDeferredPublishDestinationBlob parsed;
    UNIT_ASSERT(ParseDestinationBlob(bytes, &parsed));
    UNIT_ASSERT_VALUES_EQUAL(parsed.PartitionsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(parsed.GetPartitions(0).GetPartitionId(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.GetPartitions(0).GetTabletId(), 42u);
}

Y_UNIT_TEST(AddOrUpdateTopicPartition) {
    auto blob = MakeDestinationBlob(1, 100);
    AddOrUpdateTopicPartition(&blob, 2, 200);
    AddOrUpdateTopicPartition(&blob, 1, 101);

    UNIT_ASSERT_VALUES_EQUAL(blob.PartitionsSize(), 2);
    UNIT_ASSERT_VALUES_EQUAL(FindTopicPartitionDestination(blob, 1)->GetTabletId(), 101u);
    UNIT_ASSERT_VALUES_EQUAL(FindTopicPartitionDestination(blob, 2)->GetTabletId(), 200u);
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NPQ::NDeferredPublish
