#include <ydb/core/persqueue/deferred_publish/destination_blob.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NDeferredPublish {
namespace {

Y_UNIT_TEST_SUITE(TDeferredPublishDestinationBlob) {

Y_UNIT_TEST(RoundTrip) {
    const auto blob = MakeDestinationBlob("/Root/topic-a", 7, 42);
    const auto bytes = SerializeDestinationBlob(blob);

    NKikimrPQ::TDeferredPublishDestinationBlob parsed;
    UNIT_ASSERT(ParseDestinationBlob(bytes, &parsed));
    UNIT_ASSERT_VALUES_EQUAL(parsed.GetPath(), "/Root/topic-a");
    UNIT_ASSERT_VALUES_EQUAL(parsed.GetPartitionId(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.GetTabletId(), 42u);
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NPQ::NDeferredPublish
