#include <ydb/public/sdk/cpp/src/client/topic/impl/producer.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/deferred_publication_ack_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTopic;

namespace NYdb::inline Dev::NTopic {

struct TProducerMessageInfoTestHelper {
    static TWriteMessage RoundTrip(TWriteMessage message) {
        TProducer::TMessageInfo info("key", "", std::move(message), /*partition=*/0);
        return info.BuildMessage();
    }
};

} // namespace NYdb::NTopic

Y_UNIT_TEST_SUITE(ProducerDeferredPublication) {

Y_UNIT_TEST(MessageInfoPreservesDeferredPublication) {
    TDeferredPublication publication(42, "ext-producer");
    TWriteMessage message("payload");
    message.DeferredPublication(publication);

    const auto rebuilt = TProducerMessageInfoTestHelper::RoundTrip(std::move(message));
    UNIT_ASSERT(rebuilt.DeferredPublication_.has_value());
    UNIT_ASSERT_VALUES_EQUAL(rebuilt.DeferredPublication_->IntPublicationId, 42u);
    UNIT_ASSERT(rebuilt.DeferredPublication_->ExtPublicationId.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*rebuilt.DeferredPublication_->ExtPublicationId, "ext-producer");
    UNIT_ASSERT(
        TDeferredPublication::TAccess::AckState(publication)
        == TDeferredPublication::TAccess::AckState(*rebuilt.DeferredPublication_));
}

Y_UNIT_TEST(MessageInfoPreservesEmptyDeferredPublication) {
    TWriteMessage message("payload");
    const auto rebuilt = TProducerMessageInfoTestHelper::RoundTrip(std::move(message));
    UNIT_ASSERT(!rebuilt.DeferredPublication_.has_value());
}

} // Y_UNIT_TEST_SUITE(ProducerDeferredPublication)
