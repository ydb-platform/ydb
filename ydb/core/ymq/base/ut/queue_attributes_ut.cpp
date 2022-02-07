#include <ydb/core/ymq/base/queue_attributes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(QueueAttributes) {
    Y_UNIT_TEST(BasicStdTest) {
        const bool isFifo = false;
        const bool clamp = false;
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "0"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "2.5"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "100500"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"FifoQueue", "true"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"ContentBasedDeduplication", "true"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"ContentBasedDeduplication", "false"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "0"}, {"MaximumMessageSize", "1023"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "0"}, {"MaximumMessageSize", "1024"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "0"}, {"MaximumMessageSize", "262144"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"DelaySeconds", "0"}, {"MaximumMessageSize", "262145"}}, {}, isFifo, clamp).Validate());
    }

    Y_UNIT_TEST(BasicFifoTest) {
        const bool isFifo = true;
        const bool clamp = false;
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"FifoQueue", "true"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"FifoQueue", "false"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"FifoQueue", "omg"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"ContentBasedDeduplication", "true"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"ContentBasedDeduplication", "false"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"ReceiveMessageWaitTimeSeconds", "0"}, {"VisibilityTimeout", "1.5"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"ReceiveMessageWaitTimeSeconds", "1"}, {"VisibilityTimeout", "0"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"ReceiveMessageWaitTimeSeconds", "2"}, {"VisibilityTimeout", "31"}}, {}, isFifo, clamp).Validate());
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"ReceiveMessageWaitTimeSeconds", "3"}, {"VisibilityTimeout", "100500"}}, {}, isFifo, clamp).Validate());
    }

    Y_UNIT_TEST(BasicClampTest) {
        const bool isFifo = false;
        const bool clamp = true;
        UNIT_ASSERT(!TQueueAttributes::FromAttributesAndConfig({{"MaximumMessageSize", "100000"}}, {}, isFifo, clamp).HasClampedAttributes());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"MaximumMessageSize", "1"}}, {}, isFifo, clamp).HasClampedAttributes());
        UNIT_ASSERT(TQueueAttributes::FromAttributesAndConfig({{"MaximumMessageSize", "1000000"}}, {}, isFifo, clamp).HasClampedAttributes());
        UNIT_ASSERT(*TQueueAttributes::FromAttributesAndConfig({{"MaximumMessageSize", "1"}}, {}, isFifo, clamp).MaximumMessageSize != 1);
        UNIT_ASSERT(*TQueueAttributes::FromAttributesAndConfig({{"MaximumMessageSize", "1000000"}}, {}, isFifo, clamp).MaximumMessageSize != 1000000);
    }
}

} // namespace NKikimr::NSQS
