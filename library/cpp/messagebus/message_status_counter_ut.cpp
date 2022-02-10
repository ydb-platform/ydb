#include <library/cpp/testing/unittest/registar.h>

#include "message_status_counter.h"

#include <library/cpp/messagebus/monitoring/mon_proto.pb.h>

using namespace NBus;
using namespace NBus::NPrivate;

Y_UNIT_TEST_SUITE(MessageStatusCounter) {
    Y_UNIT_TEST(MessageStatusConversion) {
        const ::google::protobuf::EnumDescriptor* descriptor =
            TMessageStatusRecord_EMessageStatus_descriptor();

        for (int i = 0; i < MESSAGE_STATUS_COUNT; i++) {
            const ::google::protobuf::EnumValueDescriptor* valueDescriptor =
                descriptor->FindValueByName(ToString((EMessageStatus)i));
            UNIT_ASSERT_UNEQUAL(valueDescriptor, nullptr);
            UNIT_ASSERT_EQUAL(valueDescriptor->number(), i);
        }
        UNIT_ASSERT_EQUAL(MESSAGE_STATUS_COUNT, descriptor->value_count());
    }
}
