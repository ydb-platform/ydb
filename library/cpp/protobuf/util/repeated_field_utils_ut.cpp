#include "repeated_field_utils.h"
#include <library/cpp/protobuf/util/ut/common_ut.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NProtoBuf;

Y_UNIT_TEST_SUITE(RepeatedFieldUtils) {
    Y_UNIT_TEST(RemoveIf) {
        {
            NProtobufUtilUt::TWalkTest msg;
            msg.AddRepInt(0);
            msg.AddRepInt(1);
            msg.AddRepInt(2);
            msg.AddRepInt(3);
            msg.AddRepInt(4);
            msg.AddRepInt(5);
            auto cond = [](ui32 val) {
                return val % 2 == 0;
            };
            RemoveRepeatedFieldItemIf(msg.MutableRepInt(), cond);
            UNIT_ASSERT_VALUES_EQUAL(3, msg.RepIntSize());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.GetRepInt(0));
            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetRepInt(1));
            UNIT_ASSERT_VALUES_EQUAL(5, msg.GetRepInt(2));
        }

        {
            NProtobufUtilUt::TWalkTest msg;
            msg.AddRepSub()->SetOptInt(0);
            msg.AddRepSub()->SetOptInt(1);
            msg.AddRepSub()->SetOptInt(2);
            msg.AddRepSub()->SetOptInt(3);
            msg.AddRepSub()->SetOptInt(4);
            msg.AddRepSub()->SetOptInt(5);
            auto cond = [](const NProtobufUtilUt::TWalkTest& val) {
                return val.GetOptInt() % 2 == 0;
            };
            RemoveRepeatedFieldItemIf(msg.MutableRepSub(), cond);
            UNIT_ASSERT_VALUES_EQUAL(3, msg.RepSubSize());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.GetRepSub(0).GetOptInt());
            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetRepSub(1).GetOptInt());
            UNIT_ASSERT_VALUES_EQUAL(5, msg.GetRepSub(2).GetOptInt());
        }
    }
}
