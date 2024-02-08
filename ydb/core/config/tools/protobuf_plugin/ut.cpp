#include <ydb/core/config/tools/protobuf_plugin/ut/protos/config_root_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ValidationTests) {
    Y_UNIT_TEST(CanDispatchByTag) {
        NKikimrConfig::ActualConfigMessage msg;

        msg.MutableField21();

        auto [has1, get1, mut1] = NKikimrConfig::ActualConfigMessage::GetFieldAccessorsByFieldTag(NKikimrConfig::ActualConfigMessage::TField1FieldTag{});
        auto [has21, get21, mut21] = NKikimrConfig::ActualConfigMessage::GetFieldAccessorsByFieldTag(NKikimrConfig::ActualConfigMessage::TField21FieldTag{});

        Y_UNUSED(get21, get1);

        UNIT_ASSERT(!(msg.*has1)());
        UNIT_ASSERT((msg.*has21)());

        (msg.*mut1)();

        UNIT_ASSERT(msg.HasField1());
        UNIT_ASSERT((msg.*has1)());
    }
}
