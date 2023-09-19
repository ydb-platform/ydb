#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/protobuf/dynamic_prototype/dynamic_prototype.h>
#include <library/cpp/protobuf/dynamic_prototype/generate_file_descriptor_set.h>
#include <library/cpp/protobuf/dynamic_prototype/ut/my_message.pb.h>


Y_UNIT_TEST_SUITE(TDynamicPrototype) {
    Y_UNIT_TEST(Create) {
        const auto* descriptor = TMyMessage().GetDescriptor();
        const auto& fileDescriptorSet = GenerateFileDescriptorSet(descriptor);

        auto prototype = TDynamicPrototype::Create(fileDescriptorSet, "TMyMessage");
        auto myMessage = prototype->Create();

        TMyMessage reference;
        reference.SetFloat(12.1);
        reference.MutableInnerMessage()->SetInt32(42);
        reference.MutableInnerMessage()->SetString("string");

        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD reference.SerializeToString(&serialized);
        Y_PROTOBUF_SUPPRESS_NODISCARD myMessage->ParseFromString(serialized);

        UNIT_ASSERT_STRINGS_EQUAL(reference.DebugString(), myMessage->DebugString());
    }
}
