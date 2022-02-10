#pragma once

#include <util/generic/fwd.h>

namespace google {
    namespace protobuf {
        class Message;
        class FieldDescriptor;
    }
}

namespace NProtoBuf {
    using ::google::protobuf::FieldDescriptor;
    using ::google::protobuf::Message;
}

namespace NProtoBuf {
    // Reflection-based equality check for arbitrary protobuf messages

    // Strict comparison: optional field without value is NOT equal to
    // a field with explicitly set default value.
    bool IsEqual(const Message& m1, const Message& m2);
    bool IsEqual(const Message& m1, const Message& m2, TString* differentPath);

    bool IsEqualField(const Message& m1, const Message& m2, const FieldDescriptor& field);

    // Non-strict version: optional field without explicit value is compared
    // using its default value.
    bool IsEqualDefault(const Message& m1, const Message& m2);

    bool IsEqualFieldDefault(const Message& m1, const Message& m2, const FieldDescriptor& field);

}
