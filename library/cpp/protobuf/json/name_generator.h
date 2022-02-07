#pragma once

#include <util/generic/string.h>

#include <functional>

namespace google {
    namespace protobuf {
        class FieldDescriptor;
        class EnumValueDescriptor;
    }
}

namespace NProtobufJson {
    using TNameGenerator = std::function<TString(const google::protobuf::FieldDescriptor&)>;
    using TEnumValueGenerator = std::function<TString(const google::protobuf::EnumValueDescriptor&)>;

}
