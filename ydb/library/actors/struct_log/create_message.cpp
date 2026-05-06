#include "create_message.h"
#include "create_message_impl.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NStructuredLog {

namespace {
thread_local std::vector<TStructuredMessage> BuildMessageStack;
}

TStructuredMessage& TCreateMessageGuard::PushBuildMessage() {
    BuildMessageStack.push_back(TStructuredMessage());
    return GetBuildMessage();
}

TStructuredMessage& TCreateMessageGuard::GetBuildMessage() { return BuildMessageStack.back(); }

TStructuredMessage TCreateMessageGuard::PopBuildMessage() {
    auto result = std::move(GetBuildMessage());
    BuildMessageStack.pop_back();
    return result;
}

void TCreateMessageArg::OutputProtobufMessage(IOutputStream& s, const google::protobuf::Message& value) {
    google::protobuf::TextFormat::Printer p;
    p.SetSingleLineMode(true);
    TString str;
    if (p.PrintToString(value, &str)) {
        s << "{" << str << "}";
    } else {
        s << "<error>";
    }
}

void TCreateMessageArg::OutputProtobufEnum(IOutputStream& s, int enumValue, const google::protobuf::EnumDescriptor* descriptor) {
    if (descriptor) {
        if (const auto* val = descriptor->FindValueByNumber(enumValue)) {
            s << val->name();
        } else {
            s << enumValue;
        }
    } else {
        s << enumValue;
    }
}

}  // namespace NKikimr::NStructuredLog
