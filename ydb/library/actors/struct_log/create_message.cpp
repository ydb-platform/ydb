#include "create_message.h"

#include <google/protobuf/text_format.h>

namespace NActors::NStructuredLog {

namespace {
    thread_local std::vector<TStructuredMessage> BuildMessageStack;
}

TCreateMessageGuard::TCreateMessageGuard() {
    PushBuildMessage();
}

TCreateMessageGuard::~TCreateMessageGuard() {
    if (!Popped) {
        PopBuildMessage();
    }
}

TStructuredMessage TCreateMessageGuard::Pop() {
    Popped = true;
    return PopBuildMessage();
}

TStructuredMessage& TCreateMessageGuard::PushBuildMessage() {
    BuildMessageStack.push_back(TStructuredMessage());
    return GetBuildMessage();
}

TStructuredMessage& TCreateMessageGuard::GetBuildMessage() {
    return BuildMessageStack.back();
}

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

TCreateMessageArg::TCreateMessageArg(const TStructuredMessage& message) {
    TCreateMessageGuard::GetBuildMessage().AppendMessage(message);
}

TCreateMessageArg::TCreateMessageArg(const TMaybe<TStructuredMessage>& message) {
    if (message.Defined()) {
        TCreateMessageGuard::GetBuildMessage().AppendMessage(message.GetRef());
    }
}

}  // namespace NActors::NStructuredLog
