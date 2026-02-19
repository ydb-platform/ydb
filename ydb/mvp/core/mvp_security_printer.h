#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <ydb/library/protobuf_printer/security_printer.h>

#include <type_traits>

namespace NMVP {

bool MvpShouldHideSensitiveField(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field);

template <typename TMsg>
inline TString SecureShortDebugString(const TMsg& message) {
    static_assert(std::is_base_of_v<::google::protobuf::Message, TMsg>, "SecureShortDebugString requires a protobuf message");
    TString result;
    NKikimr::TSecurityTextFormatPrinterBase printer(TMsg::descriptor(), &NMVP::MvpShouldHideSensitiveField);
    printer.SetSingleLineMode(true);
    printer.PrintToString(message, &result);
    return result;
}

} // namespace NMVP
