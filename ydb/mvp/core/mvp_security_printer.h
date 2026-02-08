#pragma once
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <ydb/library/protobuf_printer/security_printer.h>

#include <ydb/public/api/client/nc_private/annotations.pb.h>
#include <ydb/public/api/client/yc_private/accessservice/sensitive.pb.h>

#include <type_traits>

namespace NMVP {

static inline bool NeedHideField(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field) {
    const auto& ops = field->options();
    return ops.GetExtension(nebius::sensitive) || ops.GetExtension(nebius::credentials) || ops.GetExtension(yandex::cloud::sensitive);
}

template <typename TMsg>
inline TString SecureShortDebugString(const TMsg& message) {
    static_assert(std::is_base_of_v<::google::protobuf::Message, TMsg>, "SecureShortDebugString requires a protobuf message");
    TString result;
    NKikimr::TSecurityTextFormatPrinterBase printer(TMsg::descriptor(), &NMVP::NeedHideField);
    printer.SetSingleLineMode(true);
    printer.PrintToString(message, &result);
    return result;
}

} // namespace NMVP
