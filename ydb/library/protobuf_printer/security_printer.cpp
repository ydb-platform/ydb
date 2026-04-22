#include "security_printer.h"

namespace NKikimr {

namespace {

TString SecureDebugStringMessageImpl(const google::protobuf::Message& message, bool singleLine) {
    TString result;
    TSecurityTextFormatPrinterBase printer(message.GetDescriptor());
    printer.SetSingleLineMode(singleLine);
    printer.PrintToString(message, &result);
    return result;
}

} // namespace

TString SecureDebugString(const google::protobuf::Message& message) {
    return SecureDebugStringMessageImpl(message, true);
}

TString SecureDebugStringMultiline(const google::protobuf::Message& message) {
    return SecureDebugStringMessageImpl(message, false);
}

} // namespace NKikimr
