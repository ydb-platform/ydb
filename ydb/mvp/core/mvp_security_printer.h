#pragma once
#include <ydb/library/security/util.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <util/generic/set.h>

#include <memory>

namespace NMVP {

class TMVPSecurityTextFormatPrinterBase : public google::protobuf::TextFormat::Printer {
public:
    TMVPSecurityTextFormatPrinterBase(const google::protobuf::Descriptor* desc);

    void Walk(const google::protobuf::Descriptor* desc, TSet<std::pair<TString, int>>& visited);
};

template<typename TMsg>
class TMVPSecurityTextFormatPrinter : public TMVPSecurityTextFormatPrinterBase {
public:
    TMVPSecurityTextFormatPrinter()
        : TMVPSecurityTextFormatPrinterBase(TMsg::descriptor())
    {}
};

template <typename TMsg>
inline TString MVPSecureDebugString(const TMsg& message) {
    TString result;
    TMVPSecurityTextFormatPrinter<TMsg> printer;
    printer.SetSingleLineMode(true);
    printer.PrintToString(message, &result);
    return result;
}

} // namespace NMVP
