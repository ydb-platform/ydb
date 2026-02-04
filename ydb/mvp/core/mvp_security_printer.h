#pragma once

#include <ydb/public/api/client/nc_private/annotations.pb.h>
#include <ydb/library/protobuf_printer/hide_field_printer.h>
#include <ydb/library/security/util.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <util/generic/set.h>

#include <memory>

namespace NMVP {

class TMVPSecurityTextFormatPrinterBase : public google::protobuf::TextFormat::Printer {
public:
    TMVPSecurityTextFormatPrinterBase(const google::protobuf::Descriptor* desc) {
        TSet<std::pair<TString, int>> visited;
        Walk(desc, visited);
    }

    void Walk(const google::protobuf::Descriptor* desc, TSet<std::pair<TString, int>>& visited) {
        if (!desc || visited.contains(std::pair<TString, int>{desc->full_name(), desc->index()})) {
            return;
        }
        visited.insert({desc->full_name(), desc->index()});
        for (int i = 0; i < desc->field_count(); i++) {
            const auto field = desc->field(i);
            const auto options = field->options();
            if (options.GetExtension(nebius::sensitive) || options.GetExtension(nebius::credentials)) {
                // NOTE FieldValuePrinter registration is order-dependent.
                // If another module registers earlier, this one is ignored.
                RegisterFieldValuePrinter(field, new NKikimr::THideFieldValuePrinter());
            }
            Walk(field->message_type(), visited);
        }
    }
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
