#pragma once

#include "hide_field_printer.h"
#include <ydb/public/api/protos/annotations/sensitive.pb.h>

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <util/generic/set.h>

namespace NKikimr {

class TSecurityTextFormatPrinterBase : public google::protobuf::TextFormat::Printer {
public:
    TSecurityTextFormatPrinterBase(const google::protobuf::Descriptor* desc) {
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
            if (options.GetExtension(Ydb::sensitive)) {
                RegisterFieldValuePrinter(field, new THideFieldValuePrinter());
            }
            Walk(field->message_type(), visited);
        }
    }
};

template<typename TMsg>
class TSecurityTextFormatPrinter : public TSecurityTextFormatPrinterBase {
public:
    TSecurityTextFormatPrinter()
        : TSecurityTextFormatPrinterBase(TMsg::descriptor())
    {}
};

template <typename TMsg>
inline TString SecureDebugString(const TMsg& message) {
    TString result;
    TSecurityTextFormatPrinter<TMsg> printer;
    printer.SetSingleLineMode(true);
    printer.PrintToString(message, &result);
    return result;
}

} // namespace NKikimr
