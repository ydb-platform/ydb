#pragma once

#include "hide_field_printer.h"
#include <ydb/public/api/protos/annotations/sensitive.pb.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <util/generic/set.h>

namespace NKikimr {

class TSecurityTextFormatPrinterBase : public google::protobuf::TextFormat::Printer {
public:
    using THideField = bool (*)(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor*);

    static bool IsSensitive(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field) {
        const auto& options = field->options();
        return options.GetExtension(Ydb::sensitive);
    }

    TSecurityTextFormatPrinterBase(const google::protobuf::Descriptor* desc, THideField hideField = &TSecurityTextFormatPrinterBase::IsSensitive) {
        TSet<std::pair<TString, int>> visited;
        Walk(desc, visited, hideField);
    }

    void Walk(const google::protobuf::Descriptor* desc, TSet<std::pair<TString, int>>& visited, THideField hideField) {
        if (!desc || visited.contains(std::pair<TString, int>{desc->full_name(), desc->index()})) {
            return;
        }
        visited.insert({desc->full_name(), desc->index()});
        for (int i = 0; i < desc->field_count(); i++) {
            const auto field = desc->field(i);
            if (hideField && hideField(desc, field)) {
                RegisterFieldValuePrinter(field, new THideFieldValuePrinter());
            }
            Walk(field->message_type(), visited, hideField);
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

template <typename TMsg>
inline TString SecureDebugStringMultiline(const TMsg& message) {
    TString result;
    TSecurityTextFormatPrinter<TMsg> printer;
    printer.SetSingleLineMode(false);
    printer.PrintToString(message, &result);
    return result;
}

} // namespace NKikimr
