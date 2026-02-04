#include "mvp_security_printer.h"
#include <ydb/library/protobuf_printer/hide_field_printer.h>

NMVP::TMVPSecurityTextFormatPrinterBase::TMVPSecurityTextFormatPrinterBase(const google::protobuf::Descriptor* desc) {
    TSet<std::pair<TString, int>> visited;
    Walk(desc, visited);
}

void NMVP::TMVPSecurityTextFormatPrinterBase::Walk(const google::protobuf::Descriptor* desc, TSet<std::pair<TString, int>>& visited) {
    if (!desc || visited.contains(std::pair<TString, int>{desc->full_name(), desc->index()})) {
        return;
    }
    visited.insert({desc->full_name(), desc->index()});
    for (int i = 0; i < desc->field_count(); i++) {
        const auto field = desc->field(i);
        const auto options = field->options();
        if (options.GetExtension(nebius::sensitive) || options.GetExtension(nebius::credentials)) {
            RegisterFieldValuePrinter(field, new NKikimr::THideFieldValuePrinter());
        }
        Walk(field->message_type(), visited);
    }
}
