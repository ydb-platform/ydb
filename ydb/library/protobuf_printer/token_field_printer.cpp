#include "token_field_printer.h"

#include <ydb/library/security/util.h>

namespace NKikimr {

void TTokenFieldValuePrinter::PrintString(const TProtoStringType& val, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    const TString masked = MaskTicket(val);
    generator->PrintLiteral("\"");
    generator->Print(masked.data(), masked.size());
    generator->PrintLiteral("\"");
}

} // namespace NKikimr
