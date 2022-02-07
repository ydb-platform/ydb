#include "hide_field_printer.h"

namespace NKikimr {

void THideFieldValuePrinter::PrintBool(bool, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintInt32(i32, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintUInt32(ui32, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintInt64(i64, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintUInt64(ui64, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintFloat(float, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintDouble(double, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

void THideFieldValuePrinter::PrintString(const TString&, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("\"***\"");
}

void THideFieldValuePrinter::PrintBytes(const TString&, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("\"***\"");
}

void THideFieldValuePrinter::PrintEnum(i32, const TString&, google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
}

bool THideFieldValuePrinter::PrintMessageContent(const google::protobuf::Message&, int,
                                                 int, bool singleLineMode,
                                                 google::protobuf::TextFormat::BaseTextGenerator* generator) const {
    generator->PrintLiteral("***");
    if (singleLineMode) {
        generator->PrintLiteral(" ");
    } else {
        generator->PrintLiteral("\n");
    }
    return true /* don't use default printing logic */;
}

} // namespace NKikimr
