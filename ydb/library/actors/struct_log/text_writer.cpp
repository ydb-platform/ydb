#include "text_writer.h"

#include <util/string/subst.h>

namespace NActors::NStructuredLog {

bool TTextWriter::Write(TStringBuilder& outputText, const TStructuredMessage& message) {
    OutputText = &outputText;
    FirstValue = true;

    auto result = MessageWriter.WriteMessage(message);

    OutputText = nullptr;
    return result;
}

TTextWriter::TValueWriter::TValueWriter(TTextWriter& writer)
    : TBaseValueWriter<TTextWriter>(writer)
{}

void TTextWriter::TValueWriter::operator()(const TString& value) const {
    auto& outputText = *Writer.OutputText;
    if (Writer.FirstValue) {
        Writer.FirstValue = false;
    } else {
        outputText << " ";
    }

    bool first = true;

    for (const auto& keyItem : *KeyName) {
        if (first) {
            first = false;
        } else {
            outputText << ".";
        }
        outputText << keyItem.ToString();
    }
    outputText << "=";
    auto str = TTypesMapping::ToString(value);
    if (SubstGlobal(str, "\n", "\\n") !=0 ||
        SubstGlobal(str, "\"", "\\\"") !=0  ||
        SubstGlobal(str, "'", "\\'") !=0 ||
        str.Contains(" ") ||
        str.Contains("=")) {
        outputText << "\"";
        outputText << str;
        outputText << "\"";
    } else {
        outputText << str;
    }
}

}  // namespace NActors::NStructuredLog
