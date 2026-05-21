#include "text_writer.h"

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
        auto str = keyItem.ToString();
        for (auto& ch: str) {
            if (ch == '\n') {
                ch = ' ';
            }
        }
        outputText << str;
    }
    outputText << "=";
    outputText << TTypesMapping::ToString(value);
}

}  // namespace NActors::NStructuredLog
