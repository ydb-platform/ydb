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
    if (str.find_first_of(" ='\"\\\n")!=std::string::npos)
    {
        outputText << "\"";
        for(std::string::size_type pos = 0;pos < str.size();pos++) {
            if (str[pos]=='"') outputText << "\\\"";
            else if (str[pos]=='\\') outputText << "\\\\";
            else if (str[pos]=='\n') outputText << "\\n";
            else outputText << str[pos];
        }
        outputText << "\"";
    } else {
        outputText << str;
    }
}

}  // namespace NActors::NStructuredLog
