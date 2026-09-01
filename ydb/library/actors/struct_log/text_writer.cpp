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

TString TTextWriter::EscapeFieldValue(const TString& value) {
    if (value.find_first_of(" ='\"\\\n")==std::string::npos) {
        return value;
    }

    TStringStream result;
    result << "\"";
    for(std::string::size_type pos = 0;pos < value.size();pos++) {
        if (value[pos]=='"') result << "\\\"";
        else if (value[pos]=='\\') result << "\\\\";
        else if (value[pos]=='\n') result << "\\n";
        else result << value[pos];
    }
    result << "\"";
    return result.Str();
}

TMaybe<TString> TTextWriter::UnescapeFieldValue(const TString& escapedFieldValue) {
    std::string::size_type startPos = 0;
    return UnescapeFieldValue(escapedFieldValue, startPos);
}

TMaybe<TString> TTextWriter::UnescapeFieldValue(const TString& escapedFieldValue, std::string::size_type& startPos) {
    if (startPos >= escapedFieldValue.size()) {
        return "";
    }

    if (escapedFieldValue[startPos]!='"') {
        auto pos = startPos;
        auto spacePos = escapedFieldValue.find(' ', startPos);
        if (spacePos == std::string::npos) {
            startPos = escapedFieldValue.size();
            return escapedFieldValue.substr(pos);
        } else {
            startPos = spacePos;
            return escapedFieldValue.substr(pos, spacePos - pos);
        }
    }

    TStringStream result;
    startPos++;
    while (startPos < escapedFieldValue.size()) {
        if (escapedFieldValue[startPos]=='"') {
            // End of escaped string found
            startPos++;
            return result.Str();
        } else if (escapedFieldValue[startPos]=='\\') {
            startPos++;
            if (startPos >= escapedFieldValue.size()) {
                // invalid escaped string (\ is last character)
                return {};
            } else {
                if (escapedFieldValue[startPos]=='n') result << '\n';
                else if (escapedFieldValue[startPos]=='\\') result << '\\';
                else if (escapedFieldValue[startPos]=='"') result << '"';
                else {
                    // invalid escaped string (unknown character after "\")
                    return {};
                }
                startPos++;
            }
        } else {
            result << escapedFieldValue[startPos];
            startPos++;
        }
    }

    // End of escaped string is not found
    return {};
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
    outputText << EscapeFieldValue(str);
}

}  // namespace NActors::NStructuredLog
