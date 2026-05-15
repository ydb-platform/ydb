#include "json_writer.h"

namespace NActors::NStructuredLog {

TJsonKeyValueWriter::TJsonKeyValueWriter(
        NJsonWriter::TBuf& jsonWriter,
        const TJsonKeyValueWriter::TNameSet& reservedKeyNames,
        bool jsonStarted)
    : JsonWriter(jsonWriter)
    , ReservedKeyNames(reservedKeyNames)
    , JsonStartedState(jsonStarted ? EJsonStartedState::ParentStarted : EJsonStartedState::NotStarted)
{}

void TJsonKeyValueWriter::Done() {
    switch (JsonStartedState) {
        case EJsonStartedState::NotStarted:
            JsonWriter.BeginObject();
            JsonWriter.EndObject();
            break;
        case EJsonStartedState::ParentStarted:
            for (std::size_t i = 1; i < LastAppendedKey.size(); i++) {
                JsonWriter.EndObject();
            }
            break;
        case EJsonStartedState::Started:
            for (std::size_t i = 1; i < LastAppendedKey.size(); i++) {
                JsonWriter.EndObject();
            }
            // Close whole json
            JsonWriter.EndObject();
            break;
    }
}

std::vector<TKeyName> TJsonKeyValueWriter::GetContext(const std::vector<TKeyName>& key) {
    auto result = key;
    if (!result.empty()) {
        result.pop_back();
    }
    return result;
}

void TJsonKeyValueWriter::AppendValue(const TString& value) {
    JsonWriter.WriteString(value);
}

TJsonWriter::TJsonWriter(const TJsonKeyValueWriter::TNameSet& reservedKeyNames)
    : ReservedKeyNames{reservedKeyNames}
{}

bool TJsonWriter::Write(NJsonWriter::TBuf& jsonWriter, const TStructuredMessage& message, bool jsonStarted) {
    TJsonKeyValueWriter keyValueWriter{jsonWriter, ReservedKeyNames, jsonStarted};
    KeyValueWriter = &keyValueWriter;

    auto result = MessageWriter.WriteMessage(message);

    KeyValueWriter = nullptr;
    keyValueWriter.Done();
    return result;
}

TJsonWriter::TValueWriter::TValueWriter(TJsonWriter& writer)
    : TBaseValueWriter<TJsonWriter>(writer)
{}

void TJsonWriter::TValueWriter::operator()(const TString& value) const {
    Writer.KeyValueWriter->AppendKeyValue(*KeyName, value);
}

}  // namespace NActors::NStructuredLog
