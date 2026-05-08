#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <library/cpp/json/writer/json.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <unordered_set>
#include <vector>

namespace NActors::NStructuredLog {

class TJsonKeyValueWriter {
public:
    using TNameSet = std::unordered_set<TString>;

    TJsonKeyValueWriter(
        NJsonWriter::TBuf& jsonWriter,
        const TJsonKeyValueWriter::TNameSet& reservedKeyNames,
        bool jsonStarted
    );

    void Done();

    template <typename T>
    void AppendKeyValue(const std::vector<TKeyName>& key, const T& value) {
        if (JsonStartedState == EJsonStartedState::NotStarted) {
            JsonWriter.BeginObject();
            JsonStartedState = EJsonStartedState::Started;
        }

        // Prepare contexts
        auto currentContext = GetContext(LastAppendedKey);
        auto requiredContext = GetContext(key);

        // Check context equality length
        std::size_t index = 0;
        while (index < currentContext.size() && index < requiredContext.size() &&
               currentContext[index] == requiredContext[index]) {
            index++;
        }

        // Check json name suffix
        if (LastAppendedKey.size() < key.size() && index + 1 == LastAppendedKey.size() &&
            LastAppendedKey[index] == key[index]) {
            requiredContext[index] = TString("_") + requiredContext[index].ToString();
        }

        if (index < currentContext.size() || index < requiredContext.size()) {
            // Close current nested values
            while (currentContext.size() > index) {
                JsonWriter.EndObject();
                currentContext.pop_back();
            }

            // Open nested values
            while (index < requiredContext.size()) {
                auto keyName = requiredContext[index].ToString();
                if (index == 0 && ReservedKeyNames.contains(keyName)) {
                    JsonWriter.WriteKey(TString("_") + keyName);
                } else {
                    JsonWriter.WriteKey(keyName);
                }
                JsonWriter.BeginObject();
                index++;
            }
        }

        // Add key-value pair
        auto keyName = key.back().ToString();
        if (key.size() == 1 && ReservedKeyNames.contains(keyName)) {
            JsonWriter.WriteKey(TString("_") + keyName);
        } else {
            JsonWriter.WriteKey(keyName);
        }
        AppendValue(value);

        LastAppendedKey = key;
    }

protected:
    NJsonWriter::TBuf& JsonWriter;
    const TJsonKeyValueWriter::TNameSet& ReservedKeyNames;

    enum class EJsonStartedState {
        NotStarted,
        ParentStarted,
        Started,
    } JsonStartedState;

    std::vector<TKeyName> LastAppendedKey;

    std::vector<TKeyName> GetContext(const std::vector<TKeyName>& key);

    void AppendValue(const TString& value);
};

class TJsonWriter {
public:
    TJsonWriter(const TJsonKeyValueWriter::TNameSet& reservedKeyNames = TJsonKeyValueWriter::TNameSet());

    bool Write(NJsonWriter::TBuf& jsonWriter, const TStructuredMessage& message, bool jsonStarted = false);

protected:
    const TJsonKeyValueWriter::TNameSet& ReservedKeyNames;
    TJsonKeyValueWriter* KeyValueWriter{nullptr};

    struct TJsonValueWriter {
        TJsonWriter& Writer;
        const std::vector<TKeyName>* KeyName{nullptr};

        TJsonValueWriter(TJsonWriter& writer);

        template <typename T>
        void operator()(const T& value) const {
            Writer.KeyValueWriter->AppendKeyValue(*KeyName, value);
        }
    };
    TJsonValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);
};

}  // namespace NActors::NStructuredLog
