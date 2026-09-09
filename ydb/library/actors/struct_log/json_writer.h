#pragma once

#include "base_writer.h"

#include <library/cpp/json/writer/json.h>

#include <util/generic/string.h>

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

    template <typename T>
    void AppendValue(const T& value) {
        if constexpr (std::is_same_v<T, TString>) {
            JsonWriter.WriteString(value);
        } else if constexpr (std::is_same_v<T, i8>) {
            JsonWriter.WriteString(std::to_string(value));
        } else if constexpr (std::is_same_v<T, ui8> || std::is_same_v<T, ui16> || std::is_same_v<T, ui32> || std::is_same_v<T, ui64>) {
            JsonWriter.WriteULongLong(value);
        } else if constexpr (std::is_same_v<T, i16> || std::is_same_v<T, i32> || std::is_same_v<T, i64>) {
            JsonWriter.WriteLongLong(value);
        } else if constexpr (std::is_same_v<T, bool>) {
            JsonWriter.WriteBool(value);
        } else {
            static_assert(false, "Attempt to serialize unsupported type");
        }
    }

};

class TJsonWriter {
    friend class TBaseMessageWriter<TJsonWriter>;

public:
    TJsonWriter(const TJsonKeyValueWriter::TNameSet& reservedKeyNames = TJsonKeyValueWriter::TNameSet());

    bool Write(NJsonWriter::TBuf& jsonWriter, const TStructuredMessage& message, bool jsonStarted = false);

protected:
    const TJsonKeyValueWriter::TNameSet& ReservedKeyNames;
    TJsonKeyValueWriter* KeyValueWriter{nullptr};

    struct TValueWriter : public TBaseValueWriter<TJsonWriter> {
        TValueWriter(TJsonWriter& writer);

        void operator()(const auto& value) const {
            Writer.KeyValueWriter->AppendKeyValue(*KeyName, value);
        }
    };

    TBaseMessageWriter<TJsonWriter> MessageWriter{*this};
};

}  // namespace NActors::NStructuredLog
