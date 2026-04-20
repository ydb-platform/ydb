#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <library/cpp/json/writer/json.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <unordered_set>
#include <vector>

namespace NKikimr::NStructLog {

class TJsonKeyValueWriter
{
public:
    using TNameSet = std::unordered_set<TString>;

    TJsonKeyValueWriter(NJsonWriter::TBuf& jsonWriter, const TJsonKeyValueWriter::TNameSet& busyKeyNames, bool jsonStarted) :
        JsonWriter(jsonWriter), BusyKeyNames(busyKeyNames), JsonStarted(jsonStarted) {
    }

    void Done() {
        if (!JsonStarted) {
            JsonWriter.BeginObject();
            JsonWriter.EndObject();
        }
        else
        {
            for(std::size_t i = 0; i < LastAppendedKey.size(); i++) {
                JsonWriter.EndObject();
            }
        }
    }

    template <typename T>
    void AppendKeyValue(const std::vector<TKeyName>& key, const T& value) {
        if (!JsonStarted) {
            JsonWriter.BeginObject();
            JsonStarted = true;
        }

        // Prepare contexts
        auto currentContext = GetContext(LastAppendedKey);
        auto requiredContext = GetContext(key);

        // Check context equality length
        std::size_t index = 0;
        while (index < currentContext.size() && index < requiredContext.size() && currentContext[index] == requiredContext[index]) {
            index++;
        }

        // Check json name suffix
        if (LastAppendedKey.size() < key.size() &&
            index == LastAppendedKey.size() - 1 &&
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
                if (index == 0 && BusyKeyNames.contains(keyName)) {
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
        if (key.size() == 1 && BusyKeyNames.contains(keyName)) {
            JsonWriter.WriteKey(TString("_") + keyName);
        } else {
            JsonWriter.WriteKey(keyName);
        }
        AppendValue(value);

        LastAppendedKey = key;
    }

protected:

    NJsonWriter::TBuf& JsonWriter;
    const TJsonKeyValueWriter::TNameSet& BusyKeyNames;
    bool JsonStarted{false};

    std::vector<TKeyName> LastAppendedKey;

    std::vector<TKeyName> GetContext(const std::vector<TKeyName>& key) {
        auto result = key;
        if (!result.empty()) {
            result.pop_back();
        }
        return result;
    }

    void AppendValue(const i8& value) { JsonWriter.WriteLongLong(value); }
    void AppendValue(const ui8& value) { JsonWriter.WriteULongLong(value); }
    void AppendValue(const i16& value) { JsonWriter.WriteLongLong(value); }
    void AppendValue(const ui16& value) { JsonWriter.WriteULongLong(value); }
    void AppendValue(const i32& value) { JsonWriter.WriteLongLong(value); }
    void AppendValue(const ui32& value) { JsonWriter.WriteULongLong(value); }
    void AppendValue(const i64& value) { JsonWriter.WriteLongLong(value); }
    void AppendValue(const ui64& value) { JsonWriter.WriteULongLong(value); }
    void AppendValue(const bool& value) { JsonWriter.WriteBool(value); }
    void AppendValue(const TString& value) { JsonWriter.WriteString(value); }
    void AppendValue(const float& value) { JsonWriter.WriteFloat(value); }
    void AppendValue(const double& value) { JsonWriter.WriteDouble(value); }
    void AppendValue(const long double& value) { JsonWriter.WriteDouble(value); }
};

class TJsonWriter
{
public:
    TJsonWriter(const TJsonKeyValueWriter::TNameSet& busyKeyNames = TJsonKeyValueWriter::TNameSet()) :
        BusyKeyNames{busyKeyNames} {}

    bool Write(NJsonWriter::TBuf& jsonWriter, const TStructuredMessage& message, bool jsonStarted = false)
    {
        TJsonKeyValueWriter keyValueWriter{jsonWriter, BusyKeyNames, jsonStarted};
        KeyValueWriter = &keyValueWriter;

        if (!message.ForEachSerialized([&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length) {
                auto it = TypeValueWriterMap.find(typeCode);
                if (it != end(TypeValueWriterMap)) {
                    ValueWriter.KeyName = &name;
                    return it->second(data, length);
                } else {
                    return false;
                }})) {
            return false;
        };

        KeyValueWriter = nullptr;

        if (!jsonStarted) {
            keyValueWriter.Done();
        }
        return true;
    }

protected:

    const TJsonKeyValueWriter::TNameSet& BusyKeyNames;
    TJsonKeyValueWriter* KeyValueWriter{nullptr};

    class TJsonValueWriter {
        public:
            TJsonWriter& Writer;
            const std::vector<TKeyName>* KeyName {nullptr};

            TJsonValueWriter(TJsonWriter& writer) : Writer(writer) {}

            template <typename T>
            void operator()(const T& value) const {
                Writer.KeyValueWriter->AppendKeyValue(*KeyName, value);
            }
    };
    TJsonValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);

};

}
