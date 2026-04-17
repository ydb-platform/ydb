#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <library/cpp/json/writer/json.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <vector>

namespace NKikimr::NStructLog {

class TJsonAppender
{
public:

    TJsonAppender(NJsonWriter::TBuf& jsonWriter) : JsonWriter(jsonWriter) {
    }

    template <typename T>
    void AppendValue(const T& value) {
        Y_UNUSED(value);
        static_assert(false, "Unsupported type");
    }

    template <> void AppendValue(const i8& value) { JsonWriter.WriteLongLong(value); }
    template <> void AppendValue(const ui8& value) { JsonWriter.WriteULongLong(value); }
    template <> void AppendValue(const i16& value) { JsonWriter.WriteLongLong(value); }
    template <> void AppendValue(const ui16& value) { JsonWriter.WriteULongLong(value); }
    template <> void AppendValue(const i32& value) { JsonWriter.WriteLongLong(value); }
    template <> void AppendValue(const ui32& value) { JsonWriter.WriteULongLong(value); }
    template <> void AppendValue(const i64& value) { JsonWriter.WriteLongLong(value); }
    template <> void AppendValue(const ui64& value) { JsonWriter.WriteULongLong(value); }
    template <> void AppendValue(const bool& value) { JsonWriter.WriteBool(value); }
    template <> void AppendValue(const TString& value) { JsonWriter.WriteString(value); }

    template <typename T>
    bool Append(const std::vector<TKeyName>& key, const T& value) {
        if (!Started) {
            JsonWriter.BeginObject();
            Started = true;
        }

        // Prepare contexts
        auto currentContext = GetContext(LastAppendedKey);
        auto requiredContext = GetContext(key);

        // Check context equality length
        std::size_t index = 0;
        while (index < currentContext.size() && index < requiredContext.size() && currentContext[index] == requiredContext[index]) {
            index++;
        }

        // Check json support
        if (LastAppendedKey.size() < key.size() &&
            index == LastAppendedKey.size() - 1 &&
            LastAppendedKey[index] == key[index]) {
            // json can't treat already added key as group
            return false;
        }

        if (index < currentContext.size() || index < requiredContext.size()) {
            // Close current nested values
            while (currentContext.size() > index) {
                JsonWriter.EndObject();
                currentContext.pop_back();
            }

            // Open nested values
            while (index < requiredContext.size()) {
                JsonWriter.WriteKey(requiredContext[index].ToString());
                JsonWriter.BeginObject();
                index ++;
            }
        }

        // Add key-value
        JsonWriter.WriteKey(key.back().ToString());
        AppendValue(value);

        LastAppendedKey = key;
        return true;
    }

    void Done() {
        if (!Started) {
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

    NJsonWriter::TBuf& JsonWriter;
    std::vector<TKeyName> LastAppendedKey;
    bool Started{false};

    std::vector<TKeyName> GetContext(const std::vector<TKeyName>& key) {
        auto result = key;
        if (!result.empty()) {
            result.pop_back();
        }
        return result;
    }
};

class TJsonWriter
{
public:
    TJsonAppender Appender;
    bool Valid{true};

    TJsonWriter(NJsonWriter::TBuf& jsonWriter) : Appender(jsonWriter) {}

    class TJsonValueWriter {
        public:
            TJsonWriter& Writer;
            const std::vector<TKeyName>* KeyName {nullptr};

            TJsonValueWriter(TJsonWriter& writer) : Writer(writer) {}

            template <typename T>
            void operator()(const T& value) const {
                Writer.Appender.Append(*KeyName, value);
            }
    };
    TJsonValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);

    bool Write(const TStructuredMessage& message, bool started = false)
    {
        if (started) {
            Appender.Started = true;
        }
        Valid = true;

        message.ForEachSerialized([&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length){
            auto it = TypeValueWriterMap.find(typeCode);
            if (it != end(TypeValueWriterMap)) {
                ValueWriter.KeyName = &name;
                it->second(data, length);
            } else {
                Valid = false;
            }
            return Valid;
        });
        if (Valid && !started) {
            Appender.Done();
        }
        return Valid;
    }
};

}