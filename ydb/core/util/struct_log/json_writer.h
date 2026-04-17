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

    TJsonAppender() {
    }

    template <typename T>
    void AppendValue(const T& value) {
        Y_UNUSED(value);
        static_assert(false, "Unsupported type");
    }

    template <> void AppendValue(const i8& value) { Json.WriteLongLong(value); }
    template <> void AppendValue(const ui8& value) { Json.WriteULongLong(value); }
    template <> void AppendValue(const i16& value) { Json.WriteLongLong(value); }
    template <> void AppendValue(const ui16& value) { Json.WriteULongLong(value); }
    template <> void AppendValue(const i32& value) { Json.WriteLongLong(value); }
    template <> void AppendValue(const ui32& value) { Json.WriteULongLong(value); }
    template <> void AppendValue(const i64& value) { Json.WriteLongLong(value); }
    template <> void AppendValue(const ui64& value) { Json.WriteULongLong(value); }
    template <> void AppendValue(const bool& value) { Json.WriteBool(value); }
    template <> void AppendValue(const TString& value) { Json.WriteString(value); }

    template <typename T>
    bool Append(const std::vector<TKeyName>& key, const T& value) {
        if (!Started) {
            Json.BeginObject();
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
                Json.EndObject();
                currentContext.pop_back();
            }

            // Open nested values
            while (index < requiredContext.size()) {
                Json.WriteKey(requiredContext[index].ToString());
                Json.BeginObject();
                index ++;
            }
        }

        // Add key-value
        Json.WriteKey(key.back().ToString());
        AppendValue(value);

        LastAppendedKey = key;
        return true;
    }

    void Done() {
        if (!Started) {
            Json.BeginObject();
            Json.EndObject();
        }
        else
        {
            for(std::size_t i = 0; i < LastAppendedKey.size(); i++) {
                Json.EndObject();
            }
        }
    }

    const TString& GetJson() const{
        return Json.Str();
    }

protected:
    NJsonWriter::TBuf Json;
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

    bool Write(const TStructuredMessage& message)
    {
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
        if (Valid) {
            Appender.Done();
        }
        return Valid;
    }

    const TString& GetJson() const{
        return Appender.GetJson();
    }
};

}