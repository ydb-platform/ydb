#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <library/cpp/logger/record.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <unordered_set>
#include <vector>

namespace NKikimr::NStructuredLog {

class TTextWriter {
public:
    TTextWriter() = default;

    bool Write(TStringBuilder& outputText, const TStructuredMessage& message) {
        OutputText = &outputText;
        FirstValue = true;

        auto processValue = [&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length) {
            auto it = TypeValueWriterMap.find(typeCode);
            if (it != end(TypeValueWriterMap)) {
                ValueWriter.KeyName = &name;
                return it->second(data, length);
            } else {
                return false;
            }
        };
        if (!message.ForEachSerialized(processValue)) {
            return false;
        };

        OutputText = nullptr;
        return true;
    }

protected:
    TStringBuilder* OutputText{nullptr};
    bool FirstValue{true};

    struct TValueWriter {
        TTextWriter& Writer;
        const std::vector<TKeyName>* KeyName{nullptr};

        TValueWriter(TTextWriter& writer) : Writer(writer) {}

        template <typename T>
        void operator()(const T& value) const {
            auto& outputText = *Writer.OutputText;
            if (Writer.FirstValue) {
                Writer.FirstValue = false;
            } else {
                outputText << " ";
            }

            bool first = true;

            for(auto& keyItem: *KeyName) {
                if (first) {
                    first = false;
                } else {
                    outputText << ".";
                }
                outputText << keyItem.ToString();
            }
            outputText << "=";
            outputText << TTypesMapping::ToString(value);
        }
    };
    TValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);
};


}
