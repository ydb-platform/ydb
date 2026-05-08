#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <library/cpp/logger/record.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <unordered_set>
#include <vector>

namespace NActors::NStructuredLog {

class TTextWriter {
public:
    TTextWriter() = default;

    bool Write(TStringBuilder& outputText, const TStructuredMessage& message);

protected:
    TStringBuilder* OutputText{nullptr};
    bool FirstValue{true};

    struct TValueWriter {
        TTextWriter& Writer;
        const std::vector<TKeyName>* KeyName{nullptr};

        TValueWriter(TTextWriter& writer);

        template <typename T>
        void operator()(const T& value) const {
            auto& outputText = *Writer.OutputText;
            if (Writer.FirstValue) {
                Writer.FirstValue = false;
            } else {
                outputText << " ";
            }

            bool first = true;

            for (auto& keyItem : *KeyName) {
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

}  // namespace NActors::NStructuredLog
