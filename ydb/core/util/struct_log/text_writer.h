#pragma once

#if 0

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <library/cpp/json/writer/json.h>
#include <library/cpp/logger/record.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <unordered_set>
#include <vector>

namespace NKikimr::NStructLog {

class TTextWriter
{
public:
    TTextWriter() {}

    bool Write(TStringBuilder& outputText, const TStructuredMessage& message)
    {
        MetaFlags = &metaFlags;

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

        MetaFlags = nullptr;
        return true;
    }

protected:
    TStringBuilder* OutputText{nullptr};

    class TJsonValueWriter {
        public:
            TMetaWriter& Writer;
            const std::vector<TKeyName>* KeyName {nullptr};

            TJsonValueWriter(TMetaWriter& writer) : Writer(writer) {}

            template <typename T>
            void operator()(const T& value) const {
                TStringBuilder metakeyName;
                metakeyName << "meta";
                for(auto& keyItem: *KeyName) {
                    metakeyName << ".";
                    metakeyName << keyItem.ToString();
                }
                Writer.MetaFlags->push_back({metakeyName, TTypesMapping::ToString(value)});
            }
    };
    TJsonValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);
};


}
#endif