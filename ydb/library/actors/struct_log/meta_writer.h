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

class TMetaWriter {
public:
    TMetaWriter() = default;

    bool Write(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message) {
        MetaFlags = &metaFlags;

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

        MetaFlags = nullptr;
        return true;
    }

protected:
    TLogRecord::TMetaFlags* MetaFlags{nullptr};

    struct TValueWriter {
        TMetaWriter& Writer;
        const std::vector<TKeyName>* KeyName {nullptr};

        TValueWriter(TMetaWriter& writer) : Writer(writer) {}

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
    TValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);
};


}
