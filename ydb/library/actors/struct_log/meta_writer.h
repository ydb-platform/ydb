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

class TMetaWriter {
public:
    TMetaWriter() = default;

    bool Write(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message);

protected:
    TLogRecord::TMetaFlags* MetaFlags{nullptr};

    struct TValueWriter {
        TMetaWriter& Writer;
        const std::vector<TKeyName>* KeyName{nullptr};

        TValueWriter(TMetaWriter& writer);

        template <typename T>
        void operator()(const T& value) const {
            TStringBuilder metakeyName;
            metakeyName << "meta";
            for (auto& keyItem : *KeyName) {
                metakeyName << ".";
                metakeyName << keyItem.ToString();
            }
            Writer.MetaFlags->push_back({metakeyName, TTypesMapping::ToString(value)});
        }
    };
    TValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);
};

}  // namespace NActors::NStructuredLog
