#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "structured_message.h"

#include <vector>

namespace NActors::NStructuredLog {

template <typename TWriter>
class TBaseValueWriter {
public:
    const std::vector<TKeyName>* KeyName{nullptr};

    TBaseValueWriter(TWriter& writer)
        : Writer(writer)
    {}

protected:
    TWriter& Writer;
};

template <typename TWriter>
class TBaseMessageWriter {
public:
    TBaseMessageWriter(TWriter& writer)
        : ValueWriter(writer)
    {}

    bool WriteMessage(const TStructuredMessage& message) {
        auto processValue =
            [&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length) {
                auto it = TypeValueWriterMap.find(typeCode);
                if (it != end(TypeValueWriterMap)) {
                    ValueWriter.KeyName = &name;
                    return it->second(data, length);
                } else {
                    return false;
                }
            };
        return message.ForEachSerialized(processValue);
    }

protected:
    TWriter::TValueWriter ValueWriter{*this};
    TInvokerMap TypeValueWriterMap = TTypesMapping::CreateInvokerMap(ValueWriter);
};

}
