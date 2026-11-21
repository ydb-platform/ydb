#pragma once

#include "base_writer.h"

#include <library/cpp/logger/record.h>

#include <util/generic/string.h>

namespace NActors::NStructuredLog {

class TMetaWriter {
    friend class TBaseMessageWriter<TMetaWriter>;

public:
    TMetaWriter() = default;

    bool Write(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message);

    // Opt-in transport for the structured_v1 producer protocol.
    void WriteCanonical(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message);

protected:
    static TString MetaKey(TStringBuf prefix, const std::vector<TKeyName>& name);

    TLogRecord::TMetaFlags* MetaFlags{nullptr};

    struct TValueWriter : public TBaseValueWriter<TMetaWriter> {
        TValueWriter(TMetaWriter& writer);

        void operator()(const auto& value) const {
            Writer.MetaFlags->push_back({MetaKey("meta", *KeyName), TTypesMapping::ToString(value)});
        }

    };

    TBaseMessageWriter<TMetaWriter> MessageWriter{*this};
};

}  // namespace NActors::NStructuredLog
