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

protected:
    TLogRecord::TMetaFlags* MetaFlags{nullptr};

    struct TValueWriter : public TBaseValueWriter<TMetaWriter> {
        TValueWriter(TMetaWriter& writer);

        void operator()(const TString& value) const;
    };

    TBaseMessageWriter<TMetaWriter> MessageWriter{*this};
};

}  // namespace NActors::NStructuredLog
