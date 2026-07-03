#pragma once

#include "base_writer.h"

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NActors::NStructuredLog {

class TTextWriter {
    friend class TBaseMessageWriter<TTextWriter>;

public:
    TTextWriter() = default;

    bool Write(TStringBuilder& outputText, const TStructuredMessage& message);

protected:
    TStringBuilder* OutputText{nullptr};
    bool FirstValue{true};

    struct TValueWriter : public TBaseValueWriter<TTextWriter> {
        TValueWriter(TTextWriter& writer);

        void operator()(const TString& value) const;
    };

    TBaseMessageWriter<TTextWriter> MessageWriter{*this};
};

}  // namespace NActors::NStructuredLog
