#include "event_field_output.h"

#include <util/string/split.h>

namespace {
    TString MakeSeparators(EFieldOutputFlags flags) {
        TString res;
        res.reserve(3);

        if (flags & EFieldOutputFlag::EscapeTab) {
            res.append('\t');
        }
        if (flags & EFieldOutputFlag::EscapeNewLine) {
            res.append('\n');
            res.append('\r');
        }
        if (flags & EFieldOutputFlag::EscapeBackSlash) {
            res.append('\\');
        }

        return res;
    }
}

TEventFieldOutput::TEventFieldOutput(IOutputStream& output, EFieldOutputFlags flags)
    : Output(output)
    , Flags(flags)
    , Separators(MakeSeparators(flags))
{
}

IOutputStream& TEventFieldOutput::GetOutputStream() {
    return Output;
}

EFieldOutputFlags TEventFieldOutput::GetFlags() const {
    return Flags;
}

void TEventFieldOutput::DoWrite(const void* buf, size_t len) {
    if (!Flags) {
        Output.Write(buf, len);
        return;
    }

    TStringBuf chunk{static_cast<const char*>(buf), len};

    for (const auto part : StringSplitter(chunk).SplitBySet(Separators.data())) {
        TStringBuf token = part.Token();
        TStringBuf delim = part.Delim();

        if (!token.empty()) {
            Output.Write(token);
        }
        if ("\n" == delim) {
            Output.Write(TStringBuf("\\n"));
        } else if ("\r" == delim) {
            Output.Write(TStringBuf("\\r"));
        } else if ("\t" == delim) {
            Output.Write(TStringBuf("\\t"));
        } else if ("\\" == delim) {
            Output.Write(TStringBuf("\\\\"));
        } else {
            Y_ASSERT(delim.empty());
        }
    }
}

