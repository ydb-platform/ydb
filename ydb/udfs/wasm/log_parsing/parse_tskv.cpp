#include "parse_tskv.h"

namespace NLogParsing {

TTskvParseResult ParseTskv(TStringBuf raw) {
    TTskvParseResult result;
    TStringBuf body = raw;
    if (body.StartsWith("tskv\t")) {
        body.Skip(5);
    }

    // Same as StringSplitter(...).Split('\t').SkipEmpty(): avoid util/string
    // split sentinel (see line_break.cpp).
    TStringBuf token;
    while (body.NextTok('\t', token)) {
        if (token.empty()) {
            continue;
        }
        const size_t eq = token.find('=');
        if (eq == TStringBuf::npos) {
            continue;
        }
        const TStringBuf key = token.Head(eq);
        const TStringBuf value = token.Tail(eq + 1);
        if (key.empty()) {
            continue;
        }
        result.Fields[TString(key)] = TString(value);
    }

    if (result.Fields.empty()) {
        result.Successed = false;
        result.Error = "cannot parse tskv";
        return result;
    }

    result.Successed = true;
    return result;
}

} // namespace NLogParsing
