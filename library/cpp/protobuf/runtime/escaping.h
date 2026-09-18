#pragma once

// Arcadia's protobuf keeps these helpers in google/protobuf/stubs/strutil.h.
// Upstream dropped that header and moved them to Abseil, which the two
// runtimes vendor under different namespaces, so route them through here.

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NProtoBufRuntime {
    TString CEscape(TStringBuf src);
    TString Utf8SafeCEscape(TStringBuf src);
    TString UnescapeCEscapeString(TStringBuf src);
} // namespace NProtoBufRuntime
