#pragma once

#include <util/generic/strbuf.h>

namespace NYql {
    constexpr TStringBuf SOLOMON_SCHEME_KIND = "kind"sv;
    constexpr TStringBuf SOLOMON_SCHEME_LABELS = "labels"sv;
    constexpr TStringBuf SOLOMON_SCHEME_VALUE = "value"sv;
    constexpr TStringBuf SOLOMON_SCHEME_TYPE = "type"sv;
    constexpr TStringBuf SOLOMON_SCHEME_TS = "ts"sv;
}
