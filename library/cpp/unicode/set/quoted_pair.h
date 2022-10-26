#pragma once

#include "unicode_set.h"

#include <util/system/defaults.h>

namespace NUnicode {
    enum EUnicodeQuotedPairType {
        UQPT_SYMBOL,
        UQPT_SET,
    };

    EUnicodeQuotedPairType ResolveUnicodeQuotedPair(wchar32 escapedSymbol, wchar32& symbol, TUnicodeSet& set);

}
