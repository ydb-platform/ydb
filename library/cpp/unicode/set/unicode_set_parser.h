#pragma once

#include "unicode_set.h"

#include <util/generic/strbuf.h>

namespace NUnicode {
    namespace NPrivate {
        void ParseUnicodeSet(TUnicodeSet& set, const TWtringBuf& data);
    }
}
