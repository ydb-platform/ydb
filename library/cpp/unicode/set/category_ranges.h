#pragma once

#include <util/charset/unidata.h>
#include <util/system/defaults.h>
#include <util/generic/strbuf.h>

namespace NUnicode {
    namespace NPrivate {
        struct TCategoryRanges {
            size_t Count;
            const wchar32* Data;
        };

        const TCategoryRanges& GetCategoryRanges(WC_TYPE cat);
        const TCategoryRanges& GetCategoryRanges(const TStringBuf& category);

    }
}
