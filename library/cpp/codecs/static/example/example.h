#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/buffer.h>

namespace NStaticCodecExample {
    enum EDictVersion : ui8 {
        DV_NULL = 0,
        DV_HUFF_20160707,
        DV_SA_HUFF_20160707,
        DV_COUNT
    };

    void Encode(TBuffer&, TStringBuf, EDictVersion dv = DV_SA_HUFF_20160707);

    void Decode(TBuffer&, TStringBuf);
}
