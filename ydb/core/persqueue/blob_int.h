#pragma once

#include <util/system/types.h>

namespace NKikimr::NPQ {

struct TClientBlob;

union TMessageFlags {
    using TValue = ui8;

    explicit TMessageFlags(TValue v = 0)
        : V(v) {
    }

    TValue V;
    struct {
        TValue HasPartData : 1;
        TValue HasWriteTimestamp : 1;
        TValue HasCreateTimestamp : 1;
        TValue HasUncompressedSize : 1;
        TValue HasKinesisData : 1;
        TValue Reserve: 3;
    } F;

    static_assert(sizeof(V) == sizeof(F));
};

static_assert(sizeof(TMessageFlags) == sizeof(TMessageFlags::TValue));

}
