#pragma once

#include <util/system/types.h>

namespace NKikimr::NPQ {

union TMessageFlags {
    using TValue = ui8;

    TValue V = 0;
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
