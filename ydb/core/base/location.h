#pragma once
#include "defs.h"

#include <util/digest/murmur.h>
#include <util/generic/string.h>

namespace NKikimr {
    inline ui32 DataCenterFromString(const TString &dc)
    {
        ui32 res = 0;
        strncpy(reinterpret_cast<char *>(&res), dc.data(), sizeof(res));
        return res;
    }

    inline TString DataCenterToString(ui32 dc)
    {
        char str[sizeof(dc) + 1];
        str[sizeof(dc)] = 0;
        strncpy(str, reinterpret_cast<char *>(&dc), sizeof(dc));
        return str;
    }

    inline ui32 RackFromString(const TString &rack)
    {
        return MurmurHash<ui32>(rack.data(), rack.size());
    }
} // namespace NKikimr
