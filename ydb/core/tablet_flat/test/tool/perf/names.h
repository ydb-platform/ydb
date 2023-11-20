#pragma once

#include "defs.h"
#include <ydb/core/tablet_flat/util_fmt_desc.h>
#include <util/system/yassert.h>

namespace NKikimr {
namespace NFmt {

    template<> struct TPut<NTable::NTest::ESponge> {
        using ESponge = NTable::NTest::ESponge;

        static void Do(TOut &out, const ESponge *sponge)
        {
            out << By(*sponge);
        }

        static const char* By(ESponge sponge)
        {
            if (sponge == ESponge::None)  return "none";
            if (sponge == ESponge::Murmur)return "murmur";
            if (sponge == ESponge::City)  return "city";
            if (sponge == ESponge::Fnv)   return "fnv";
            if (sponge == ESponge::Xor)   return "xor";

            Y_ABORT("Unreachable code");
        }
    };
}
}
