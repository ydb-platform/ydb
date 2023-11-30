#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/base/defs.h
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/debug/valgrind_check.h>
#include <util/generic/array_ref.h>
#include <util/generic/string.h>
#include <util/system/byteorder.h>

namespace NKikimr {
    // actorlib is organic part of kikimr so we emulate global import by this directive
    using namespace NActors;

    struct TPtrHash {
        template<typename TSmartPtr>
        size_t operator()(const TSmartPtr &x) const {
            return THash<intptr_t>()(reinterpret_cast<intptr_t>(&*x));
        }
    };

    struct TPtrEqual {
        template<typename TPtrLeft, typename TPtrRight>
        bool operator()(const TPtrLeft &left, const TPtrRight &right) const {
            return (&*left == &*right);
        }
    };

    struct TPtrLess {
        template<typename TPtrLeft, typename TPtrRight>
        bool operator()(const TPtrLeft &left, const TPtrRight &right) const {
            return (&*left < &*right);
        }
    };

    inline ui64 Hash64to32(ui64 x) noexcept {
        const ui64 x1 = 0x001DFF3D8DC48F5Dull * (x >> 32ull);
        const ui64 x2 = 0x179CA10C9242235Dull * (x & 0x00000000FFFFFFFFull);

        const ui64 sum = 0x0F530CAD458B0FB1ull + x1 + x2;

        return (sum >> 32);
    }

    inline ui64 Hash128to32(ui64 a, ui64 b) noexcept {
        const ui64 x1 = 0x001DFF3D8DC48F5Dull * (a & 0xFFFFFFFFull);
        const ui64 x2 = 0x179CA10C9242235Dull * (a >> 32);
        const ui64 x3 = 0x0F530CAD458B0FB1ull * (b & 0xFFFFFFFFull);
        const ui64 x4 = 0xB5026F5AA96619E9ull * (b >> 32);

        const ui64 sum = 0x06C9C021156EAA1Full + x1 + x2 + x3 + x4;

        return (sum >> 32);
    }
}
