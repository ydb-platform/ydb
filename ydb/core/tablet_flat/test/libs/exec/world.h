#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <util/system/types.h>
#include <array>

namespace NKikimr {
namespace NFake {

    using EMail = NActors::TMailboxType::EType;

    enum class EPath : ui16 {
        Root    = 0,    /* The leader actor, shuts the system   */
    };

    struct TWorld {

        static NActors::TActorId Where(EPath path) noexcept
        {
            std::array<char,13> token{ "NFakeWorld??" };

            token[10] = 0xff & ui16(path);
            token[11] = 0xff & (ui16(path) >> 8);

            return NActors::TActorId(0, TStringBuf(token.begin(), 12));
        }
    };

}
}
