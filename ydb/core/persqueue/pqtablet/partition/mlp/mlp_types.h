#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NPQ::NMLP {

struct TReadResult {
    NActors::TActorId Sender;
    ui64 Cookie;
    std::vector<ui64> Offsets;
};

} // namespace NKikimr::NPQ::NMLP
