#pragma once

#include <ydb/core/protos/auth.pb.h>
#include <ydb/library/actors/core/actorid.h>

#include <optional>

namespace NKikimr {

struct TTokenManagerSettings {
    NKikimrProto::TTokenManager Config;
    std::optional<NActors::TActorId> HttpProxyId;
};

} // NKikimr
