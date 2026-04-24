#pragma once

#include "schema.h"

#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NSchema {

struct TDropTopicOperationSettings {
    TString Database;
    TString PeerName;
    TString Path;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IfExists = false;
    ui64 Cookie = 0;
};

NActors::IActor* CreateDropTopicOperationActor(NActors::TActorId parentId, TDropTopicOperationSettings&& settings);

} // namespace NKikimr::NPQ::NSchema
