#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimrProto {

class TServiceTicketManager;

} // NKikimrProto


namespace NKikimr {

inline NActors::TActorId MakeServiceTicketManagerID() {
    static const char name[12] = "srvticktmgr";
    return NActors::TActorId(0, TStringBuf(name, 12));
}

NActors::IActor* CreateServiceTicketManager(const NKikimrProto::TServiceTicketManager& config);

} // NKikimr
