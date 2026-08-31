#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NDataplane {

enum EEv : ui32 {
    EvWriteInit = InternalEventSpaceBegin(NEvents::EServices::DATAPLANE),
    EvWrite,
    EvWriteUpdateToken,
    EvWriteTokenRefreshed,
    EvWriteClientDone,
    EvWriteDieCommand,
    EvWriteInitAck,
    EvWriteAck,
    EvWriteUpdateTokenAck,
    EvWriteRefreshToken,
    EvWriteUnauthenticated,
    EvWriteClosed,
    EvWriteReadNext,
    EvWriteConsumedRequestUnits,
    EvEnd
};

static_assert(EvEnd <= InternalEventSpaceBegin(NEvents::EServices::MLP));

namespace NWrite {

struct TWriteSessionSettings;

NActors::IActor* CreateWriteSessionLogicActor(TWriteSessionSettings settings);

} // namespace NWrite

} // namespace NKikimr::NPQ::NDataplane
