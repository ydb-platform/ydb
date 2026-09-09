#pragma once
#include "ddisk_actor.h"

namespace NKikimr::NDDisk {
class TDDiskActorTestPeer {
public:
    static bool IsBroken(const TDDiskActor& actor) { return actor.IsBroken(); }
    static void SetDestructionClock(TDDiskActor& actor,
            std::function<TMonotonic()> now, std::function<void()> sleep) {
        actor.DestructionNow = std::move(now);
        actor.DestructionSleep = std::move(sleep);
    }
};
}
