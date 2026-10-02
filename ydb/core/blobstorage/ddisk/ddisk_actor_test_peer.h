#pragma once
#include "ddisk_actor.h"

namespace NKikimr::NDDisk {
class TDDiskActorTestPeer {
public:
    static bool IsShutdownDrained(const TDDiskActor& actor) {
        return actor.OwnDrainComplete && actor.PersistentBufferGone;
    }
    static bool IsBroken(const TDDiskActor& actor) { return actor.IsBroken(); }
    // Called in the actor's mailbox, after setup writes have completed.
    static bool ReservationsSettled(const TDDiskActor& actor) {
        return actor.LogReplayComplete && !actor.ReserveInFlight
            && actor.FormattingChunks.empty() && actor.ChunkAllocateQueue.empty()
            && actor.DataChunkAllocationsInFlight.empty()
            && !actor.IssuePersistentBufferChunkAllocationInflight
            && actor.PersistentBufferChunks.size() >= actor.PersistentBufferFormat.InitChunks;
    }
    static void SetDestructionClock(TDDiskActor& actor,
            std::function<TMonotonic()> now, std::function<void()> sleep) {
        actor.DestructionNow = std::move(now);
        actor.DestructionSleep = std::move(sleep);
    }
};
}
