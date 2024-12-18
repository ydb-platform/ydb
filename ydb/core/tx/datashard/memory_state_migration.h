#pragma once
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NDataShard {

class TDataShard;

class IDataShardInMemoryRestoreActor {
protected:
    IDataShardInMemoryRestoreActor() = default;
    ~IDataShardInMemoryRestoreActor() = default;

public:
    /**
     * Called from tablet destructor. Detaches from tablet and waits for the
     * eventual destruction (this method should only be called during the
     * actor system shutdown).
     */
    virtual void OnTabletDestroyed() = 0;

    /**
     * Called when tablet dies. Detaches from tablet and schedules itself for
     * destruction.
     */
    virtual void OnTabletDead() = 0;
};

class IDataShardInMemoryStateActor {
protected:
    IDataShardInMemoryStateActor() = default;
    ~IDataShardInMemoryStateActor() = default;

public:
    /**
     * Confirms that this state actor address is persistent
     * This is used to notify previous actors that they are no longer needed
     */
    virtual void ConfirmPersistent() = 0;

    /**
     * Called from tablet destructor. Detaches from tablet and waits for the
     * eventual destruction (this method should only be called during the
     * actor system shutdown).
     */
    virtual void OnTabletDestroyed() = 0;

    /**
     * Called when tablet dies. Serializes current state, detaches from tablet
     * and waits to transfer this state to the next generation or timeout.
     */
    virtual void OnTabletDead() = 0;
};

} // namespace NKikimr::NDataShard
