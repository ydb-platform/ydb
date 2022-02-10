#include "statestorage.h"

namespace NKikimr {

void RegisterStateStorageEventScopes(const std::shared_ptr<TEventFilter>& filter) {
    static const ui32 eventsFromPeerToSystem[] = {
        TEvStateStorage::EvReplicaLookup,
        TEvStateStorage::EvReplicaUpdate,
        TEvStateStorage::EvReplicaLock,
        TEvStateStorage::EvReplicaDumpRequest,
        TEvStateStorage::EvReplicaRegFollower,
        TEvStateStorage::EvReplicaUnregFollower,
        TEvStateStorage::EvReplicaDelete,
        TEvStateStorage::EvReplicaCleanup,

        TEvStateStorage::EvReplicaBoardPublish,
        TEvStateStorage::EvReplicaBoardLookup,
        TEvStateStorage::EvReplicaBoardCleanup,
    };

    static const ui32 eventsFromSystemToPeer[] = {
        TEvStateStorage::EvReplicaLeaderDemoted,
        TEvStateStorage::EvReplicaDump,
        TEvStateStorage::EvReplicaInfo,

        TEvStateStorage::EvReplicaBoardPublishAck,
        TEvStateStorage::EvReplicaBoardInfo,
    };

    for (ui32 e : eventsFromPeerToSystem) {
        filter->RegisterEvent(e, TEventFilter::MakeRouteMask({
                {ENodeClass::PEER_TENANT, ENodeClass::SYSTEM},
                {ENodeClass::SYSTEM, ENodeClass::SYSTEM}
            }));
    }

    for (ui32 e : eventsFromSystemToPeer) {
        filter->RegisterEvent(e, TEventFilter::MakeRouteMask({
                {ENodeClass::SYSTEM, ENodeClass::PEER_TENANT},
                {ENodeClass::SYSTEM, ENodeClass::SYSTEM}
            }));
    }
}

}
