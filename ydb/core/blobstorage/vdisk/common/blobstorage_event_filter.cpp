#include "blobstorage_event_filter.h"
#include "vdisk_events.h"

namespace NKikimr {

    void RegisterBlobStorageEventScopes(const std::shared_ptr<TEventFilter>& filter) {
        using E = ENodeClass;

        static const ui32 eventsFromPeerToSystem[] = {
            TEvBlobStorage::EvVPut,
            TEvBlobStorage::EvVGet,
            TEvBlobStorage::EvVBlock,
            TEvBlobStorage::EvVGetBlock,
            TEvBlobStorage::EvVCollectGarbage,
            TEvBlobStorage::EvVGetBarrier,
            TEvBlobStorage::EvVReadyNotify,
            TEvBlobStorage::EvVStatus,
            TEvBlobStorage::EvVCheckReadiness,
            TEvBlobStorage::EvVCompact,

            TEvBlobStorage::EvGroupStatReport,

            TEvBlobStorage::EvControllerProposeGroupKey,
            TEvBlobStorage::EvControllerRegisterNode,
            TEvBlobStorage::EvControllerGetGroup,
        };

        static const ui32 eventsFromSystemToPeer[] = {
            TEvBlobStorage::EvVPutResult,
            TEvBlobStorage::EvVGetResult,
            TEvBlobStorage::EvVBlockResult,
            TEvBlobStorage::EvVGetBlockResult,
            TEvBlobStorage::EvVCollectGarbageResult,
            TEvBlobStorage::EvVGetBarrierResult,
            TEvBlobStorage::EvVStatusResult,
            TEvBlobStorage::EvVWindowChange,
            TEvBlobStorage::EvVCheckReadinessResult,
            TEvBlobStorage::EvVCompactResult,
            TEvBlobStorage::EvControllerNodeServiceSetUpdate,
        };

        static const ui32 eventsIntraSystem[] = {
            TEvBlobStorage::EvVSync,
            TEvBlobStorage::EvVSyncFull,
            TEvBlobStorage::EvVSyncGuid,
            TEvBlobStorage::EvVSyncResult,
            TEvBlobStorage::EvVSyncFullResult,
            TEvBlobStorage::EvVSyncGuidResult,
            TEvBlobStorage::EvVBaldSyncLog,
            TEvBlobStorage::EvVBaldSyncLogResult,

            // BS_CONTROLLER
            TEvBlobStorage::EvControllerSelectGroups,
            TEvBlobStorage::EvControllerUpdateDiskStatus,
            TEvBlobStorage::EvControllerConfigRequest,
            TEvBlobStorage::EvControllerConfigResponse,
            TEvBlobStorage::EvControllerUpdateGroupStat,

            TEvBlobStorage::EvControllerSelectGroupsResult,
            TEvBlobStorage::EvRequestControllerInfo,
            TEvBlobStorage::EvResponseControllerInfo,
            TEvBlobStorage::EvControllerNodeReport,
        };

        for (ui32 event : eventsFromPeerToSystem) {
            filter->RegisterEvent(event, TEventFilter::MakeRouteMask({
                    {E::PEER_TENANT, E::SYSTEM},
                    {E::SYSTEM, E::SYSTEM}
                }));
        }

        for (ui32 event : eventsFromSystemToPeer) {
            filter->RegisterEvent(event, TEventFilter::MakeRouteMask({
                    {E::SYSTEM, E::PEER_TENANT},
                    {E::SYSTEM, E::SYSTEM}
                }));
        }

        for (ui32 event : eventsIntraSystem) {
            filter->RegisterEvent(event, TEventFilter::MakeRouteMask({
                    {E::SYSTEM, E::SYSTEM}
                }));
        }
    }

} // NKikimr
