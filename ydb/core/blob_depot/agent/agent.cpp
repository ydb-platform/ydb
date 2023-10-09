#include "agent.h"
#include "agent_impl.h"
#include "blocks.h"
#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    TBlobDepotAgent::TBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId)
        : TRequestSender(*this)
        , VirtualGroupId(virtualGroupId)
        , ProxyId(proxyId)
        , AgentInstanceId(RandomNumber<ui64>())
        , BlocksManagerPtr(new TBlocksManager(*this))
        , BlocksManager(*BlocksManagerPtr)
        , BlobMappingCachePtr(new TBlobMappingCache(*this))
        , BlobMappingCache(*BlobMappingCachePtr)
    {
        if (info) {
            Y_ABORT_UNLESS(info->BlobDepotId);
            TabletId = *info->BlobDepotId;
            LogId = TStringBuilder() << '{' << TabletId << '@' << virtualGroupId << '}';
        } else {
            LogId = TStringBuilder() << '{' << '?' << '@' << virtualGroupId << "}";
        }
    }

    TBlobDepotAgent::~TBlobDepotAgent() {
        TRequestSender::ClearRequestsInFlight();
    }

    void TBlobDepotAgent::Bootstrap() {
        Become(&TThis::StateFunc);

        if (TabletId && TabletId != Max<ui64>()) {
            ConnectToBlobDepot();
        }

        HandleQueryWatchdog();
        HandlePendingEventQueueWatchdog();
        HandlePushMetrics();
    }

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId) {
        return new TBlobDepotAgent(virtualGroupId, std::move(info), proxyId);
    }

} // NKikimr::NBlobDepot
