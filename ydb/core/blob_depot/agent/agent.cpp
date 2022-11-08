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
            Y_VERIFY(info->BlobDepotId);
            TabletId = *info->BlobDepotId;
        }
    }

    TBlobDepotAgent::~TBlobDepotAgent()
    {}

    void TBlobDepotAgent::Bootstrap() {
        Become(&TThis::StateFunc);

        if (TabletId && TabletId != Max<ui64>()) {
            ConnectToBlobDepot();
        }

        HandleQueryWatchdog();
    }

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId) {
        return new TBlobDepotAgent(virtualGroupId, std::move(info), proxyId);
    }

} // NKikimr::NBlobDepot
