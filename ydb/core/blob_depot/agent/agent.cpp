#include "agent.h"
#include "agent_impl.h"
#include "blocks.h"
#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    TBlobDepotAgent::TBlobDepotAgent(ui32 virtualGroupId)
        : TActor(&TThis::StateFunc)
        , TRequestSender(*this)
        , VirtualGroupId(virtualGroupId)
        , AgentInstanceId(RandomNumber<ui64>())
        , BlocksManagerPtr(new TBlocksManager(*this))
        , BlocksManager(*BlocksManagerPtr)
        , BlobMappingCachePtr(new TBlobMappingCache(*this))
        , BlobMappingCache(*BlobMappingCachePtr)
    {
        Y_VERIFY(TGroupID(VirtualGroupId).ConfigurationType() == EGroupConfigurationType::Virtual);
    }

    TBlobDepotAgent::~TBlobDepotAgent()
    {}

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId) {
        return new TBlobDepotAgent(virtualGroupId);
    }

} // NKikimr::NBlobDepot
