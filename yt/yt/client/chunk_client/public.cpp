#include "public.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const TChunkId NullChunkId = NObjectClient::NullObjectId;
const TChunkViewId NullChunkViewId = NObjectClient::NullObjectId;
const TChunkListId NullChunkListId = NObjectClient::NullObjectId;
const TChunkTreeId NullChunkTreeId = NObjectClient::NullObjectId;

const std::string DefaultStoreAccountName("sys");
const std::string DefaultStoreMediumName("default");
const std::string DefaultCacheMediumName("cache");
const std::string DefaultSlotsMediumName("default");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
