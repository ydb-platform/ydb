#pragma once

#include <util/stream/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TMonPageData;

// Renders node-failure controls for every direct block group.
void RenderChaos(IOutputStream& str, const TMonPageData& data);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
