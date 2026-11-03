#pragma once

#include <util/stream/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TMonPageData;

// Renders the append-only history of DDisks released from touched vchunks.
void RenderDeletedDDisks(IOutputStream& str, const TMonPageData& data);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
