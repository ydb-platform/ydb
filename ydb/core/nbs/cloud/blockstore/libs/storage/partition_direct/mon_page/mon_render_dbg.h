#pragma once

#include "mon_model.h"

#include <util/stream/output.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

//////////////////////////////////////////////////////////////////////////////////

// Positional range of VChunks displayed on one DBG detail page.
struct TVChunkPageRange
{
    size_t From = 0;
    size_t Count = 0;
};

// Returns the positional VChunk range displayed on a DBG detail page.
TVChunkPageRange GetVChunkPageRange(size_t page);

void RenderDbg(IOutputStream& str, const TMonPageData& data);

//////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
