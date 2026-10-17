#pragma once

#include <util/stream/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TMonPageData;

// Renders memory usage for every direct block group and the disk total.
void RenderMemory(IOutputStream& str, const TMonPageData& data);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
