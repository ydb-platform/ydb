#pragma once

#include "mon_model.h"

#include <util/stream/output.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

//////////////////////////////////////////////////////////////////////////////////

void RenderDbg(IOutputStream& str, const TMonPageData& data);

//////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
