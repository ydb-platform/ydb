#pragma once

#include "mon_model.h"

#include <util/stream/output.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Renders the state persisted in the partition tablet's Local DB.
void RenderLocalDb(IOutputStream& str, const TLocalDbContents& db);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
