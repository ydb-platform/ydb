#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBlocksDirtyMap;
using TBlocksDirtyMapPtr = std::shared_ptr<TBlocksDirtyMap>;

struct ILockableRanges;
using ILockableRangesWeakPtr = std::weak_ptr<ILockableRanges>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
