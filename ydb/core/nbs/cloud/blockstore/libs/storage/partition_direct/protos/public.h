#pragma once

#include <util/generic/map.h>
#include <util/system/types.h>

namespace NYdb::NBS::PartitionDirect::NProto {

////////////////////////////////////////////////////////////////////////////////

class TBlockField;
class TDDiskState;
class TDirtyMapState;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::PartitionDirect::NProto

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

using TBlockFieldProto = NYdb::NBS::PartitionDirect::NProto::TBlockField;
using TDDiskStateProto = NYdb::NBS::PartitionDirect::NProto::TDDiskState;
using TDirtyMapStateProto = NYdb::NBS::PartitionDirect::NProto::TDirtyMapState;

using TDirtyMapStateProtos = TMap<ui32, TDirtyMapStateProto>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
