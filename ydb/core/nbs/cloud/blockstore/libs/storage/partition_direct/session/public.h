#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

class TPartitionSession;
using TPartitionSessionPtr = std::shared_ptr<TPartitionSession>;

struct IPartitionSessionControl;
using IPartitionSessionControlPtr = std::shared_ptr<IPartitionSessionControl>;

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
