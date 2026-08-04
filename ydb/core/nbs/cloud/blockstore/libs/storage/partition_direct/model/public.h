#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class IOracle;
using IOraclePtr = IOracle*;

class TOracleConfig;
using TOracleConfigPtr = std::shared_ptr<TOracleConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
