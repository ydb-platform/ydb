#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EPersistResult
{
    Success,
    Cancelled,
};
using TPersistResultFuture = NThreading::TFuture<EPersistResult>;
using TPersistResultPromise = NThreading::TPromise<EPersistResult>;

////////////////////////////////////////////////////////////////////////////////

class IOracle;
using IOraclePtr = IOracle*;

class TOracleConfig;
using TOracleConfigPtr = std::shared_ptr<TOracleConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
