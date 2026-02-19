#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/public.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NYdb::NBS {

namespace NProto {
class TPartitionConfig;
class TStorageServiceConfig;
}   // namespace NProto

namespace NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStorageConfig;
using TStorageConfigPtr = std::shared_ptr<TStorageConfig>;

struct IWriteBlocksHandler;
using IWriteBlocksHandlerPtr = std::shared_ptr<IWriteBlocksHandler>;

struct IReadBlocksHandler;
using IReadBlocksHandlerPtr = std::shared_ptr<IReadBlocksHandler>;

struct ICompactionPolicy;
using ICompactionPolicyPtr = std::shared_ptr<ICompactionPolicy>;

struct TManuallyPreemptedVolumes;
using TManuallyPreemptedVolumesPtr = std::shared_ptr<TManuallyPreemptedVolumes>;

////////////////////////////////////////////////////////////////////////////////

enum class EStorageAccessMode
{
    Default,
    Repair,
};

}   // namespace NStorage
}   // namespace NYdb::NBS
