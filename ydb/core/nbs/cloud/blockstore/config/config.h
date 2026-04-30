#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class EWriteMode: ui32
{
    PBufferReplication,
    DirectPBuffersFilling,
};

EWriteMode GetWriteModeFromProto(NProto::EWriteMode writeMode);
NProto::EWriteMode GetProtoWriteMode(EWriteMode writeMode);

class TStorageConfig
{
public:
    explicit TStorageConfig(
        const NProto::TStorageServiceConfig& storageServiceConfig);

    [[nodiscard]] TDuration GetTraceSamplePeriod() const;
    [[nodiscard]] ui32 GetSyncRequestsBatchSize() const;
    [[nodiscard]] ui64 GetStripeSize() const;
    [[nodiscard]] TDuration GetWriteHedgingDelay() const;
    [[nodiscard]] TDuration GetWriteRequestTimeout() const;
    [[nodiscard]] TString GetDDiskPoolName() const;
    [[nodiscard]] TString GetPersistentBufferDDiskPoolName() const;
    [[nodiscard]] NProto::EWriteMode GetWriteMode() const;
    [[nodiscard]] TDuration GetPBufferReplyTimeout() const;
    [[nodiscard]] ui64 GetVChunkSize() const;
    [[nodiscard]] ui32 GetThreadPoolSize() const;

private:
    NProto::TStorageServiceConfig StorageServiceConfig;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
