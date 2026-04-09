#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStorageConfig
{
public:
    explicit TStorageConfig(NProto::TStorageServiceConfig storageServiceConfig);

    [[nodiscard]] TDuration GetTraceSamplePeriod() const;
    [[nodiscard]] ui32 GetSyncRequestsBatchSize() const;
    [[nodiscard]] ui64 GetStripeSize() const;
    [[nodiscard]] TDuration GetWriteHandoffDelay() const;
    [[nodiscard]] TString GetDDiskPoolName() const;
    [[nodiscard]] TString GetPersistentBufferDDiskPoolName() const;
    [[nodiscard]] NProto::EWriteMode GetWriteMode() const;
    [[nodiscard]] TDuration GetPBufferReplyTimeout() const;

private:
    NProto::TStorageServiceConfig StorageServiceConfig;
};

}   // namespace NYdb::NBS::NStorage
