#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/pdisk/subsystem/subsystem.h>
#include "blobstorage_pdisk_config.h"
#include "blobstorage_pdisk_defs.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    struct TActorSetupCmd;
}

namespace NKikimr {

class IPDiskServiceFactory : public TThrRefBase {
public:
    virtual void Create(const TActorContext &ctx, ui32 pDiskID, const TIntrusivePtr<TPDiskConfig> &cfg,
        const NPDisk::TMainKey &mainKey, ui32 poolId, ui32 nodeId) = 0;
};

std::unique_ptr<IPDiskSubsystem> CreatePDiskSubsystem();

class TRealPDiskServiceFactory : public IPDiskServiceFactory {
public:
    void Create(const TActorContext &ctx, ui32 pDiskID, const TIntrusivePtr<TPDiskConfig> &cfg,
        const NPDisk::TMainKey &mainKey, ui32 poolId, ui32 nodeId) override;
};

} // NKikimr
