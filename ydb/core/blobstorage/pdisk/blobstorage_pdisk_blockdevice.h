#pragma once
#include "defs.h"

#include "blobstorage_pdisk_completion.h"
#include "blobstorage_pdisk_request_id.h"
#include "blobstorage_pdisk_util_devicemode.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/drivedata.h>
#include <ydb/library/pdisk_io/sector_map.h>

namespace NActors {
class TActorSystem;
}

namespace NKikimr {

struct TPDiskMon;

namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// IBlockDevice - PDisk Hardware abstraction layer
////////////////////////////////////////////////////////////////////////////

class IBlockDevice {
public:
    virtual ~IBlockDevice()
    {};
    // Initialization methods
    virtual void Initialize(TActorSystem *actorSystem, const TActorId &pdiskActor) = 0;
    virtual bool IsGood() = 0;
    virtual int GetLastErrno() = 0;

    // Synchronous intefrace
    virtual void PwriteSync(const void *data, ui64 size, ui64 offset, TReqId reqId, NWilson::TTraceId *traceId) = 0;
    virtual void PreadSync(void *data, ui32 size, ui64 offset, TReqId reqId, NWilson::TTraceId *traceId) = 0;
    virtual void TrimSync(ui32 size, ui64 offset) = 0;

    // Asynchronous intefrace
    virtual void PwriteAsync(const void *data, ui64 size, ui64 offset, TCompletionAction *completionAction,
            TReqId reqId, NWilson::TTraceId *traceId) = 0;
    virtual void PreadAsync(void *data, ui32 size, ui64 offset, TCompletionAction *completionAction,
            TReqId reqId, NWilson::TTraceId *traceId) = 0;
    virtual void CachedPreadAsync(void *data, ui32 size, ui64 offset, TCompletionAction *completionAction,
            TReqId reqId, NWilson::TTraceId *traceId) = 0;
    virtual void ClearCache() = 0;
    virtual void EraseCacheRange(ui64 begin, ui64 end) = 0; // erases offsets range [begin, end)
    virtual void FlushAsync(TCompletionAction *completionAction, TReqId reqId) = 0;
    virtual void NoopAsync(TCompletionAction *completionAction, TReqId reqId) = 0;
    virtual void NoopAsyncHackForLogReader(TCompletionAction *completionAction, TReqId reqId) = 0;
    virtual void TrimAsync(ui32 size, ui64 offset, TCompletionAction *completionAction, TReqId reqId) = 0;

    // Control methods
    virtual bool GetIsTrimEnabled() = 0;
    virtual TDriveData GetDriveData() = 0;
    virtual ui32 GetPDiskId() = 0;
    virtual void SetWriteCache(bool isEnable) = 0;
    virtual void Stop() = 0;
    virtual TString DebugInfo() = 0;
};

class TPDisk;

IBlockDevice* CreateRealBlockDevice(const TString &path, ui32 pDiskId, TPDiskMon &mon,
        ui64 reorderingCycles, ui64 seekCostNs, ui64 deviceInFlight, TDeviceMode::TFlags flags,
        ui32 maxQueuedCompletionActions, ui32 completionThreadsCount, TIntrusivePtr<TSectorMap> sectorMap, TPDisk * const pdisk = nullptr, bool readOnly = false);
IBlockDevice* CreateRealBlockDeviceWithDefaults(const TString &path, TPDiskMon &mon, TDeviceMode::TFlags flags,
        TIntrusivePtr<TSectorMap> sectorMap, TActorSystem *actorSystem, TPDisk * const pdisk = nullptr, bool readOnly = false);

} // NPDisk
} // NKikimr
