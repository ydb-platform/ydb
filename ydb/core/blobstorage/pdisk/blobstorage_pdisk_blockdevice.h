#pragma once
#include "defs.h"

#include "blobstorage_pdisk_completion.h"
#include "blobstorage_pdisk_request_id.h"
#include "blobstorage_pdisk_util_devicemode.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/drivedata.h>
#include <ydb/library/pdisk_io/sector_map.h>

#include <util/system/file.h>

namespace NKikimr {

struct TPDiskMon;

namespace NPDisk {

struct TPDiskCtx;

////////////////////////////////////////////////////////////////////////////
// IBlockDevice - PDisk Hardware abstraction layer
////////////////////////////////////////////////////////////////////////////

class IBlockDevice {
public:
    virtual ~IBlockDevice()
    {};
    // Initialization methods
    virtual void Initialize(std::shared_ptr<TPDiskCtx> pdiskCtx) = 0;
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

    // Returns a duplicated file descriptor for the underlying block device.
    // The caller owns the returned TFileHandle and is responsible for closing it.
    // Returns an invalid (not open) TFileHandle if the device does not support fd duplication (e.g. SectorMap).
    virtual TFileHandle DuplicateFd() = 0;
};

class TPDisk;

IBlockDevice* CreateRealBlockDevice(const TString &path, TPDiskMon &mon,
        ui64 reorderingCycles, ui64 seekCostNs, ui64 deviceInFlight, TDeviceMode::TFlags flags,
        ui32 maxQueuedCompletionActions, ui32 completionThreadsCount, TIntrusivePtr<TSectorMap> sectorMap,
        ui64 pDiskBufferSize = 512ull << 10,
        TPDisk * const pdisk = nullptr, bool readOnly = false, bool useBytesFlightControl = false);
IBlockDevice* CreateRealBlockDeviceWithDefaults(const TString &path, TPDiskMon &mon, TDeviceMode::TFlags flags,
        TIntrusivePtr<TSectorMap> sectorMap, TActorSystem *actorSystem, TPDisk * const pdisk = nullptr,
        bool readOnly = false, bool useBytesFlightControl = false);

} // NPDisk
} // NKikimr
