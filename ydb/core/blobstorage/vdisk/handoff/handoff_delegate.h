#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/base/utility.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Forward declarations
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class THandoffMap;

    template <class TKey, class TMemRec>
    class TLevelIndexSnapshot;

    ////////////////////////////////////////////////////////////////////////////
    // THandoffParams
    ////////////////////////////////////////////////////////////////////////////
    struct THandoffParams {
        TString VDiskLogPrefix;
        ui64 MaxWaitQueueSize;
        ui64 MaxWaitQueueByteSize;
        ui64 MaxInFlightSize;
        ui64 MaxInFlightByteSize;
        TDuration Timeout;
    };

    ////////////////////////////////////////////////////////////////////////////
    // Handoff manager delegate
    ////////////////////////////////////////////////////////////////////////////
    class THandoffDelegate : public TThrRefBase {
    public:
        THandoffDelegate(const TVDiskIdShort &self,
                         TIntrusivePtr<TBlobStorageGroupInfo> info,
                         const THandoffParams &params);
        TActiveActors RunProxies(const TActorContext &ctx);
        bool Restore(const TActorContext &ctx,
                     const TVDiskIdShort &vdisk,
                     const TLogoBlobID &id,
                     ui64 fullDataSize,
                     TRope&& data);
        TActorId GetMonActorID() const;

    private:
        struct TFields;
        std::unique_ptr<TFields> Fields;
    };

} // NKikimr
