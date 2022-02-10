#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Messages
    ////////////////////////////////////////////////////////////////////////////
    struct TEvLocalHandoff :
        public TEventLocal<TEvLocalHandoff, TEvBlobStorage::EvLocalHandoff>,
        public TIntrusiveListItem<TEvLocalHandoff>
    {
        const TLogoBlobID Id;
        const ui64 FullDataSize;
        const TString Data;
        ui64 Cookie;

        TEvLocalHandoff(const TLogoBlobID &id, ui64 fullDataSize, const TString &data)
            : Id(id)
            , FullDataSize(fullDataSize)
            , Data(data)
            , Cookie(0)
        {}

        ui32 ByteSize() {
            return ByteSize(Data.size());
        }

        static ui32 ByteSize(ui32 dataSize) {
            // Approximation message size. With different types of strings implementation there is no
            // chance to guess... but it's not needed actually. A following approximation should work.
            return sizeof(TEvLocalHandoff) + dataSize + 16u;
        }
    };


    // FIXME: Small blobs we should put to the buffer, but for large blobs we can save their id and read it later.
    //        This will allow us to handle spikes gracefully.

    namespace NHandoff {

        ////////////////////////////////////////////////////////////////////////////
        // TCounters
        ////////////////////////////////////////////////////////////////////////////
        struct TCounters : public TThrRefBase {
            // handling local handoff
            ui64 LocalHandoffSendRightAway = 0;
            ui64 LocalHandoffPostpone = 0;
            ui64 LocalHandoffDiscard = 0;

            // handling put result
            ui64 ReplyOKResult = 0;
            ui64 ReplyInvalidVDisk = 0;
            ui64 ReplyBadStatusResult = 0;
            ui64 ReplyOrphanOKResult = 0;
            ui64 ReplyCookieMismatch = 0;

            // problem handlers
            ui64 ReplyUndelivered = 0;
            ui64 WakeupsAlreadyGood = 0;
            ui64 WakeupsStillBad = 0;

            // state transition
            ui64 StateGoodToBadTransition = 0;
            ui64 StateBadToGoodTransition = 0;

            TCounters &operator += (const TCounters &s);
            void OutputHtml(IOutputStream &str) const;
        };

        typedef TIntrusivePtr<TCounters> TCountersPtr;

        struct TProxyState {
            TAtomic Monitor;
            TAtomic EarlyDiscardCounter;
            TVDiskID SelfVDiskID;
            TVDiskID TargetVDiskID;
            TActorId ProxyID;
            ui32 MaxBytes;
            bool Initialized;

            TProxyState();
            TProxyState(IInputStream &);
            void Init(const TVDiskID &selfVDiskID,
                      const TVDiskID &targetVDiskID,
                      ui32 maxElems,
                      ui32 maxBytes);
            bool Restore(const TActorContext &ctx,
                         const TLogoBlobID &id,
                         ui64 fullDataSize,
                         TRope&& data);
            void FreeElement(ui32 byteSize);
            TString ToString() const;
        };

        using TVDiskInfo = NSync::TVDiskInfo<TProxyState>;
        using TProxies = NSync::TVDiskNeighbors<TProxyState>;
        using TProxiesPtr = TIntrusivePtr<TProxies>;


        ////////////////////////////////////////////////////////////////////////////
        // TPrivateProxyState
        ////////////////////////////////////////////////////////////////////////////
        struct TPrivateProxyState : public TThrRefBase {
            enum EBadness {
                GOOD = 0,
                FLAP = 1,
                BAD = 2,
                PERMANENTLY_BAD = 3,
                BADNESS_MAX = 4
            };

            static EBadness NextBad(EBadness s) {
                return (s == PERMANENTLY_BAD) ? s : static_cast<EBadness>(static_cast<int>(s) + 1);
            }

            ui64 WaitQueueSize = 0;
            ui64 WaitQueueByteSize = 0;
            ui64 InFlightQueueSize = 0;
            ui64 InFlightQueueByteSize = 0;
            ui64 CookieCounter = TAppData::RandomProvider->GenRand64();
            TInstant LastSendTime = {};
            ui32 WakeupCounter = 0;
            EBadness BadnessState = GOOD;

            TPrivateProxyState() = default;
            void OutputHtml(IOutputStream &str) const;
        };

        typedef TIntrusivePtr<TPrivateProxyState> TPrivateProxyStatePtr;


        ////////////////////////////////////////////////////////////////////////////
        // Messages
        ////////////////////////////////////////////////////////////////////////////
        struct TEvHandoffProxyMon : public TEventLocal<TEvHandoffProxyMon, TEvBlobStorage::EvHandoffProxyMon> {
        };

        struct TEvHandoffProxyMonResult : public TEventLocal<TEvHandoffProxyMonResult, TEvBlobStorage::EvHandoffProxyMonResult> {
            TCountersPtr CountersPtr;
            TPrivateProxyStatePtr PrivateProxyStatePtr;
            TVDiskID VDiskID;

            TEvHandoffProxyMonResult(const TCounters &counters, const TPrivateProxyState &privateState,
                                     const TVDiskID &vdisk)
                : CountersPtr(new TCounters(counters))
                , PrivateProxyStatePtr(new TPrivateProxyState(privateState))
                , VDiskID(vdisk)
            {}
        };

    } // NHandoff

} // NKikimr

