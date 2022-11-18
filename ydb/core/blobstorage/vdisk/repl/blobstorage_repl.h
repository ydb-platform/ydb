#pragma once

#include "defs.h"
#include "blobstorage_replctx.h"
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <util/generic/fwd.h>

namespace NKikimr {

    namespace NRepl {
        struct TProxyStat;
    };

    using TBlobIdQueue = std::deque<TLogoBlobID>;
    using TBlobIdQueuePtr = std::shared_ptr<TBlobIdQueue>;

    ////////////////////////////////////////////////////////////////////////////
    // Internal Repl messages
    ////////////////////////////////////////////////////////////////////////////
    struct TEvReplStarted : TEventLocal<TEvReplStarted, TEvBlobStorage::EvReplStarted> {};

    struct TEvReplFinished : public TEventLocal<TEvReplFinished, TEvBlobStorage::EvReplFinished> {
        struct TInfo : public TThrRefBase {
            // basic
            TInstant Start;
            TInstant End;
            TLogoBlobID KeyPos;
            bool Eof;
            TVDiskID DonorVDiskId;
            bool DropDonor = false;

            // generate plan stats
            ui64 ReplicaOk = 0;
            ui64 RecoveryScheduled = 0;
            ui64 IgnoredDueToGC = 0;
            ui64 WorkUnitsPlanned = 0;
            ui64 WorkUnitsTotal = 0;
            ui64 WorkUnitsProcessed = 0;
            ui64 PhantomLike = 0;

            // plan execution stats
            ui64 DataRecoverySuccess = 0;
            ui64 DataRecoveryFailure = 0;
            ui64 DataRecoveryNoParts = 0;
            ui64 DataRecoverySkip = 0;
            ui64 DataRecoveryPhantomCheck = 0;

            // detailed stats
            ui64 BytesRecovered = 0;
            ui64 LogoBlobsRecovered = 0;
            ui64 HugeLogoBlobsRecovered = 0;
            ui64 ChunksWritten = 0;
            ui64 SstBytesWritten = 0;
            ui64 MultipartBlobs = 0;
            ui64 MetadataBlobs = 0;
            ui64 PartsPlanned = 0;
            ui64 PartsExact = 0;
            ui64 PartsRestored = 0;
            ui64 PartsMissing = 0;

            // time durations
            TDuration PreparePlanDuration;
            TDuration TokenWaitDuration;
            TDuration ProxyWaitDuration;
            TDuration MergeDuration;
            TDuration PDiskDuration;
            TDuration CommitDuration;
            TDuration OtherDuration;
            TDuration PhantomDuration;

            std::unique_ptr<NRepl::TProxyStat> ProxyStat;

            void Finish(const TLogoBlobID &keyPos, bool eof, bool dropDonor) {
                End = TAppData::TimeProvider->Now();
                KeyPos = keyPos;
                Eof = eof;
                DropDonor = dropDonor;
            }

            TString ToString() const;
            void OutputHtml(IOutputStream &str) const;

            TInfo();
            ~TInfo();
        };

        typedef TIntrusivePtr<TInfo> TInfoPtr;
        TInfoPtr Info;

        TEvReplFinished(TInfoPtr &info)
            : Info(info)
        {}
    };

    struct TEvReplResume : TEventLocal<TEvReplResume, TEvBlobStorage::EvReplResume> {};

    ////////////////////////////////////////////////////////////////////////////
    // Message for recovered data (to Skeleton)
    ////////////////////////////////////////////////////////////////////////////
    struct TEvRecoveredHugeBlob : public TEventLocal<TEvRecoveredHugeBlob, TEvBlobStorage::EvRecoveredHugeBlob> {
        const TLogoBlobID Id;
        TRope Data;

        TEvRecoveredHugeBlob(const TLogoBlobID &id, TRope&& data)
            : Id(id)
            , Data(std::move(data))
        {
            Y_VERIFY_DEBUG(Id.PartId() != 0);
        }

        size_t ByteSize() const {
            return sizeof(TLogoBlobID) + Data.GetSize();
        }
    };

    struct TEvDetectedPhantomBlob : public TEventLocal<TEvDetectedPhantomBlob, TEvBlobStorage::EvDetectedPhantomBlob> {
        TDeque<TLogoBlobID> Phantoms;

        TEvDetectedPhantomBlob(TDeque<TLogoBlobID>&& phantoms)
            : Phantoms(std::move(phantoms))
        {}

        size_t ByteSize() const {
            return sizeof(TLogoBlobID) * Phantoms.size();
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // REPL ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateReplActor(std::shared_ptr<TReplCtx> &replCtx);

} // NKikimr
