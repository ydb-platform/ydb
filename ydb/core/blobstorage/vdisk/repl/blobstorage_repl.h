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

    struct TUnreplicatedBlobRecord { // for monitoring purposes
        TIngress Ingress; // merged ingress from all the peers
        ui32 PartsMask = 0;
        ui32 DisksRepliedOK = 0;
        ui32 DisksRepliedNODATA = 0;
        ui32 DisksRepliedNOT_YET = 0;
        ui32 DisksRepliedOther = 0;
        bool LooksLikePhantom = false;
    };

    using TUnreplicatedBlobRecords = std::unordered_map<TLogoBlobID, TUnreplicatedBlobRecord>;

    struct TEvReplInvoke : TEventLocal<TEvReplInvoke, TEvBlobStorage::EvReplInvoke> {
        std::function<void(const TUnreplicatedBlobRecords&, TString)> Callback;

        TEvReplInvoke(std::function<void(const TUnreplicatedBlobRecords&, TString)> callback)
            : Callback(std::move(callback))
        {}
    };

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
            bool UnrecoveredNonphantomBlobs = false;
            TVDiskID DonorVDiskId;
            bool DropDonor = false;

            // plan generation stats
            ui64 ItemsTotal = 0; // total blobs to be recovered
            ui64 ItemsPlanned = 0; // total blobs to be recovered in this quantum
            ui64 WorkUnitsTotal = 0;
            ui64 WorkUnitsPlanned = 0;

            // plan execution stats
            ui64 ItemsRecovered = 0; // blobs successfully recovered
            ui64 ItemsNotRecovered = 0; // blobs with not enough parts to recover
            ui64 ItemsException = 0; // blobs with exception during restoration
            ui64 ItemsPartiallyRecovered = 0; // partially recovered blobs -- with exact parts, but not complete
            ui64 ItemsPhantom = 0; // actual phantom blobs that are dropped
            ui64 ItemsNonPhantom = 0; // phantom-like blobs kept
            ui64 WorkUnitsPerformed = 0;

            // detailed stats
            ui64 BytesRecovered = 0;
            ui64 LogoBlobsRecovered = 0;
            ui64 HugeLogoBlobsRecovered = 0;
            ui64 ChunksWritten = 0;
            ui64 SstBytesWritten = 0;
            ui64 MetadataBlobs = 0;

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

            TUnreplicatedBlobRecords UnreplicatedBlobRecords;

            void Finish(const TLogoBlobID &keyPos, bool eof, bool dropDonor, TUnreplicatedBlobRecords&& ubr) {
                End = TAppData::TimeProvider->Now();
                KeyPos = keyPos;
                Eof = eof;
                DropDonor = dropDonor;
                UnreplicatedBlobRecords = std::move(ubr);
            }

            TString ToString() const;
            void OutputHtml(IOutputStream &str) const;

            TString WorkUnits() const {
                return TStringBuilder()
                    << "{Total# " << WorkUnitsTotal
                    << " Planned# " << WorkUnitsPlanned
                    << " Performed# " << WorkUnitsPerformed
                    << "}";
            }

            TString Items() const {
                return TStringBuilder()
                    << "{Total# " << ItemsTotal
                    << " Planned# " << ItemsPlanned
                    << " Recovered# " << ItemsRecovered
                    << " NotRecovered# " << ItemsNotRecovered
                    << " Exception# " << ItemsException
                    << " PartiallyRecovered# " << ItemsPartiallyRecovered
                    << " Phantom# " << ItemsPhantom
                    << " NonPhantom# " << ItemsNonPhantom
                    << "}";
            }

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
            Y_DEBUG_ABORT_UNLESS(Id.PartId() != 0);
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

    struct TEvReplCheckProgress : TEventLocal<TEvReplCheckProgress, TEvBlobStorage::EvReplCheckProgress> {};

    ////////////////////////////////////////////////////////////////////////////
    // REPL ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateReplActor(std::shared_ptr<TReplCtx> &replCtx);

    IActor *CreateReplMonRequestHandler(TActorId skeletonId, TVDiskIdShort vdiskId,
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology, NMon::TEvHttpInfo::TPtr ev);

} // NKikimr
