#pragma once

#include "defs.h"
#include "scrub_actor.h"

namespace NKikimr {

    struct TEvRestoreCorruptedBlobResult;
    struct TEvNonrestoredCorruptedBlobNotify;

    class TScrubCoroImpl : public TActorCoroImpl {
        using TFreshSegmentSnapshot = NKikimr::TFreshSegmentSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
        using TIndexRecordMerger = NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelIndexSnapshot = NKikimr::TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelSegment = NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;

        TString CurrentState;
        const TScrubContext::TPtr ScrubCtx;
        const TIntrusivePtr<TVDiskContext> VCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TString LogPrefix;

        std::optional<NKikimrVDiskData::TScrubState> State;

        ::NMonitoring::TDynamicCounterPtr Counters;
        NMonGroup::TScrubGroup MonGroup;

        TRopeArena Arena;

        struct TExDie {};
        struct TExPoison {};

        struct TBlobOnDisk {
            TLogoBlobID Id;
            NMatrix::TVectorType Local;
            TDiskPart Part;
        };

        TActorId BlobRecoveryActorId;

        std::optional<ui64> SstId;
        ui32 Checkpoints = 0;

        bool Success = false;

    private:
        struct TUnreadableBlobState {
            NMatrix::TVectorType UnreadableParts; // parts we're going to recover from peer disks
            TDiskPart CorruptedPart;
            ui64 RecoveryInFlightCookie = 0; // 0 -- not being recovered
            TInstant RetryTimestamp;

            TUnreadableBlobState(NMatrix::TVectorType unreadableParts, TDiskPart corruptedPart)
                : UnreadableParts(unreadableParts)
                , CorruptedPart(corruptedPart)
            {}
        };

        using TUnreadableBlobStateMap = std::unordered_map<TLogoBlobID, TUnreadableBlobState, THash<TLogoBlobID>>;
        TUnreadableBlobStateMap UnreadableBlobs;
        ui64 LastRestoreCookie = 0;
        ui64 LastReceivedRestoreCookie = 0;
        bool GenerateRestoreCorruptedBlobQueryScheduled = false;

        void DropGarbageBlob(const TLogoBlobID& fullId);
        void AddUnreadableParts(const TLogoBlobID& fullId, NMatrix::TVectorType corrupted, TDiskPart corruptedPart);
        void UpdateUnreadableParts(const TLogoBlobID& fullId, NMatrix::TVectorType corrupted, TDiskPart corruptedPart);
        void UpdateReadableParts(const TLogoBlobID& fullId, NMatrix::TVectorType readable);
        void Handle(TEvTakeHullSnapshotResult::TPtr ev);
        void FilterUnreadableBlobs(THullDsSnap& snap, TBarriersSnapshot::TBarriersEssence& barriers);

        ui64 GenerateRestoreCorruptedBlobQuery();
        void Handle(TAutoPtr<TEventHandle<TEvRestoreCorruptedBlobResult>> ev);
        void Handle(TAutoPtr<TEventHandle<TEvNonrestoredCorruptedBlobNotify>> ev);
        void HandleGenerateRestoreCorruptedBlobQuery();

    public:
        TScrubCoroImpl(TScrubContext::TPtr scrubCtx, NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint,
            ui64 scrubEntrypointLsn);
        void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev);
        void Handle(NMon::TEvHttpInfo::TPtr ev);
        void ForwardToBlobRecoveryActor(TAutoPtr<IEventHandle> ev);

        TString RenderHtml(ui32 maxUnreadableBlobs) const;

        void Run() override;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // MAIN OPERATION CYCLE
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        NKikimrVDiskData::TScrubEntrypoint ScrubEntrypoint;
        ui64 ScrubEntrypointLsn = 0;

        void RequestState();
        void Quantum();
        void CommitStateUpdate();
        void IssueEntrypoint();
        void Handle(NPDisk::TEvLogResult::TPtr ev);
        void Handle(NPDisk::TEvCutLog::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SNAPSHOT OPERATION
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<THullDsSnap> Snap;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;

        void TakeSnapshot();
        void ReleaseSnapshot();
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> GetBarriersEssence();

        class THugeBlobMerger;
        class THugeBlobAndIndexMerger;
        class TSstBlobMerger;
        class TBlobLocationExtractorMerger;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDISK INTERACTION
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<TRcBuf> Read(const TDiskPart& part);
        bool IsReadable(const TDiskPart& part);
        void Write(const TDiskPart& part, TString data);

        template<typename T>
        typename T::TPtr WaitForPDiskEvent() {
            auto res = WaitForSpecificEvent<T>(&TScrubCoroImpl::ProcessUnexpectedEvent);
            if (res->Get()->Status == NKikimrProto::INVALID_ROUND) {
                throw TExDie(); // this VDisk is dead and racing with newly created one, so we terminate the disk
            }
            return res;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // HUGE BLOB SCRUBBING
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Function does iteration of huge blob scrubbing; last scrubbed blob is taken from the state and written back
        // to the state unless the process is finished. In this case, BlobId is simply reset in the state.
        void ScrubHugeBlobs();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SST SCRUBBING
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void ScrubSst(TLevelSegmentPtr sst);
        void ReadOutAndResilverIndex(TLevelSegmentPtr sst);
        std::vector<TBlobOnDisk> MakeBlobList(TLevelSegmentPtr sst);
        void ReadOutSelectedBlobs(std::vector<TBlobOnDisk>&& blobsOnDisk);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // DEBUG
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ui64 ScrubIterationCounter = 0;
        using TScrubAwaitQueue = std::multimap<ui64, std::pair<TActorId, ui64>>;
        TScrubAwaitQueue ScrubAwaitQueue;
        void HandleScrubAwait(TAutoPtr<IEventHandle> ev);

    private:
        static TIntrusivePtr<IContiguousChunk> AllocateRopeArenaChunk();
    };

} // NKikimr
