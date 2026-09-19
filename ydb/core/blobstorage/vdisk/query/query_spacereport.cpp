#include "query_public.h"
#include "query_spacereport_scan.h"
#include "query_statalgo.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/chunk_keeper/chunk_keeper_events.h>
#include <ydb/core/blobstorage/vdisk/common/align.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>

#include <util/string/join.h>

#include <utility>
#include <vector>

namespace NKikimr {
namespace {

    using namespace NVDiskSpaceReport;

    struct TComponentState {
        ui64 ChunkCount = 0;
        ui64 AllocatedBytes = 0;
        TSpaceBreakdown Breakdown;
    };

    struct THugeClassState {
        NHuge::TSizeClassSpaceStat Allocator;
        TSpaceBreakdown Classified;
        ui64 UsefulSlots = 0;
        ui64 GcDeadSlots = 0;
        ui64 MergeRedundantSlots = 0;
        ui64 UnclassifiedSlots = 0;
        TSpaceBreakdown Final;
    };

    struct TSyncLogSourceState {
        ui64 ChunkSizeBytes = 0;
        ui64 ActiveChunkCount = 0;
        ui64 UsedBytes = 0;
        ui64 FreeBytes = 0;
    };

    template <class TMerger, class TCommit>
    class TPerKeySpaceAggregator {
    public:
        TPerKeySpaceAggregator(TMerger* merger, TCommit commit)
            : Merger(merger)
            , Commit(std::move(commit))
        {}

        template <class TKey>
        void BeginKey(const TKey&) {
            Merger->Clear();
        }

        template <class TMemRec, class TKey>
        void UpdateFreshRecord(const TMemRec& memRec, const TRope* data, const TKey& key, ui64 lsn) {
            Merger->AddFromFresh(memRec, data, key, lsn);
        }

        template <class TMemRec, class TKey>
        void UpdateLevelRecord(const TMemRec& memRec, const TDiskPart* outbound, const TKey& key,
                ui64 circaLsn, const TLevelSegment<TKey, TMemRec>* sst) {
            Merger->AddFromSegment(memRec, outbound, key, circaLsn, sst);
        }

        template <class TKey>
        void FinishKey(const TKey& key) {
            Merger->Finish();
            Commit(key, *Merger);
        }

        void Finish() {
        }

    private:
        TMerger* Merger;
        TCommit Commit;
    };

    void FillBreakdown(const TSpaceBreakdown& source, NKikimrVDisk::TVDiskSpaceBreakdown* target) {
        target->SetUsefulBlobDataBytes(source.UsefulBlobDataBytes);
        target->SetLiveMetadataBytes(source.LiveMetadataBytes);
        target->SetLiveAuxiliaryDataBytes(source.LiveAuxiliaryDataBytes);
        target->SetGcDeadBlobDataBytes(source.GcDeadBlobDataBytes);
        target->SetGcDeadMetadataBytes(source.GcDeadMetadataBytes);
        target->SetMergeRedundantBlobDataBytes(source.MergeRedundantBlobDataBytes);
        target->SetMergeRedundantMetadataBytes(source.MergeRedundantMetadataBytes);
        target->SetWritePaddingBytes(source.WritePaddingBytes);
        target->SetSlotInternalFragmentationBytes(source.SlotInternalFragmentationBytes);
        target->SetFreeSlotBytes(source.FreeSlotBytes);
        target->SetChunkTailBytes(source.ChunkTailBytes);
        target->SetFreeChunkReserveBytes(source.FreeChunkReserveBytes);
        target->SetLockedOrQuarantinedBytes(source.LockedOrQuarantinedBytes);
        target->SetUnclassifiedBytes(source.UnclassifiedBytes);
    }

    void FillComponent(const TComponentState& source, NKikimrVDisk::TVDiskSpaceComponent* target) {
        target->SetChunkCount(source.ChunkCount);
        target->SetAllocatedBytes(source.AllocatedBytes);
        FillBreakdown(source.Breakdown, target->MutableBreakdown());
    }

    void AddPhysicalSsts(TComponentState& component, const TPhysicalSstEstimate& estimate) {
        component.ChunkCount += estimate.ChunkCount;
        component.Breakdown.LiveMetadataBytes += estimate.StructuralMetadataBytes;
    }

    void FinishHullComponent(TComponentState& component, ui64 chunkSize) {
        component.AllocatedBytes = component.ChunkCount * chunkSize;
        const ui64 accountedBytes = component.Breakdown.TotalBytes();
        if (accountedBytes < component.AllocatedBytes) {
            component.Breakdown.ChunkTailBytes += component.AllocatedBytes - accountedBytes;
        }
    }

    class TVDiskSpaceReportActor : public TActorBootstrapped<TVDiskSpaceReportActor> {
        using TThis = TVDiskSpaceReportActor;
        using TBase = TActorBootstrapped<TThis>;
        using TBlobYieldedState = TDbStatYieldedState<TKeyLogoBlob, TMemRecLogoBlob>;
        using TBlocksYieldedState = TDbStatYieldedState<TKeyBlock, TMemRecBlock>;
        using TBarriersYieldedState = TDbStatYieldedState<TKeyBarrier, TMemRecBarrier>;

        enum EEv {
            EvSourceTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

        struct TEvSourceTimeout : TEventLocal<TEvSourceTimeout, EvSourceTimeout> {};

        enum class EPhase {
            LogoBlobs,
            Blocks,
            Barriers,
            Done,
        };

        static constexpr size_t MaxHugeReferencesPerKey = 4096;
        static constexpr TDuration SourceTimeout = TDuration::Seconds(10);
        static constexpr TDbStatYieldPolicy YieldPolicy = {
            // Check after every complete key. Consequently, only the accepted
            // overpopulated-key case can overrun the target quantum.
            .StepsBeforeMeasures = 1,
            .QuantumDuration = TDuration::MilliSeconds(5),
            .DelayBetweenQuanta = TDuration::MilliSeconds(10),
        };

        friend class TActorBootstrapped<TThis>;

        void Bootstrap() {
            TThis::Become(&TThis::StateFunc);
            RequestSources();
        }

        void RequestSources() {
            AwaitedSources = 1;
            TThis::Send(PDiskCtx->PDiskId,
                new NPDisk::TEvCheckSpace(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound));

            if (HugeKeeperId) {
                ++AwaitedSources;
                TThis::Send(HugeKeeperId, new TEvHugeSpaceStat);
            } else {
                SourceErrors.emplace_back("HugeKeeper is unavailable");
            }
            if (SyncLogId) {
                ++AwaitedSources;
                TThis::Send(SyncLogId, new NSyncLog::TEvSyncLogSpaceStat);
            } else {
                SourceErrors.emplace_back("SyncLog is unavailable");
            }
            if (ChunkKeeperId) {
                ++AwaitedSources;
                TThis::Send(ChunkKeeperId, new TEvChunkKeeperSpaceStat);
            } else {
                SourceErrors.emplace_back("ChunkKeeper is unavailable");
            }

            TThis::Schedule(SourceTimeout, new TEvSourceTimeout);
        }

        void SourceReceived() {
            Y_ABORT_UNLESS(AwaitedSources);
            if (!--AwaitedSources) {
                StartScan();
            }
        }

        void StartScan() {
            if (!std::exchange(ScanStarted, true)) {
                InitializeHugeClasses();
                RequestSnapshot();
            }
        }

        void InitializeHugeClasses() {
            HugeClasses.reserve(HugeSource.SizeClasses.size());
            for (auto& source : HugeSource.SizeClasses) {
                const size_t index = HugeClasses.size();
                const auto [_, inserted] = HugeClassBySlotSize.emplace(source.SlotSize, index);
                if (!inserted) {
                    SourceErrors.emplace_back("HugeKeeper returned a duplicate slot size");
                    continue;
                }
                HugeClasses.push_back({.Allocator = source});
            }
        }

        void RequestSnapshot() {
            Y_ABORT_UNLESS(ScanStarted && Phase != EPhase::Done && !SnapshotRequested);
            SnapshotRequested = true;
            TThis::Send(ParentId, new TEvTakeHullSnapshot(true));
        }

        void AddHugeBlob(const TClassifiedHugeBlob& blob) {
            if (!HugeBlobCtx || !HugeBlobCtx->HugeSlotsMap) {
                return;
            }
            const THugeSlotsMap::TSlotInfo* slotInfo = HugeBlobCtx->HugeSlotsMap->GetSlotInfo(blob.Part.Size);
            if (!slotInfo) {
                return;
            }
            const auto it = HugeClassBySlotSize.find(slotInfo->SlotSize);
            if (it == HugeClassBySlotSize.end()) {
                return;
            }
            if (blob.Part.Size > slotInfo->SlotSize) {
                return;
            }

            THugeClassState& sizeClass = HugeClasses[it->second];
            AddClassifiedHugeBlob(sizeClass.Classified, blob);
            const ui64 writtenSize = Min<ui64>(
                slotInfo->SlotSize,
                AlignUpAppendBlockSize(blob.Part.Size, PDiskCtx->Dsk->AppendBlockSize));
            sizeClass.Classified.WritePaddingBytes += writtenSize - blob.Part.Size;
            sizeClass.Classified.SlotInternalFragmentationBytes += slotInfo->SlotSize - writtenSize;
            switch (blob.Classification) {
                case EHugeBlobClassification::Useful:
                    ++sizeClass.UsefulSlots;
                    break;
                case EHugeBlobClassification::GcDead:
                    ++sizeClass.GcDeadSlots;
                    break;
                case EHugeBlobClassification::MergeRedundant:
                    ++sizeClass.MergeRedundantSlots;
                    break;
            }
        }

        bool ScanLogoBlobs(THullDsSnap& snapshot) {
            const auto barriers = snapshot.BarriersSnap.CreateEssence(HullCtx);
            TLogoBlobSpaceMerger merger(
                HullCtx->VCtx->Top->GType,
                barriers.Get(),
                HullCtx->AllowKeepFlags,
                true,
                MaxHugeReferencesPerKey,
                HugeBlobCtx.get(),
                MinHugeBlobInBytes);

            auto aggregator = TPerKeySpaceAggregator(&merger,
                [this](const TKeyLogoBlob&, const TLogoBlobSpaceMerger& keyMerger) {
                    const TLogoBlobKeyEstimate& estimate = keyMerger.GetConclusion();
                    LogoBlobs.Breakdown += estimate.Hull;
                    AddPhysicalSsts(LogoBlobs, estimate.PhysicalSsts);
                    if (!estimate.HugeRefsOverflow) {
                        for (const TClassifiedHugeBlob& blob : estimate.HugeBlobs) {
                            AddHugeBlob(blob);
                        }
                    }
                });
            BlobYieldedState = TraverseDbWithoutMerge(
                HullCtx,
                &aggregator,
                snapshot.LogoBlobsSnap,
                std::move(BlobYieldedState),
                YieldPolicy);
            return !BlobYieldedState;
        }

        bool ScanBlocks(THullDsSnap& snapshot) {
            TBlocksSpaceMerger merger(
                HullCtx->VCtx->Top->GType,
                nullptr,
                HullCtx->AllowKeepFlags,
                true);
            auto aggregator = TPerKeySpaceAggregator(&merger,
                [this](const TKeyBlock&, const TBlocksSpaceMerger& keyMerger) {
                    const auto& estimate = keyMerger.GetConclusion();
                    Blocks.Breakdown += estimate.Breakdown;
                    AddPhysicalSsts(Blocks, estimate.PhysicalSsts);
                });
            BlocksYieldedState = TraverseDbWithoutMerge(
                HullCtx,
                &aggregator,
                snapshot.BlocksSnap,
                std::move(BlocksYieldedState),
                YieldPolicy);
            return !BlocksYieldedState;
        }

        bool ScanBarriers(THullDsSnap& snapshot) {
            const auto barriers = snapshot.BarriersSnap.CreateEssence(HullCtx);
            TBarriersSpaceMerger merger(
                HullCtx->VCtx->Top->GType,
                barriers.Get(),
                HullCtx->AllowKeepFlags,
                true);
            auto aggregator = TPerKeySpaceAggregator(&merger,
                [this](const TKeyBarrier&, const TBarriersSpaceMerger& keyMerger) {
                    const auto& estimate = keyMerger.GetConclusion();
                    Barriers.Breakdown += estimate.Breakdown;
                    AddPhysicalSsts(Barriers, estimate.PhysicalSsts);
                });
            BarriersYieldedState = TraverseDbWithoutMerge(
                HullCtx,
                &aggregator,
                snapshot.BarriersSnap,
                std::move(BarriersYieldedState),
                YieldPolicy);
            return !BarriersYieldedState;
        }

        void FinishHuge(TComponentState& huge) {
            for (THugeClassState& sizeClass : HugeClasses) {
                const auto& allocator = sizeClass.Allocator;
                const ui64 capacity = allocator.ChunkCount * ChunkSize;
                const ui64 totalSlots = allocator.ChunkCount * allocator.SlotsPerChunk;
                const ui64 slotArea = totalSlots * allocator.SlotSize;
                const ui64 classifiedSlots = sizeClass.UsefulSlots
                    + sizeClass.GcDeadSlots
                    + sizeClass.MergeRedundantSlots;

                bool invalid = slotArea > capacity
                    || allocator.FreeSlots > totalSlots
                    || allocator.LockedFreeSlots > allocator.FreeSlots
                    || allocator.AllocatedSlots > totalSlots - allocator.FreeSlots
                    || classifiedSlots > allocator.AllocatedSlots;

                TSpaceBreakdown breakdown;
                if (!invalid) {
                    breakdown = sizeClass.Classified;
                    sizeClass.UnclassifiedSlots = allocator.AllocatedSlots - classifiedSlots;
                }

                if (invalid) {
                    sizeClass.UsefulSlots = 0;
                    sizeClass.GcDeadSlots = 0;
                    sizeClass.MergeRedundantSlots = 0;
                    sizeClass.UnclassifiedSlots = totalSlots;
                    breakdown.UnclassifiedBytes = capacity;
                } else {
                    breakdown.UnclassifiedBytes += sizeClass.UnclassifiedSlots * allocator.SlotSize;
                    breakdown.FreeSlotBytes +=
                        (allocator.FreeSlots - allocator.LockedFreeSlots) * allocator.SlotSize;
                    breakdown.LockedOrQuarantinedBytes += allocator.LockedFreeSlots * allocator.SlotSize;

                    const ui64 describedSlots = allocator.AllocatedSlots + allocator.FreeSlots;
                    if (describedSlots < totalSlots) {
                        const ui64 missingSlots = totalSlots - describedSlots;
                        sizeClass.UnclassifiedSlots += missingSlots;
                        breakdown.UnclassifiedBytes += missingSlots * allocator.SlotSize;
                    }
                }

                if (!invalid) {
                    breakdown.ChunkTailBytes += capacity - slotArea;
                }
                sizeClass.Final = std::move(breakdown);
                huge.ChunkCount += allocator.ChunkCount;
                huge.Breakdown += sizeClass.Final;
            }

            const ui64 reserveBytes = HugeSource.FreeChunkCount * ChunkSize;
            huge.Breakdown.FreeChunkReserveBytes += reserveBytes;
            huge.ChunkCount += HugeSource.FreeChunkCount;
            huge.AllocatedBytes = huge.ChunkCount * ChunkSize;
        }

        void FinishSyncLog() {
            const ui64 activeBytes = SyncLogSource.ActiveChunkCount * ChunkSize;
            SyncLog.ChunkCount = SyncLogSource.ActiveChunkCount;
            SyncLog.AllocatedBytes = activeBytes;

            const ui64 describedActive = SyncLogSource.UsedBytes + SyncLogSource.FreeBytes;
            if (SyncLogSource.ChunkSizeBytes == ChunkSize && describedActive <= activeBytes) {
                SyncLog.Breakdown.LiveAuxiliaryDataBytes = SyncLogSource.UsedBytes;
                SyncLog.Breakdown.ChunkTailBytes = SyncLogSource.FreeBytes;
                SyncLog.Breakdown.UnclassifiedBytes = activeBytes - describedActive;
            } else {
                SyncLog.Breakdown.UnclassifiedBytes = activeBytes;
            }
        }

        void FinishChunkKeeper() {
            ChunkKeeper.reserve(ChunkKeeperSource.size());
            for (const auto& source : ChunkKeeperSource) {
                TComponentState component;
                component.ChunkCount = source.CommittedChunkCount;
                component.AllocatedBytes = component.ChunkCount * ChunkSize;
                component.Breakdown.UnclassifiedBytes = component.AllocatedBytes;
                ChunkKeeper.emplace_back(source.Subsystem, std::move(component));
            }
        }

        void FinalizeAndReply() {
            FinishHullComponent(LogoBlobs, ChunkSize);
            FinishHullComponent(Blocks, ChunkSize);
            FinishHullComponent(Barriers, ChunkSize);

            TComponentState huge;
            FinishHuge(huge);
            FinishSyncLog();
            FinishChunkKeeper();

            ui64 namedChunks = LogoBlobs.ChunkCount + Blocks.ChunkCount + Barriers.ChunkCount;
            namedChunks += huge.ChunkCount;
            namedChunks += SyncLog.ChunkCount;
            for (const auto& [_, component] : ChunkKeeper) {
                namedChunks += component.ChunkCount;
            }

            TComponentState unattributed;
            if (namedChunks < PDiskAllocatedChunks) {
                unattributed.ChunkCount = PDiskAllocatedChunks - namedChunks;
                unattributed.AllocatedBytes = unattributed.ChunkCount * ChunkSize;
                unattributed.Breakdown.UnclassifiedBytes = unattributed.AllocatedBytes;
            }

            TSpaceBreakdown total;
            total += LogoBlobs.Breakdown;
            total += Blocks.Breakdown;
            total += Barriers.Breakdown;
            total += huge.Breakdown;
            total += SyncLog.Breakdown;
            for (const auto& [_, component] : ChunkKeeper) {
                total += component.Breakdown;
            }
            total += unattributed.Breakdown;

            const ui64 pdiskBytes = PDiskAllocatedChunks * ChunkSize;
            const ui64 accountedBytes = total.TotalBytes();
            const NKikimrProto::EReplyStatus status = SourceErrors.empty()
                ? NKikimrProto::OK
                : NKikimrProto::ERROR;
            const TString errorReason = JoinSeq("; ", SourceErrors);
            auto response = std::make_unique<TEvGetVDiskSpaceReportResponse>(
                status, errorReason, TActivationContext::Now(), nullptr, nullptr);
            auto* report = response->Record.MutableReport();
            report->SetChunkSizeBytes(ChunkSize);
            report->SetPDiskAllocatedChunks(PDiskAllocatedChunks);
            report->SetPDiskAllocatedBytes(pdiskBytes);
            report->SetAccountedBytes(accountedBytes);
            report->SetReconciliationDeltaBytes(
                static_cast<i64>(pdiskBytes) - static_cast<i64>(accountedBytes));
            FillBreakdown(total, report->MutableTotal());
            FillComponent(LogoBlobs, report->MutableLogoBlobs());
            FillComponent(Blocks, report->MutableBlocks());
            FillComponent(Barriers, report->MutableBarriers());

            auto* hugeReport = report->MutableHuge();
            FillComponent(huge, hugeReport->MutableTotal());
            hugeReport->SetFreeReserveChunks(HugeSource.FreeChunkCount);
            for (const THugeClassState& sizeClass : HugeClasses) {
                auto* item = hugeReport->AddSizeClasses();
                item->SetSlotSizeBytes(sizeClass.Allocator.SlotSize);
                item->SetSlotsPerChunk(sizeClass.Allocator.SlotsPerChunk);
                item->SetChunkCount(sizeClass.Allocator.ChunkCount);
                item->SetLiveSlotCount(sizeClass.UsefulSlots);
                item->SetGcDeadSlotCount(sizeClass.GcDeadSlots);
                item->SetMergeRedundantSlotCount(sizeClass.MergeRedundantSlots);
                item->SetUnclassifiedSlotCount(sizeClass.UnclassifiedSlots);
                FillBreakdown(sizeClass.Final, item->MutableBreakdown());
            }

            FillComponent(SyncLog, report->MutableSyncLog());
            for (const auto& [subsystem, component] : ChunkKeeper) {
                auto* item = report->AddChunkKeeper();
                item->SetSubsystemId(subsystem);
                FillComponent(component, item->MutableTotal());
            }
            FillComponent(unattributed, report->MutableUnattributed());

            SendVDiskResponse(
                TActivationContext::AsActorContext(),
                Recipient,
                response.release(),
                Cookie,
                HullCtx->VCtx,
                {});
            PassAway();
        }

        void ReplyErrorAndDie(NKikimrProto::EReplyStatus status, const TString& errorReason) {
            auto response = std::make_unique<TEvGetVDiskSpaceReportResponse>(
                status, errorReason, TActivationContext::Now(), nullptr, nullptr);
            SendVDiskResponse(
                TActivationContext::AsActorContext(),
                Recipient,
                response.release(),
                Cookie,
                HullCtx->VCtx,
                {});
            PassAway();
        }

        void Handle(NPDisk::TEvCheckSpaceResult::TPtr& ev) {
            if (ScanStarted || std::exchange(PDiskReceived, true)) {
                return;
            }
            const auto& result = *ev->Get();
            if (result.Status != NKikimrProto::OK) {
                return ReplyErrorAndDie(result.Status, result.ErrorReason);
            }
            PDiskAllocatedChunks = result.UsedChunks;
            SourceReceived();
        }

        void Handle(TEvHugeSpaceStatResult::TPtr& ev) {
            if (ScanStarted || std::exchange(HugeReceived, true)) {
                return;
            }
            HugeSource = std::move(ev->Get()->Stat);
            SourceReceived();
        }

        void Handle(NSyncLog::TEvSyncLogSpaceStatResult::TPtr& ev) {
            if (ScanStarted || std::exchange(SyncLogReceived, true)) {
                return;
            }
            const auto& source = *ev->Get();
            SyncLogSource = {
                .ChunkSizeBytes = source.ChunkSizeBytes,
                .ActiveChunkCount = source.ActiveChunkCount,
                .UsedBytes = source.UsedBytes,
                .FreeBytes = source.FreeBytes,
            };
            SourceReceived();
        }

        void Handle(TEvChunkKeeperSpaceStatResult::TPtr& ev) {
            if (ScanStarted || std::exchange(ChunkKeeperReceived, true)) {
                return;
            }
            auto& source = *ev->Get();
            if (source.Status == NKikimrProto::OK) {
                ChunkKeeperSource = std::move(source.Subsystems);
            }
            SourceReceived();
        }

        void Handle(TEvSourceTimeout::TPtr&) {
            if (ScanStarted) {
                return;
            }
            if (!PDiskReceived) {
                return ReplyErrorAndDie(NKikimrProto::ERROR, "PDisk space counter timed out");
            }
            if (!HugeReceived && HugeKeeperId) {
                SourceErrors.emplace_back("HugeKeeper space counters timed out");
            }
            if (!SyncLogReceived && SyncLogId) {
                SourceErrors.emplace_back("SyncLog space counters timed out");
            }
            if (!ChunkKeeperReceived && ChunkKeeperId) {
                SourceErrors.emplace_back("ChunkKeeper space counters timed out");
            }
            StartScan();
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr& ev) {
            if (!ScanStarted || Phase == EPhase::Done ||
                    !std::exchange(SnapshotRequested, false)) {
                return;
            }
            THullDsSnap snapshot = std::move(ev->Get()->Snap);
            bool phaseComplete = false;
            switch (Phase) {
                case EPhase::LogoBlobs:
                    phaseComplete = ScanLogoBlobs(snapshot);
                    break;
                case EPhase::Blocks:
                    phaseComplete = ScanBlocks(snapshot);
                    break;
                case EPhase::Barriers:
                    phaseComplete = ScanBarriers(snapshot);
                    break;
                case EPhase::Done:
                    Y_ABORT("Unexpected completed VDisk space-report phase");
            }

            // No Hull snapshot or barriers essence survives this event turn.
            snapshot.LogoBlobsSnap.Destroy();
            snapshot.BlocksSnap.Destroy();
            snapshot.BarriersSnap.Destroy();

            if (phaseComplete) {
                Phase = static_cast<EPhase>(static_cast<ui8>(Phase) + 1);
            }
            if (Phase == EPhase::Done) {
                FinalizeAndReply();
            } else {
                TThis::Schedule(YieldPolicy.DelayBetweenQuanta, new TEvents::TEvWakeup);
            }
        }

        void HandleWakeup() {
            if (ScanStarted && Phase != EPhase::Done && !SnapshotRequested) {
                RequestSnapshot();
            }
        }

        void PassAway() override {
            TThis::Send(ParentId, new TEvents::TEvGone);
            TBase::PassAway();
        }

        STRICT_STFUNC(StateFunc, {
            hFunc(NPDisk::TEvCheckSpaceResult, Handle);
            hFunc(TEvHugeSpaceStatResult, Handle);
            hFunc(NSyncLog::TEvSyncLogSpaceStatResult, Handle);
            hFunc(TEvChunkKeeperSpaceStatResult, Handle);
            hFunc(TEvSourceTimeout, Handle);
            hFunc(TEvTakeHullSnapshotResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        })

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TVDiskSpaceReportActor(
                TIntrusivePtr<THullCtx> hullCtx,
                std::shared_ptr<THugeBlobCtx> hugeBlobCtx,
                TPDiskCtxPtr pdiskCtx,
                TActorId parentId,
                TActorId hugeKeeperId,
                TActorId syncLogId,
                TActorId chunkKeeperId,
                ui32 minHugeBlobInBytes,
                const TEvGetVDiskSpaceReportRequest::TPtr& ev)
            : HullCtx(std::move(hullCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , ParentId(parentId)
            , HugeKeeperId(hugeKeeperId)
            , SyncLogId(syncLogId)
            , ChunkKeeperId(chunkKeeperId)
            , MinHugeBlobInBytes(minHugeBlobInBytes)
            , Recipient(ev->Sender)
            , Cookie(ev->Cookie)
            , ChunkSize(PDiskCtx->Dsk->ChunkSize)
        {}

    private:
        const TIntrusivePtr<THullCtx> HullCtx;
        const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const TPDiskCtxPtr PDiskCtx;
        const TActorId ParentId;
        const TActorId HugeKeeperId;
        const TActorId SyncLogId;
        const TActorId ChunkKeeperId;
        const ui32 MinHugeBlobInBytes;
        const TActorId Recipient;
        const ui64 Cookie;
        const ui64 ChunkSize;

        ui32 AwaitedSources = 0;
        bool PDiskReceived = false;
        bool HugeReceived = false;
        bool SyncLogReceived = false;
        bool ChunkKeeperReceived = false;
        bool ScanStarted = false;
        bool SnapshotRequested = false;
        ui64 PDiskAllocatedChunks = 0;
        NHuge::THeapSpaceStat HugeSource;
        TSyncLogSourceState SyncLogSource;
        std::vector<TEvChunkKeeperSpaceStatResult::TSubsystemStat> ChunkKeeperSource;
        std::vector<TString> SourceErrors;

        EPhase Phase = EPhase::LogoBlobs;
        std::optional<TBlobYieldedState> BlobYieldedState;
        std::optional<TBlocksYieldedState> BlocksYieldedState;
        std::optional<TBarriersYieldedState> BarriersYieldedState;
        TComponentState LogoBlobs;
        TComponentState Blocks;
        TComponentState Barriers;
        TComponentState SyncLog;
        std::vector<THugeClassState> HugeClasses;
        THashMap<ui64, size_t> HugeClassBySlotSize;
        std::vector<std::pair<ui32, TComponentState>> ChunkKeeper;
    };

} // anonymous namespace

    IActor* CreateVDiskSpaceReportActor(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const std::shared_ptr<THugeBlobCtx>& hugeBlobCtx,
            const TPDiskCtxPtr& pdiskCtx,
            const TActorId& parentId,
            const TActorId& hugeKeeperId,
            const TActorId& syncLogId,
            const TActorId& chunkKeeperId,
            ui32 minHugeBlobInBytes,
            const TEvGetVDiskSpaceReportRequest::TPtr& ev)
    {
        return new TVDiskSpaceReportActor(
            hullCtx,
            hugeBlobCtx,
            pdiskCtx,
            parentId,
            hugeKeeperId,
            syncLogId,
            chunkKeeperId,
            minHugeBlobInBytes,
            ev);
    }

} // namespace NKikimr
