#include "query_public.h"
#include "query_spacereport_scan.h"
#include "query_statalgo.h"

#include <ydb/core/blobstorage/base/utility.h>
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
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/digest/multi.h>
#include <util/generic/hash_set.h>
#include <util/random/fast.h>
#include <util/string/join.h>
#include <util/string/cast.h>
#include <util/system/datetime.h>

#include <iterator>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NKikimr {
namespace {

    using namespace NVDiskSpaceReport;

#define VDISK_SPACE_REPORT_BREAKDOWN_FIELDS(XX) \
    XX(UsefulBlobDataBytes)                      \
    XX(LiveMetadataBytes)                        \
    XX(LiveAuxiliaryDataBytes)                   \
    XX(GcDeadBlobDataBytes)                      \
    XX(GcDeadMetadataBytes)                      \
    XX(MergeRedundantBlobDataBytes)              \
    XX(MergeRedundantMetadataBytes)              \
    XX(WritePaddingBytes)                        \
    XX(SlotInternalFragmentationBytes)           \
    XX(FreeSlotBytes)                            \
    XX(ChunkTailBytes)                           \
    XX(FreeChunkReserveBytes)                    \
    XX(LockedOrQuarantinedBytes)                 \
    XX(UnclassifiedBytes)                        \
    XX(FreeStripeBytes)

    enum EEv {
        EvSourceTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvScanComplete,
        EvPeriodicTick,
        EvWatchdog,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

    struct TEvSourceTimeout : TEventLocal<TEvSourceTimeout, EvSourceTimeout> {};

    struct TScanMetrics {
        TDuration Duration;
        ui64 CpuTimeUs = 0;
        ui64 Quanta = 0;
        ui64 VisitedKeys = 0;
        ui64 PhysicalRecords = 0;
    };

    struct TEvScanComplete : TEventLocal<TEvScanComplete, EvScanComplete> {
        const ui64 AttemptId;
        std::unique_ptr<TEvGetVDiskSpaceReportResponse> Response;
        const TScanMetrics Metrics;

        TEvScanComplete(
                ui64 attemptId,
                std::unique_ptr<TEvGetVDiskSpaceReportResponse> response,
                TScanMetrics metrics)
            : AttemptId(attemptId)
            , Response(std::move(response))
            , Metrics(metrics)
        {}
    };

    struct TEvPeriodicTick : TEventLocal<TEvPeriodicTick, EvPeriodicTick> {
        const ui64 Generation;
        const bool CheckOnly;

        TEvPeriodicTick(ui64 generation, bool checkOnly)
            : Generation(generation)
            , CheckOnly(checkOnly)
        {}
    };

    struct TEvWatchdog : TEventLocal<TEvWatchdog, EvWatchdog> {
        const ui64 AttemptId;

        explicit TEvWatchdog(ui64 attemptId)
            : AttemptId(attemptId)
        {}
    };

    struct TComponentState {
        ui64 ChunkCount = 0;
        ui64 StripedBytes = 0;
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
        TPerKeySpaceAggregator(
                TMerger* merger,
                TCommit commit,
                ui64* visitedKeys,
                ui64* physicalRecords)
            : Merger(merger)
            , Commit(std::move(commit))
            , VisitedKeys(visitedKeys)
            , PhysicalRecords(physicalRecords)
        {}

        template <class TKey>
        void BeginKey(const TKey&) {
            Merger->Clear();
        }

        template <class TMemRec, class TKey>
        void UpdateFreshRecord(const TMemRec& memRec, const TRope* data, const TKey& key, ui64 lsn) {
            ++*PhysicalRecords;
            Merger->AddFromFresh(memRec, data, key, lsn);
        }

        template <class TMemRec, class TKey>
        void UpdateLevelRecord(const TMemRec& memRec, const TDiskPart* outbound, const TKey& key,
                ui64 circaLsn, const TLevelSegment<TKey, TMemRec>* sst) {
            ++*PhysicalRecords;
            Merger->AddFromSegment(memRec, outbound, key, circaLsn, sst);
        }

        template <class TKey>
        void FinishKey(const TKey& key) {
            Merger->Finish();
            Commit(key, *Merger);
            ++*VisitedKeys;
        }

        void Finish() {
        }

    private:
        TMerger* Merger;
        TCommit Commit;
        ui64* VisitedKeys;
        ui64* PhysicalRecords;
    };

    void FillBreakdown(const TSpaceBreakdown& source, NKikimrVDisk::TVDiskSpaceBreakdown* target) {
#define SET_FIELD(name) target->Set##name(source.name);
        VDISK_SPACE_REPORT_BREAKDOWN_FIELDS(SET_FIELD)
#undef SET_FIELD
    }

    ui64 CalculateAccountedBytes(const NKikimrVDisk::TVDiskSpaceBreakdown& value) {
        ui64 result = 0;
#define ADD_FIELD(name) result += value.Get##name();
        VDISK_SPACE_REPORT_BREAKDOWN_FIELDS(ADD_FIELD)
#undef ADD_FIELD
        return result;
    }

    void FillComponent(const TComponentState& source, NKikimrVDisk::TVDiskSpaceComponent* target) {
        target->SetChunkCount(source.ChunkCount);
        target->SetStripedBytes(source.StripedBytes);
        target->SetAllocatedBytes(source.AllocatedBytes);
        FillBreakdown(source.Breakdown, target->MutableBreakdown());
    }

    void AddPhysicalSsts(TComponentState& component, const TPhysicalSstEstimate& estimate) {
        component.ChunkCount += estimate.ChunkCount;
        component.StripedBytes += estimate.StripedBytes;
        component.Breakdown.LiveMetadataBytes += estimate.StructuralMetadataBytes;
    }

    void FinishHullComponent(TComponentState& component, ui64 chunkSize) {
        // ChunkCount contains only dedicated chunks. Shared stripe chunks are
        // represented by the component-owned extents in StripedBytes instead.
        component.AllocatedBytes = component.ChunkCount * chunkSize + component.StripedBytes;
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
            CollectionStartedAt = TActivationContext::Now();
            CollectionStartedMonotonic = TActivationContext::Monotonic();
            TThis::Become(&TThis::StateFunc);
            RequestSources();
        }

        void RequestSources() {
            AwaitedSources = 1;
            TThis::Send(PDiskCtx->PDiskId,
                new NPDisk::TEvCheckSpace(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound));

            if (HugeKeeperId) {
                AwaitedSources += 2;
                TThis::Send(HugeKeeperId, new TEvHugeSpaceStat);
                TActivationContext::Send(new IEventHandle(
                    TEvBlobStorage::EvHugeQueryStripeChunks,
                    0,
                    HugeKeeperId,
                    TThis::SelfId(),
                    nullptr,
                    0));
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
            TThis::Send(SnapshotProviderId, new TEvTakeHullSnapshot(true));
        }

        void AddHugeBlob(const TClassifiedHugeBlob& blob) {
            if (StripeChunksReceived && StripeChunks.contains(blob.Part.ChunkIdx)) {
                AddClassifiedHugeBlob(StripedHugeBreakdown, blob);
                const ui64 allocatedBytes = AlignUpAppendBlockSize(
                    blob.Part.Size,
                    PDiskCtx->Dsk->AppendBlockSize);
                StripedHugeBreakdown.WritePaddingBytes += allocatedBytes - blob.Part.Size;
                HugeStripedBytes += allocatedBytes;
                return;
            }
            if (!StripeChunksReceived && HugeSource.StripeHeap.ChunkCount) {
                return;
            }
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
                PDiskCtx->Dsk->AppendBlockSize,
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
                }, &VisitedKeys, &PhysicalRecords);
            BlobYieldedState = TraverseDbWithoutMerge(
                HullCtx,
                &aggregator,
                snapshot.LogoBlobsSnap,
                std::move(BlobYieldedState),
                YieldPolicy);
            return !BlobYieldedState;
        }

        template <class TMerger, class TSnapshot, class TYieldedState>
        bool ScanMetadata(
                TMerger& merger,
                TComponentState& component,
                const TSnapshot& snapshot,
                std::optional<TYieldedState>& yieldedState) {
            auto aggregator = TPerKeySpaceAggregator(&merger,
                [&component](const auto&, const TMerger& keyMerger) {
                    const auto& estimate = keyMerger.GetConclusion();
                    component.Breakdown += estimate.Breakdown;
                    AddPhysicalSsts(component, estimate.PhysicalSsts);
                }, &VisitedKeys, &PhysicalRecords);
            yieldedState = TraverseDbWithoutMerge(
                HullCtx,
                &aggregator,
                snapshot,
                std::move(yieldedState),
                YieldPolicy);
            return !yieldedState;
        }

        bool ScanBlocks(THullDsSnap& snapshot) {
            TBlocksSpaceMerger merger(
                HullCtx->VCtx->Top->GType,
                nullptr,
                HullCtx->AllowKeepFlags,
                true,
                PDiskCtx->Dsk->AppendBlockSize);
            return ScanMetadata(merger, Blocks, snapshot.BlocksSnap, BlocksYieldedState);
        }

        bool ScanBarriers(THullDsSnap& snapshot) {
            const auto barriers = snapshot.BarriersSnap.CreateEssence(HullCtx);
            TBarriersSpaceMerger merger(
                HullCtx->VCtx->Top->GType,
                barriers.Get(),
                HullCtx->AllowKeepFlags,
                true,
                PDiskCtx->Dsk->AppendBlockSize);
            return ScanMetadata(merger, Barriers, snapshot.BarriersSnap, BarriersYieldedState);
        }

        void FinishHuge(TComponentState& huge, ui64 stripeChunkCount) {
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

            huge.StripedBytes = HugeStripedBytes;
            huge.Breakdown += StripedHugeBreakdown;

            const auto& stripes = HugeSource.StripeHeap;
            const ui64 stripeCapacity = stripeChunkCount * ChunkSize;
            const ui64 classifiedStripeBytes = LogoBlobs.StripedBytes
                + Blocks.StripedBytes
                + Barriers.StripedBytes
                + huge.StripedBytes;
            if (classifiedStripeBytes < stripeCapacity) {
                ui64 remaining = stripeCapacity - classifiedStripeBytes;
                const ui64 freeBytes = Min(stripes.FreeBytes, remaining);
                huge.Breakdown.FreeStripeBytes += freeBytes;
                remaining -= freeBytes;

                const ui64 lockedFreeBytes = Min(stripes.LockedFreeBytes, remaining);
                huge.Breakdown.LockedOrQuarantinedBytes += lockedFreeBytes;
                remaining -= lockedFreeBytes;

                huge.Breakdown.UnclassifiedBytes += remaining;
                huge.StripedBytes += stripeCapacity - classifiedStripeBytes;
            }
            huge.AllocatedBytes = huge.ChunkCount * ChunkSize + huge.StripedBytes;
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

        void FinalizeAndComplete() {
            FinishHullComponent(LogoBlobs, ChunkSize);
            FinishHullComponent(Blocks, ChunkSize);
            FinishHullComponent(Barriers, ChunkSize);

            const ui64 stripeChunkCount = HugeReceived
                ? HugeSource.StripeHeap.ChunkCount
                : StripeChunksReceived ? StripeChunks.size() : 0;
            TComponentState huge;
            FinishHuge(huge, stripeChunkCount);
            FinishSyncLog();
            FinishChunkKeeper();

            ui64 namedChunks = LogoBlobs.ChunkCount + Blocks.ChunkCount + Barriers.ChunkCount;
            namedChunks += huge.ChunkCount;
            namedChunks += SyncLog.ChunkCount;
            for (const auto& [_, component] : ChunkKeeper) {
                namedChunks += component.ChunkCount;
            }
            namedChunks += stripeChunkCount;

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
            report->SetCollectionStartedAtUnixMs(CollectionStartedAt.MilliSeconds());
            report->SetCollectionCompletedAtUnixMs(TActivationContext::Now().MilliSeconds());

            auto* stripeReport = report->MutableStripeHeap();
            stripeReport->SetChunkCount(stripeChunkCount);
            stripeReport->SetAllocatedBytes(stripeChunkCount * ChunkSize);
            stripeReport->SetUsedBytes(HugeSource.StripeHeap.UsedBytes);
            stripeReport->SetFreeBytes(HugeSource.StripeHeap.FreeBytes);
            stripeReport->SetLockedFreeBytes(HugeSource.StripeHeap.LockedFreeBytes);

            CompleteAndDie(std::move(response));
        }

        void CompleteErrorAndDie(NKikimrProto::EReplyStatus status, const TString& errorReason) {
            auto response = std::make_unique<TEvGetVDiskSpaceReportResponse>(
                status, errorReason, TActivationContext::Now(), nullptr, nullptr);
            CompleteAndDie(std::move(response));
        }

        void CompleteAndDie(std::unique_ptr<TEvGetVDiskSpaceReportResponse> response) {
            const TMonotonic completed = TActivationContext::Monotonic();
            TThis::Send(OwnerId, new TEvScanComplete(
                AttemptId,
                std::move(response),
                {
                    .Duration = completed - CollectionStartedMonotonic,
                    .CpuTimeUs = ScanCpuTimeUs,
                    .Quanta = ScanQuanta,
                    .VisitedKeys = VisitedKeys,
                    .PhysicalRecords = PhysicalRecords,
                }));
            PassAway();
        }

        void Handle(NPDisk::TEvCheckSpaceResult::TPtr& ev) {
            if (ScanStarted || std::exchange(PDiskReceived, true)) {
                return;
            }
            const auto& result = *ev->Get();
            if (result.Status != NKikimrProto::OK) {
                return CompleteErrorAndDie(result.Status, result.ErrorReason);
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

        void Handle(TEvHugeStripeChunks::TPtr& ev) {
            if (ScanStarted || std::exchange(StripeChunksReceived, true)) {
                return;
            }
            auto& chunks = ev->Get()->StripeChunks;
            StripeChunks = std::unordered_set<TChunkIdx>(
                std::make_move_iterator(chunks.begin()),
                std::make_move_iterator(chunks.end()));
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
            } else if (ChunkKeeperEnabled) {
                SourceErrors.emplace_back(source.ErrorReason
                    ? source.ErrorReason
                    : "ChunkKeeper space counters failed");
            }
            SourceReceived();
        }

        void Handle(TEvSourceTimeout::TPtr&) {
            if (ScanStarted) {
                return;
            }
            if (!PDiskReceived) {
                return CompleteErrorAndDie(NKikimrProto::ERROR, "PDisk space counter timed out");
            }
            if (!HugeReceived && HugeKeeperId) {
                SourceErrors.emplace_back("HugeKeeper space counters timed out");
            }
            if (!StripeChunksReceived && HugeKeeperId) {
                SourceErrors.emplace_back("HugeKeeper stripe chunks timed out");
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
            const ui64 cpuStarted = ThreadCPUTime();
            ++ScanQuanta;
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
            ScanCpuTimeUs += ThreadCPUTime() - cpuStarted;

            // No Hull snapshot or barriers essence survives this event turn.
            snapshot.LogoBlobsSnap.Destroy();
            snapshot.BlocksSnap.Destroy();
            snapshot.BarriersSnap.Destroy();

            if (phaseComplete) {
                Phase = static_cast<EPhase>(static_cast<ui8>(Phase) + 1);
            }
            if (Phase == EPhase::Done) {
                FinalizeAndComplete();
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
            TThis::Send(OwnerId, new TEvents::TEvGone);
            TBase::PassAway();
        }

        STRICT_STFUNC(StateFunc, {
            hFunc(NPDisk::TEvCheckSpaceResult, Handle);
            hFunc(TEvHugeSpaceStatResult, Handle);
            hFunc(TEvHugeStripeChunks, Handle);
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
                TActorId snapshotProviderId,
                TActorId ownerId,
                TActorId hugeKeeperId,
                TActorId syncLogId,
                TActorId chunkKeeperId,
                bool chunkKeeperEnabled,
                ui32 minHugeBlobInBytes,
                ui64 attemptId)
            : HullCtx(std::move(hullCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , SnapshotProviderId(snapshotProviderId)
            , OwnerId(ownerId)
            , HugeKeeperId(hugeKeeperId)
            , SyncLogId(syncLogId)
            , ChunkKeeperId(chunkKeeperId)
            , ChunkKeeperEnabled(chunkKeeperEnabled)
            , MinHugeBlobInBytes(minHugeBlobInBytes)
            , AttemptId(attemptId)
            , ChunkSize(PDiskCtx->Dsk->ChunkSize)
        {}

    private:
        const TIntrusivePtr<THullCtx> HullCtx;
        const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const TPDiskCtxPtr PDiskCtx;
        const TActorId SnapshotProviderId;
        const TActorId OwnerId;
        const TActorId HugeKeeperId;
        const TActorId SyncLogId;
        const TActorId ChunkKeeperId;
        const bool ChunkKeeperEnabled;
        const ui32 MinHugeBlobInBytes;
        const ui64 AttemptId;
        const ui64 ChunkSize;

        TInstant CollectionStartedAt;
        TMonotonic CollectionStartedMonotonic;
        ui64 ScanCpuTimeUs = 0;
        ui64 ScanQuanta = 0;
        ui64 VisitedKeys = 0;
        ui64 PhysicalRecords = 0;

        ui32 AwaitedSources = 0;
        bool PDiskReceived = false;
        bool HugeReceived = false;
        bool StripeChunksReceived = false;
        bool SyncLogReceived = false;
        bool ChunkKeeperReceived = false;
        bool ScanStarted = false;
        bool SnapshotRequested = false;
        ui64 PDiskAllocatedChunks = 0;
        NHuge::THeapSpaceStat HugeSource;
        std::unordered_set<TChunkIdx> StripeChunks;
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
        ui64 HugeStripedBytes = 0;
        TSpaceBreakdown StripedHugeBreakdown;
        std::vector<std::pair<ui32, TComponentState>> ChunkKeeper;
    };

    class TVDiskSpaceReportManager : public TActorBootstrapped<TVDiskSpaceReportManager> {
        using TThis = TVDiskSpaceReportManager;
        using TBase = TActorBootstrapped<TThis>;
        using TCounterGroup = TIntrusivePtr<NMonitoring::TDynamicCounters>;
        using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

        struct TPendingRequest {
            TActorId Recipient;
            ui64 Cookie;
        };

        static constexpr TDuration WatchdogTimeout = TDuration::Minutes(30);
        static constexpr TDuration DisabledControlPollPeriod = TDuration::Minutes(1);
        static constexpr TDuration FailureLogPeriod = TDuration::Minutes(1);

        friend class TActorBootstrapped<TThis>;

        struct TBreakdownCounters {
            const TCounterGroup Group;
#define DECLARE_COUNTER(name) const TCounterPtr name;
            VDISK_SPACE_REPORT_BREAKDOWN_FIELDS(DECLARE_COUNTER)
#undef DECLARE_COUNTER

            explicit TBreakdownCounters(const TCounterGroup& group)
                : Group(group)
#define INITIALIZE_COUNTER(name) , name(Group->GetCounter(#name))
                VDISK_SPACE_REPORT_BREAKDOWN_FIELDS(INITIALIZE_COUNTER)
#undef INITIALIZE_COUNTER
            {}

            void Set(const NKikimrVDisk::TVDiskSpaceBreakdown& value) const {
#define SET_COUNTER(name) name->Set(value.Get##name());
                VDISK_SPACE_REPORT_BREAKDOWN_FIELDS(SET_COUNTER)
#undef SET_COUNTER
            }
        };

        struct TComponentCounters {
            const TCounterGroup Group;
            const TCounterPtr ChunkCount;
            const TCounterPtr AllocatedBytes;
            const TCounterPtr StripedBytes;
            const TCounterPtr AccountedBytes;
            const TBreakdownCounters Breakdown;

            explicit TComponentCounters(TCounterGroup group)
                : Group(std::move(group))
                , ChunkCount(Group->GetCounter("ChunkCount"))
                , AllocatedBytes(Group->GetCounter("AllocatedBytes"))
                , StripedBytes(Group->GetCounter("StripedBytes"))
                , AccountedBytes(Group->GetCounter("AccountedBytes"))
                , Breakdown(Group)
            {}

            TComponentCounters(const TCounterGroup& root, const TString& name)
                : TComponentCounters(root->GetSubgroup("component", name))
            {}

            void Set(const NKikimrVDisk::TVDiskSpaceComponent& value) const {
                ChunkCount->Set(value.GetChunkCount());
                AllocatedBytes->Set(value.GetAllocatedBytes());
                StripedBytes->Set(value.GetStripedBytes());
                AccountedBytes->Set(CalculateAccountedBytes(value.GetBreakdown()));
                Breakdown.Set(value.GetBreakdown());
            }
        };

        struct THugeClassCounters {
            const TCounterPtr SlotsPerChunk;
            const TCounterPtr ChunkCount;
            const TCounterPtr LiveSlotCount;
            const TCounterPtr GcDeadSlotCount;
            const TCounterPtr MergeRedundantSlotCount;
            const TCounterPtr UnclassifiedSlotCount;
            const TBreakdownCounters Breakdown;

            explicit THugeClassCounters(const TCounterGroup& group)
                : SlotsPerChunk(group->GetCounter("SlotsPerChunk"))
                , ChunkCount(group->GetCounter("ChunkCount"))
                , LiveSlotCount(group->GetCounter("LiveSlotCount"))
                , GcDeadSlotCount(group->GetCounter("GcDeadSlotCount"))
                , MergeRedundantSlotCount(group->GetCounter("MergeRedundantSlotCount"))
                , UnclassifiedSlotCount(group->GetCounter("UnclassifiedSlotCount"))
                , Breakdown(group)
            {}

            void Set(const NKikimrVDisk::TVDiskHugeSizeClass& value) const {
                SlotsPerChunk->Set(value.GetSlotsPerChunk());
                ChunkCount->Set(value.GetChunkCount());
                LiveSlotCount->Set(value.GetLiveSlotCount());
                GcDeadSlotCount->Set(value.GetGcDeadSlotCount());
                MergeRedundantSlotCount->Set(value.GetMergeRedundantSlotCount());
                UnclassifiedSlotCount->Set(value.GetUnclassifiedSlotCount());
                Breakdown.Set(value.GetBreakdown());
            }
        };

        struct TChunkKeeperSubsystemCounters {
            const TCounterPtr SubsystemId;
            const TComponentCounters Total;

            explicit TChunkKeeperSubsystemCounters(const TCounterGroup& group)
                : SubsystemId(group->GetCounter("SubsystemId"))
                , Total(group)
            {}

            void Set(const NKikimrVDisk::TVDiskChunkKeeperSpace& value) const {
                SubsystemId->Set(value.GetSubsystemId());
                Total.Set(value.GetTotal());
            }
        };

        struct TStripeHeapCounters {
            const TCounterPtr ChunkCount;
            const TCounterPtr AllocatedBytes;
            const TCounterPtr UsedBytes;
            const TCounterPtr FreeBytes;
            const TCounterPtr LockedFreeBytes;

            explicit TStripeHeapCounters(const TCounterGroup& group)
                : ChunkCount(group->GetCounter("ChunkCount"))
                , AllocatedBytes(group->GetCounter("AllocatedBytes"))
                , UsedBytes(group->GetCounter("UsedBytes"))
                , FreeBytes(group->GetCounter("FreeBytes"))
                , LockedFreeBytes(group->GetCounter("LockedFreeBytes"))
            {}

            void Set(const NKikimrVDisk::TVDiskStripeHeapSpace& value) const {
                ChunkCount->Set(value.GetChunkCount());
                AllocatedBytes->Set(value.GetAllocatedBytes());
                UsedBytes->Set(value.GetUsedBytes());
                FreeBytes->Set(value.GetFreeBytes());
                LockedFreeBytes->Set(value.GetLockedFreeBytes());
            }
        };

        ui64 GetPeriodSeconds() const {
            return Max<i64>(0, static_cast<i64>(PeriodSeconds));
        }

        TDuration CalculateInitialDelay(ui64 periodSeconds) {
            const ui32 periodMs = static_cast<ui32>(periodSeconds * 1000);
            return TDuration::MilliSeconds(JitterRng.Uniform(periodMs));
        }

        TDuration CalculateRecurringDelay(ui64 periodSeconds) {
            const ui32 periodMs = static_cast<ui32>(periodSeconds * 1000);
            const ui32 minDelayMs = periodMs * 3 / 4;
            const ui32 jitterRangeMs = periodMs / 2 + 1;
            return TDuration::MilliSeconds(minDelayMs + JitterRng.Uniform(jitterRangeMs));
        }

        void Bootstrap() {
            TThis::Become(&TThis::StateFunc);
            ScheduleNext(true);
        }

        void ScheduleNext(bool initial) {
            const ui64 generation = ++ScheduleGeneration;
            const ui64 periodSeconds = GetPeriodSeconds();
            if (!periodSeconds) {
                TThis::Schedule(DisabledControlPollPeriod, new TEvPeriodicTick(generation, true));
                return;
            }

            const TDuration delay = initial
                ? CalculateInitialDelay(periodSeconds)
                : CalculateRecurringDelay(periodSeconds);
            TThis::Schedule(delay, new TEvPeriodicTick(generation, false));
        }

        void StartRefresh() {
            if (ActiveWorkerId) {
                return;
            }

            ActiveAttemptId = NextAttemptId++;
            AttemptStarted = TActivationContext::Monotonic();
            auto* worker = new TVDiskSpaceReportActor(
                HullCtx,
                HugeBlobCtx,
                PDiskCtx,
                SkeletonId,
                SelfId(),
                HugeKeeperId,
                SyncLogId,
                ChunkKeeperId,
                ChunkKeeperEnabled,
                MinHugeBlobInBytes,
                ActiveAttemptId);
            ActiveWorkerId = RunInBatchPool(TActivationContext::AsActorContext(), worker);
            RefreshInProgress->Set(1);
            TThis::Schedule(WatchdogTimeout, new TEvWatchdog(ActiveAttemptId));
        }

        void ReplyForcedRequests(const NKikimrVDisk::TGetVDiskSpaceReportResponse& record) {
            for (const TPendingRequest& request : ForcedRequests) {
                auto response = std::make_unique<TEvGetVDiskSpaceReportResponse>(
                    NKikimrProto::OK, TString(), TActivationContext::Now(), nullptr, nullptr);
                response->Record.CopyFrom(record);
                SendVDiskResponse(TActivationContext::AsActorContext(), request.Recipient,
                    response.release(), request.Cookie, HullCtx->VCtx, {});
            }
            ForcedRequests.clear();
        }

        void ReplyForcedRequests(NKikimrProto::EReplyStatus status, const TString& errorReason) {
            NKikimrVDisk::TGetVDiskSpaceReportResponse record;
            record.SetStatus(NKikimrProto::EReplyStatus_Name(status));
            record.SetErrorReason(errorReason);
            ReplyForcedRequests(record);
        }

        void Reply(TEvGetVDiskSpaceReportRequest::TPtr& ev) {
            if (CachedReport) {
                auto response = std::make_unique<TEvGetVDiskSpaceReportResponse>(
                    NKikimrProto::OK, TString(), TActivationContext::Now(), nullptr, nullptr);
                response->Record.MutableReport()->CopyFrom(*CachedReport);
                SendVDiskResponse(TActivationContext::AsActorContext(), ev->Sender, response.release(),
                    ev->Cookie, HullCtx->VCtx, {});
                return;
            }

            ColdCacheRequests->Inc();

            TString errorReason = "VDisk space report cache is not ready; "
                "set ForceRecalculation or wait for periodic refresh";
            if (LastAttemptError) {
                errorReason += ": ";
                errorReason += LastAttemptError;
            }
            auto response = std::make_unique<TEvGetVDiskSpaceReportResponse>(
                NKikimrProto::NOTREADY, errorReason, TActivationContext::Now(), nullptr, nullptr);
            SendVDiskResponse(TActivationContext::AsActorContext(), ev->Sender, response.release(),
                ev->Cookie, HullCtx->VCtx, {});
        }

        template <typename TValueRange, typename TCounterMap, typename TGetLabel>
        void PublishLabeledCounters(
                const char* labelName,
                const TValueRange& values,
                TCounterMap& counterMap,
                TGetLabel getLabel) {
            using TCounters = typename TCounterMap::mapped_type::element_type;

            THashSet<TString> currentLabels;
            for (const auto& value : values) {
                const TString label = ToString(getLabel(value));
                currentLabels.insert(label);
                auto& counters = counterMap[label];
                if (!counters) {
                    counters = std::make_unique<TCounters>(Counters->GetSubgroup(labelName, label));
                }
                counters->Set(value);
            }

            for (auto it = counterMap.begin(); it != counterMap.end();) {
                if (currentLabels.contains(it->first)) {
                    ++it;
                } else {
                    Counters->RemoveSubgroup(labelName, it->first);
                    it = counterMap.erase(it);
                }
            }
        }

        void PublishReport(const NKikimrVDisk::TVDiskSpaceReport& report) {
            ChunkSizeBytes->Set(report.GetChunkSizeBytes());
            PDiskAllocatedChunks->Set(report.GetPDiskAllocatedChunks());
            PDiskAllocatedBytes->Set(report.GetPDiskAllocatedBytes());
            ReportAccountedBytes->Set(report.GetAccountedBytes());
            ReconciliationDeltaBytes->Set(static_cast<TAtomicBase>(report.GetReconciliationDeltaBytes()));

            const i64 delta = report.GetReconciliationDeltaBytes();
            UnaccountedBytes->Set(delta > 0 ? static_cast<ui64>(delta) : 0);
            const ui64 overaccounted = delta < 0
                ? static_cast<ui64>(-(delta + 1)) + 1
                : 0;
            OveraccountedBytes->Set(overaccounted);

            Total.Set(report.GetTotal());
            InplaceBlobs.Set(report.GetLogoBlobs());
            Blocks.Set(report.GetBlocks());
            Barriers.Set(report.GetBarriers());
            HugeBlobs.Set(report.GetHuge().GetTotal());
            HugeFreeReserveChunks->Set(report.GetHuge().GetFreeReserveChunks());
            SyncLog.Set(report.GetSyncLog());
            Unattributed.Set(report.GetUnattributed());
            StripeHeap.Set(report.GetStripeHeap());

            PublishLabeledCounters(
                "slot_size_bytes",
                report.GetHuge().GetSizeClasses(),
                HugeClasses,
                [](const auto& value) { return value.GetSlotSizeBytes(); });
            PublishLabeledCounters(
                "chunk_keeper_subsystem",
                report.GetChunkKeeper(),
                ChunkKeeperSubsystems,
                [](const auto& value) { return value.GetSubsystemId(); });
        }

        void UpdateAttemptMetrics(const TScanMetrics& metrics) {
            LastRefreshDurationMs->Set(metrics.Duration.MilliSeconds());
            LastRefreshCpuTimeUs->Set(metrics.CpuTimeUs);
            LastRefreshQuanta->Set(metrics.Quanta);
            LastRefreshVisitedKeys->Set(metrics.VisitedKeys);
            LastRefreshPhysicalRecords->Set(metrics.PhysicalRecords);
        }

        void RecordFailure(TString errorReason, const TScanMetrics& metrics) {
            LastAttemptError = std::move(errorReason);
            LastAttemptSuccessful->Set(0);
            RefreshFailures->Inc();
            UpdateAttemptMetrics(metrics);

            const TMonotonic now = TActivationContext::Monotonic();
            if (LastFailureLog == TMonotonic::Zero() || now - LastFailureLog >= FailureLogPeriod) {
                LastFailureLog = now;
                YDB_LOG_WARN_CTX_COMP(TActivationContext::AsActorContext(), BS_VDISK_OTHER,
                    "VDisk SpaceReport refresh failed",
                    {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                    {"ErrorReason", LastAttemptError},
                    {"marker", "BSVS47"});
            }
        }

        void FinishAttempt() {
            ActiveWorkerId = {};
            ActiveAttemptId = 0;
            RefreshInProgress->Set(0);
            ScheduleNext(false);
        }

        void Handle(TEvGetVDiskSpaceReportRequest::TPtr& ev) {
            if (ev->Get()->Record.GetForceRecalculation()) {
                ForcedRequests.push_back({ev->Sender, ev->Cookie});
                StartRefresh();
            } else {
                Reply(ev);
            }
        }

        void Handle(TEvPeriodicTick::TPtr& ev) {
            if (ev->Get()->Generation != ScheduleGeneration) {
                return;
            }

            const ui64 periodSeconds = GetPeriodSeconds();
            if (ev->Get()->CheckOnly) {
                ScheduleNext(periodSeconds != 0);
            } else if (!periodSeconds) {
                ScheduleNext(false);
            } else if (ActiveWorkerId) {
                PeriodicTicksSkipped->Inc();
            } else {
                StartRefresh();
            }
        }

        void Handle(TEvScanComplete::TPtr& ev) {
            auto* result = ev->Get();
            if (ev->Sender != ActiveWorkerId || result->AttemptId != ActiveAttemptId) {
                return;
            }

            Y_ABORT_UNLESS(result->Response);
            const auto& record = result->Response->Record;
            const bool success = record.GetStatus() == NKikimrProto::EReplyStatus_Name(NKikimrProto::OK)
                && record.HasReport();
            if (success) {
                CachedReport = std::make_unique<NKikimrVDisk::TVDiskSpaceReport>();
                CachedReport->CopyFrom(record.GetReport());
                PublishReport(record.GetReport());
                LastAttemptError.clear();
                LastAttemptSuccessful->Set(1);
                RefreshSuccesses->Inc();
                UpdateAttemptMetrics(result->Metrics);
            } else {
                RecordFailure(
                    record.GetErrorReason().empty()
                        ? TString("SpaceReport worker returned no report")
                        : TString(record.GetErrorReason()),
                    result->Metrics);
            }
            ReplyForcedRequests(record);
            FinishAttempt();
        }

        void Handle(TEvWatchdog::TPtr& ev) {
            if (!ActiveWorkerId || ev->Get()->AttemptId != ActiveAttemptId) {
                return;
            }

            const TString errorReason = "SpaceReport refresh exceeded the 30 minute watchdog";
            TThis::Send(ActiveWorkerId, new TEvents::TEvPoisonPill);
            RecordFailure(
                errorReason,
                {.Duration = TActivationContext::Monotonic() - AttemptStarted});
            ReplyForcedRequests(NKikimrProto::ERROR, errorReason);
            // Keep the worker as active until TEvGone arrives. Poison cannot
            // interrupt an activation, so clearing it here could let a second
            // scan overlap a worker that is still unwinding.
            ActiveAttemptId = 0;
            ++ScheduleGeneration;
        }

        void Handle(TEvents::TEvGone::TPtr& ev) {
            if (ev->Sender != ActiveWorkerId) {
                return;
            }

            if (!ActiveAttemptId) {
                ActiveWorkerId = {};
                RefreshInProgress->Set(0);
                if (ForcedRequests.empty()) {
                    ScheduleNext(false);
                } else {
                    StartRefresh();
                }
                return;
            }

            const TString errorReason = "SpaceReport worker terminated without a completion event";
            RecordFailure(
                errorReason,
                {.Duration = TActivationContext::Monotonic() - AttemptStarted});
            ReplyForcedRequests(NKikimrProto::ERROR, errorReason);
            FinishAttempt();
        }

        void Handle(TEvMinHugeBlobSizeUpdate::TPtr& ev) {
            MinHugeBlobInBytes = ev->Get()->MinHugeBlobInBytes;
        }

        void HandlePoison() {
            if (ActiveWorkerId) {
                TThis::Send(ActiveWorkerId, new TEvents::TEvPoisonPill);
                ActiveWorkerId = {};
            }
            ReplyForcedRequests(NKikimrProto::ERROR, "VDisk space report manager stopped");
            PassAway();
        }

        void PassAway() override {
            TThis::Send(SkeletonId, new TEvents::TEvGone);
            TBase::PassAway();
        }

        STRICT_STFUNC(StateFunc, {
            hFunc(TEvGetVDiskSpaceReportRequest, Handle);
            hFunc(TEvPeriodicTick, Handle);
            hFunc(TEvScanComplete, Handle);
            hFunc(TEvWatchdog, Handle);
            hFunc(TEvents::TEvGone, Handle);
            hFunc(TEvMinHugeBlobSizeUpdate, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
        })

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TVDiskSpaceReportManager(
                TIntrusivePtr<THullCtx> hullCtx,
                std::shared_ptr<THugeBlobCtx> hugeBlobCtx,
                TPDiskCtxPtr pdiskCtx,
                TActorId skeletonId,
                TActorId hugeKeeperId,
                TActorId syncLogId,
                TActorId chunkKeeperId,
                bool chunkKeeperEnabled,
                ui32 minHugeBlobInBytes,
                TControlWrapper periodSeconds,
                ui32 pdiskId,
                ui32 vdiskSlotId)
            : HullCtx(std::move(hullCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , SkeletonId(skeletonId)
            , HugeKeeperId(hugeKeeperId)
            , SyncLogId(syncLogId)
            , ChunkKeeperId(chunkKeeperId)
            , ChunkKeeperEnabled(chunkKeeperEnabled)
            , MinHugeBlobInBytes(minHugeBlobInBytes)
            , PeriodSeconds(std::move(periodSeconds))
            , JitterRng(MultiHash(SkeletonId.NodeId(), pdiskId, vdiskSlotId))
            , Counters(HullCtx->VCtx->VDiskCounters->GetSubgroup("subsystem", "vdisk_space_report"))
            , ChunkSizeBytes(Counters->GetCounter("ChunkSizeBytes"))
            , PDiskAllocatedChunks(Counters->GetCounter("PDiskAllocatedChunks"))
            , PDiskAllocatedBytes(Counters->GetCounter("PDiskAllocatedBytes"))
            , ReportAccountedBytes(Counters->GetCounter("AccountedBytes"))
            , ReconciliationDeltaBytes(Counters->GetCounter("ReconciliationDeltaBytes"))
            , UnaccountedBytes(Counters->GetCounter("UnaccountedBytes"))
            , OveraccountedBytes(Counters->GetCounter("OveraccountedBytes"))
            , Total(Counters->GetSubgroup("scope", "total"))
            , InplaceBlobs(Counters, "inplace_blobs")
            , Blocks(Counters, "blocks")
            , Barriers(Counters, "barriers")
            , HugeBlobs(Counters, "huge_blobs")
            , HugeFreeReserveChunks(HugeBlobs.Group->GetCounter("FreeReserveChunks"))
            , SyncLog(Counters, "sync_log")
            , Unattributed(Counters, "unattributed")
            , StripeHeap(Counters->GetSubgroup("scope", "stripe_heap"))
            , RefreshInProgress(Counters->GetCounter("RefreshInProgress"))
            , LastAttemptSuccessful(Counters->GetCounter("LastAttemptSuccessful"))
            , LastRefreshDurationMs(Counters->GetCounter("LastRefreshDurationMs"))
            , LastRefreshCpuTimeUs(Counters->GetCounter("LastRefreshCpuTimeUs"))
            , LastRefreshQuanta(Counters->GetCounter("LastRefreshQuanta"))
            , LastRefreshVisitedKeys(Counters->GetCounter("LastRefreshVisitedKeys"))
            , LastRefreshPhysicalRecords(Counters->GetCounter("LastRefreshPhysicalRecords"))
            , RefreshSuccesses(Counters->GetCounter("RefreshSuccesses", true))
            , RefreshFailures(Counters->GetCounter("RefreshFailures", true))
            , PeriodicTicksSkipped(Counters->GetCounter("PeriodicTicksSkipped", true))
            , ColdCacheRequests(Counters->GetCounter("ColdCacheRequests", true))
        {
            RefreshInProgress->Set(0);
            LastAttemptSuccessful->Set(0);
        }

    private:
        const TIntrusivePtr<THullCtx> HullCtx;
        const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const TPDiskCtxPtr PDiskCtx;
        const TActorId SkeletonId;
        const TActorId HugeKeeperId;
        const TActorId SyncLogId;
        const TActorId ChunkKeeperId;
        const bool ChunkKeeperEnabled;
        ui32 MinHugeBlobInBytes;
        const TControlWrapper PeriodSeconds;
        TReallyFastRng32 JitterRng;

        const TCounterGroup Counters;
        const TCounterPtr ChunkSizeBytes;
        const TCounterPtr PDiskAllocatedChunks;
        const TCounterPtr PDiskAllocatedBytes;
        const TCounterPtr ReportAccountedBytes;
        const TCounterPtr ReconciliationDeltaBytes;
        const TCounterPtr UnaccountedBytes;
        const TCounterPtr OveraccountedBytes;
        const TBreakdownCounters Total;
        const TComponentCounters InplaceBlobs;
        const TComponentCounters Blocks;
        const TComponentCounters Barriers;
        const TComponentCounters HugeBlobs;
        const TCounterPtr HugeFreeReserveChunks;
        const TComponentCounters SyncLog;
        const TComponentCounters Unattributed;
        const TStripeHeapCounters StripeHeap;
        const TCounterPtr RefreshInProgress;
        const TCounterPtr LastAttemptSuccessful;
        const TCounterPtr LastRefreshDurationMs;
        const TCounterPtr LastRefreshCpuTimeUs;
        const TCounterPtr LastRefreshQuanta;
        const TCounterPtr LastRefreshVisitedKeys;
        const TCounterPtr LastRefreshPhysicalRecords;
        const TCounterPtr RefreshSuccesses;
        const TCounterPtr RefreshFailures;
        const TCounterPtr PeriodicTicksSkipped;
        const TCounterPtr ColdCacheRequests;

        std::unique_ptr<NKikimrVDisk::TVDiskSpaceReport> CachedReport;
        std::vector<TPendingRequest> ForcedRequests;
        TString LastAttemptError;
        TActorId ActiveWorkerId;
        ui64 ActiveAttemptId = 0;
        ui64 NextAttemptId = 1;
        TMonotonic AttemptStarted = TMonotonic::Zero();
        TMonotonic LastFailureLog = TMonotonic::Zero();
        ui64 ScheduleGeneration = 0;
        std::unordered_map<TString, std::unique_ptr<THugeClassCounters>> HugeClasses;
        std::unordered_map<TString, std::unique_ptr<TChunkKeeperSubsystemCounters>> ChunkKeeperSubsystems;
    };

#undef VDISK_SPACE_REPORT_BREAKDOWN_FIELDS

} // anonymous namespace

    IActor* CreateVDiskSpaceReportManager(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const std::shared_ptr<THugeBlobCtx>& hugeBlobCtx,
            const TPDiskCtxPtr& pdiskCtx,
            const TActorId& skeletonId,
            const TActorId& hugeKeeperId,
            const TActorId& syncLogId,
            const TActorId& chunkKeeperId,
            bool chunkKeeperEnabled,
            ui32 minHugeBlobInBytes,
            TControlWrapper periodSeconds,
            ui32 pdiskId,
            ui32 vdiskSlotId)
    {
        return new TVDiskSpaceReportManager(
            hullCtx,
            hugeBlobCtx,
            pdiskCtx,
            skeletonId,
            hugeKeeperId,
            syncLogId,
            chunkKeeperId,
            chunkKeeperEnabled,
            minHugeBlobInBytes,
            std::move(periodSeconds),
            pdiskId,
            vdiskSlotId);
    }

} // namespace NKikimr
