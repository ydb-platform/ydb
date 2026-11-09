#pragma once
#include "ddisk_actor.h"
#include "direct_io_op.h"
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/library/actors/async/cancellation.h>
#include <ydb/library/actors/core/subsystems/async_frame_cache.h>
#include <util/system/event.h>

namespace NKikimr::NDDisk {
class TDDiskActorTestPeer {
public:
    static void LaunchIntegrity(TDDiskActor& actor, std::function<NActors::async<void>()> factory) {
        co_await factory();
        if (!actor.Stopping && !actor.IsBroken()) {
            actor.PrepareIntegrityReclamation();
        }
    }

    static bool ReadCredentialsUnchanged(TDDiskActor& actor, TQueryCredentials credentials) {
        TEvRead::TPtr request = reinterpret_cast<TEventHandle<TEvRead>*>(
            new IEventHandle(actor.SelfId(), actor.SelfId(),
                new TEvRead(credentials, {0, 0, IntegrityUnitSize}, {true})));
        const auto original = request->Get()->Record.SerializeAsString();
        TQueryCredentials resolved;
        return actor.CheckQuery(*request, nullptr, resolved)
            && resolved.TabletId == credentials.TabletId
            && request->Get()->Record.SerializeAsString() == original;
    }

    static void PrintReadFootprint() {
        Cerr << "DDisk read sizes: payload=" << sizeof(TReadPayload)
            << " checksums=" << sizeof(TReadChecksums)
            << " result=" << sizeof(TDDiskActor::TDDiskReadResult)
            << " completion=" << sizeof(TDDiskActor::TIoCompletion)
            << " batch=" << sizeof(TDDiskActor::TBatchedIOAwaiter) << Endl;
    }

    static ui64 ChecksumMismatches(const TDDiskActor& actor) {
        return actor.Counters.Checksums.ChecksumMismatch->Val();
    }

    static void BeginStopping(TDDiskActor& actor) {
        actor.BeginStopping("controlled inline interruption");
    }

    static auto ChunkMapSnapshot(TDDiskActor& actor) {
        return actor.CreateChunkMapSnapshot();
    }

    static bool AllocationLogIssued(const TDDiskActor& actor, ui64 tabletId, ui64 vChunkIndex) {
        const auto allocation = actor.DataChunkAllocationsInFlight.find({tabletId, vChunkIndex});
        return allocation != actor.DataChunkAllocationsInFlight.end() && allocation->second.LogIssued;
    }

    static TChunkIdx PublishedChunk(const TDDiskActor& actor, ui64 tabletId, ui64 vChunkIndex) {
        const auto tablet = actor.Tablets.find(tabletId);
        if (tablet == actor.Tablets.end()) {
            return 0;
        }
        const auto chunk = tablet->second.ChunkRefs.find(vChunkIndex);
        return chunk != tablet->second.ChunkRefs.end() ? chunk->second.ChunkIdx : 0;
    }

    static size_t ReservedChunks(const TDDiskActor& actor) {
        return actor.ChunkManager.GetReservedChunkCount();
    }

    static bool IoCountersBalanced(const TDDiskActor& actor) {
        const auto& io = actor.Counters.DirectIO;
        return !actor.GetDirectIoInflight() && !io.RunningCount->Val()
            && !io.Read.RequestsInFlight->Val() && !io.Read.BytesInFlight->Val()
            && !io.Write.RequestsInFlight->Val() && !io.Write.BytesInFlight->Val();
    }

    // Live client/metadata/format runners and aggregates which keep their own records.
    static size_t RequestWaiters(const TDDiskActor& actor) {
        return actor.DataRequestsInFlight + actor.SyncsInFlight.size()
            + actor.TabletChunkDeletionReplies.size();
    }

    // Actor-work guards, including metadata and format runners.
    static size_t DataRequests(const TDDiskActor& actor) {
        return actor.DataRequestsInFlight;
    }

    // Logical reads TIntegrityManager still owes a checksum result to, including those
    // joined to metadata reads started by another reader.
    static size_t PendingReads(const TDDiskActor& actor) {
        return actor.IntegrityManager ? actor.IntegrityManager->PendingReadCount() : 0;
    }

    static size_t CachedIntegrityImages(const TDDiskActor& actor) {
        return actor.IntegrityManager ? actor.IntegrityManager->CachedBlockStates() : 0;
    }


    static size_t PendingSyncs(const TDDiskActor& actor) {
        return actor.SyncsInFlight.size();
    }

    static TIntegrityManager::TWriteOperation HoldIntegrityPair(TDDiskActor& actor,
            ui64 tabletId, ui64 vChunkIndex, ui32 offset)
    {
        return actor.IntegrityManager->PrepareWrite({tabletId, vChunkIndex}, offset, IntegrityUnitSize);
    }

#if defined(__linux__)
    // Observable outcome of one TBatchedIOAwaiter, as the data path uses it.
    struct TBatchProbe {
        // Set once the frame has resumed from the batch wait.
        bool Resumed = false;
        // Set when the frame left its cancellation scope and retired.
        bool Finished = false;
        // Observes callback ownership without prolonging its lifetime.
        std::weak_ptr<void> Callback;
        size_t MetadataCapacity = 0;
        std::vector<NKikimrBlobStorage::NDDisk::TReplyStatus::E> Statuses;
        std::vector<TReadPayload> Data;
    };

    struct TBatchCompletionGate {
        TManualEvent Reached;
        TManualEvent Continue;
        bool LastCompletion = false;
        bool TookBridge = false;
        long OwnersAfterWait = 0;
        ui32 OperationDestructions = 0;
    };

private:
    // Uses the production completion phases while allowing the test to stop between
    // them. The ordinary direct-I/O completion guard keeps the actor alive throughout.
    class TPausedBatchIoOp final : public TDDiskActor::TDirectIoOpBase {
    public:
        TPausedBatchIoOp(TDDiskActor& actor, std::shared_ptr<TDDiskActor::TBatchedIOAwaiter> batch,
                TBatchCompletionGate& gate, bool takeBeforeWait)
            : TDirectIoOpBase(actor)
            , Batch(std::move(batch))
            , Gate(gate)
            , TakeBeforeWait(takeBeforeWait)
        {
            PrepareRead(IntegrityUnitSize, 0, 100, 0);
        }

        ~TPausedBatchIoOp() override {
            ++Gate.OperationDestructions;
        }

        void Reply(NActors::TActorSystem*, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                TString reason) noexcept override
        {
            TDDiskActor::TIoCompletion completion;
            completion.Status = status;
            completion.ErrorMessage = std::move(reason);
            completion.Data = ExtractReadPayload();
            Gate.LastCompletion = Batch->RecordCompletion(std::move(completion), 0);
            std::coroutine_handle<> bridge;
            if (Gate.LastCompletion && TakeBeforeWait) {
                bridge = Batch->TakeBridge();
            }
            Gate.Reached.Signal();
            Gate.Continue.WaitI();
            Gate.OwnersAfterWait = Batch.use_count();
            if (Gate.LastCompletion && !TakeBeforeWait) {
                bridge = Batch->TakeBridge();
            }
            Gate.TookBridge = bool(bridge);
            if (bridge) {
                bridge.resume();
            }
        }

    private:
        std::shared_ptr<TDDiskActor::TBatchedIOAwaiter> Batch;
        TBatchCompletionGate& Gate;
        const bool TakeBeforeWait;
    };

public:
    static void SubmitPausedMetadataReadBatch(TDDiskActor& actor, TBatchProbe& probe,
            TBatchCompletionGate& gate, bool takeBeforeWait)
    {
        LaunchIntegrity(actor, [&actor, &probe, &gate, takeBeforeWait]() -> NActors::async<void> {
            auto batch = std::make_shared<TDDiskActor::TBatchedIOAwaiter>(actor);
            probe.Callback = batch;
            batch->MetadataResults.resize(1);
            // The test operation owns this count; Wait still installs the real library
            // bridge and submission guard. The scripted router completes it later.
            batch->Pending.fetch_add(1);
            std::unique_ptr<TDDiskActor::TDirectIoOpBase> op =
                std::make_unique<TPausedBatchIoOp>(actor, batch, gate, takeBeforeWait);
            actor.DirectUringOp(op);
            co_await batch->Wait();
            probe.Statuses.push_back(batch->MetadataResults[0].Status);
            probe.Data.push_back(std::move(batch->MetadataResults[0].Data));
            probe.Resumed = true;
            probe.Finished = true;
        });
    }

    // Mirrors the metadata batch of a cold read: one critical operation per descriptor, a single
    // resume for the whole group, and results readable only after every operation is back.
    // Optionally wraps the wait in a cancellation scope so callers can assert that an
    // accepted batch keeps its buffers until the device is done with them.
    static void SubmitMetadataReadBatch(TDDiskActor& actor, size_t count, TBatchProbe& probe,
            NActors::TAsyncCancellationScope& scope, bool cancelBeforeWait = false, bool usePool = false)
    {
        probe.Statuses.assign(count, NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN);
        probe.Data.resize(count);
        LaunchIntegrity(actor, [&actor, count, &probe, &scope, cancelBeforeWait, usePool]()
                -> NActors::async<void> {
            co_await scope.Wrap([&]() -> NActors::async<void> {
                auto batch = usePool ? actor.AllocateBatchedIOAwaiter()
                    : std::make_shared<TDDiskActor::TBatchedIOAwaiter>(actor);
                probe.Callback = batch;
                for (size_t i = 0; i < count; ++i) {
                    const TIntegrityManager::TMetadataRead read{i + 1, 100,
                        ui32(i * IntegrityUnitSize), IntegrityUnitSize};
                    batch->Add(actor.PrepareMetadataRead(*batch, read));
                }
                probe.MetadataCapacity = batch->MetadataResults.capacity();
                if (cancelBeforeWait) {
                    scope.Cancel();
                }
                co_await batch->Wait();
                for (size_t i = 0; i < count; ++i) {
                    probe.Statuses[i] = batch->MetadataResults[i].Status;
                    probe.Data[i] = std::move(batch->MetadataResults[i].Data);
                }
                probe.Resumed = true;
                if (usePool) {
                    actor.ReturnBatchedIOAwaiter(std::move(batch));
                }
            });
            probe.Finished = true;
        });
    }

    // Exercise concurrent batch callbacks without recycling operations through the
    // single-producer DDisk I/O pool. The returned callback owns the batch.
    static auto WaitForMetadataBatchCallbacks(TDDiskActor& actor, size_t count, TBatchProbe& probe) {
        auto batch = std::make_shared<TDDiskActor::TBatchedIOAwaiter>(actor);
        probe.Callback = batch;
        batch->MetadataResults.resize(count);
        // The callbacks come from the test instead of device operations.
        batch->Pending.fetch_add(static_cast<ui32>(count));
        LaunchIntegrity(actor, [batch, &probe]() -> NActors::async<void> {
            co_await batch->Wait();
            for (auto& result : batch->MetadataResults) {
                probe.Statuses.push_back(result.Status);
                probe.Data.push_back(std::move(result.Data));
            }
            probe.Resumed = true;
            probe.Finished = true;
        });
        return [batch = std::move(batch)](size_t index, TRcBuf data) {
            TDDiskActor::TIoCompletion completion;
            completion.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            completion.Data = TReadPayload(std::move(data));
            batch->OnComplete(std::move(completion), index);
        };
    }

    // Keep the first awaiter alive after its continuation starts a second wait on the
    // same batch, so the test can destroy it while the second awaiter is active.
    static auto WaitForReusedMetadataBatchCallbacks(TDDiskActor& actor, TBatchProbe& probe,
            std::unique_ptr<TDDiskActor::TBatchedIOAwaiter::TWaiter>& firstWaiter)
    {
        auto batch = std::make_shared<TDDiskActor::TBatchedIOAwaiter>(actor);
        probe.Callback = batch;
        batch->MetadataResults.resize(1);
        batch->Pending.fetch_add(1);
        LaunchIntegrity(actor, [batch, &probe, &firstWaiter]() -> NActors::async<void> {
            firstWaiter = std::make_unique<TDDiskActor::TBatchedIOAwaiter::TWaiter>(*batch);
            co_await *firstWaiter;
            probe.Resumed = true;
            batch->ClearForReuse();
            batch->MetadataResults.resize(1);
            batch->Pending.fetch_add(1);
            co_await batch->Wait();
            probe.Statuses.push_back(batch->MetadataResults[0].Status);
            probe.Finished = true;
        });
        return [batch = std::move(batch)] {
            TDDiskActor::TIoCompletion completion;
            completion.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            batch->OnComplete(std::move(completion), 0);
        };
    }

    // Metadata reads are submitted as critical I/O so they share the integrity retry
    // and fail-stop policy; client data I/O is not.
    static bool IsCriticalIo(const NPDisk::TUringOperationBase& op) {
        return static_cast<const TDDiskActor::TDirectIoOpBase&>(op).IsCriticalDDiskIo();
    }

    static bool UsesRouter(const TDDiskActor& actor) {
        return bool(actor.UringRouter);
    }
#endif
    static void EnterBroken(TDDiskActor& actor, TString reason) {
        NActors::TActorRunnableQueue queue(&actor);
        actor.EnterBroken(std::move(reason));
    }

    static bool IsShutdownDrained(const TDDiskActor& actor) {
        return actor.OwnDrainComplete && actor.PersistentBufferGone;
    }
    static bool IsBroken(const TDDiskActor& actor) { return actor.IsBroken(); }
    static bool IsAllocationPending(const TDDiskActor& actor, ui64 tabletId, ui64 vChunkIndex) {
        return actor.Tablets.at(tabletId).ChunkRefs.at(vChunkIndex).AllocationPending;
    }
    // Called in the actor's mailbox, after setup writes have completed.
    static bool ReservationsSettled(const TDDiskActor& actor) {
        return actor.LogReplayComplete && !actor.ChunkManager.IsReservationInFlight()
            && actor.FormattingChunks.empty() && !actor.ChunkManager.HasPendingAllocations()
            && actor.DataChunkAllocationsInFlight.empty()
            && !actor.IssuePersistentBufferChunkAllocationInflight
            && actor.PersistentBufferChunks.size() >= actor.PersistentBufferFormat.InitChunks;
    }
    static bool RequestFollowupsQuiescent(const TDDiskActor& actor, ui64 tabletId) {
        if (actor.Stopping || actor.IsBroken()
                || !ReservationsSettled(actor) || !IoCountersBalanced(actor)
                || actor.ReservationCookie
                || !actor.PendingChunkRelease.empty()
                || !actor.LogWaiters.empty()
                || !actor.WriteCallbacks.empty() || !actor.ReadCallbacks.empty()
                || actor.DataRequestsInFlight
                || !actor.DelayedRetries.empty()
                || !actor.SyncsInFlight.empty()
                || !actor.SyncSourceCookies.empty()
                || (actor.IntegrityManager
                    && actor.IntegrityManager->HasInFlightOperationsForTablet(tabletId))) {
            return false;
        }
        const auto tablet = actor.Tablets.find(tabletId);
        if (tablet != actor.Tablets.end()) {
            for (const auto& [_, chunk] : tablet->second.ChunkRefs) {
                if (chunk.AllocationPending || chunk.ChunkRefPins) {
                    return false;
                }
            }
        }
        return true;
    }

    static void SetDestructionClock(TDDiskActor& actor,
            std::function<TMonotonic()> now, std::function<void()> sleep) {
        actor.DestructionNow = std::move(now);
        actor.DestructionSleep = std::move(sleep);
    }
};
}
