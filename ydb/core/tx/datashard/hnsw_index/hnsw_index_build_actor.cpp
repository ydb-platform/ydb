#include "hnsw_index_build_actor.h"
#include <ydb/core/tx/datashard/datashard_impl.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/thread/pool.h>
#include <mutex>

namespace NKikimr::NDataShard {

    using namespace NActors;

    namespace {
        TThreadPool& HnswDeletePool() {
            static TThreadPool pool(TThreadPoolParams().SetThreadName("HnswDelete"));
            static const bool started = [&] { pool.Start(1); return true; }();
            Y_UNUSED(started);
            return pool;
        }

        std::shared_ptr<THnswIndex> OwnHnswGraph(std::unique_ptr<THnswIndex> index,
                                               std::shared_ptr<void> reservation) {
            struct TOwnedGraph {
                std::shared_ptr<void> Reservation;
                std::unique_ptr<THnswIndex> Index;
            };
            auto* graph = new TOwnedGraph{std::move(reservation), std::move(index)};
            std::shared_ptr<TOwnedGraph> owner(graph, [](TOwnedGraph* retired) noexcept {
                try {
                    // The queued memory remains charged until destruction finishes.
                    if (HnswDeletePool().AddFunc([retired] { delete retired; })) { return; }
                } catch (...) {
                    // Allocation failure or shutdown must still release the graph.
                }
                delete retired;
            });
            return std::shared_ptr<THnswIndex>(std::move(owner), graph->Index.get());
        }

        // One bounded CPU pool per process (and therefore at most two builders
        // per node). Scans, compactions and tablet mailboxes never run CreateIndex.
        TThreadPool& HnswBuildPool() {
            HnswDeletePool();
            static TThreadPool pool(TThreadPoolParams().SetThreadName("HnswBuild"));
            static const bool started = [&] { pool.Start(2, 16); return true; }();
            Y_UNUSED(started);
            return pool;
        }

        struct TEvBuildDone : TEventLocal<TEvBuildDone, EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 100> {
            THnswIndexBuildResult Result;
            explicit TEvBuildDone(THnswIndexBuildResult&& result) : Result(std::move(result)) {}
        };

        struct TBuildCompletionTarget {
            std::mutex Mutex;
            TActorSystem* System = nullptr;
            TActorId Actor;
            bool Alive = true;

            void Close() {
                std::lock_guard guard(Mutex);
                Alive = false;
            }
            void Send(THnswIndexBuildResult&& result) {
                std::lock_guard guard(Mutex);
                if (Alive) { System->Send(new IEventHandle(Actor, TActorId(), new TEvBuildDone(std::move(result)))); }
            }
        };
    }

    class THnswIndexBuildWorker : public TActorBootstrapped<THnswIndexBuildWorker>, public IActorExceptionHandler {
    public:
        THnswIndexBuildWorker(const Ydb::Table::VectorIndexSettings& settings,
            std::vector<std::pair<TString, TString>> rows, std::shared_ptr<void> reservation,
            ui64 budget, THnswIndexBuildCallback callback, bool allowEmpty,
            std::shared_ptr<std::atomic<bool>> cancelled, std::function<void(ui64)> reduceReservation)
            : TActorBootstrapped(NKikimrServices::TActivity::DATASHARD_READ_SCAN)
            , Settings(settings), Rows(std::move(rows)), Reservation(std::move(reservation))
            , Budget(budget), Callback(std::move(callback)), AllowEmpty(allowEmpty)
            , Cancelled(cancelled ? std::move(cancelled) : std::make_shared<std::atomic<bool>>(false))
            , ReduceReservation(std::move(reduceReservation))
        {}

        ~THnswIndexBuildWorker() {
            Cancelled->store(true, std::memory_order_relaxed);
            if (Target) { Target->Close(); }
        }

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateWork);
            Target = std::make_shared<TBuildCompletionTarget>();
            Target->System = ctx.ActorSystem();
            Target->Actor = SelfId();
            const bool added = HnswBuildPool().AddFunc([
                target = std::weak_ptr<TBuildCompletionTarget>(Target), cancelled = Cancelled,
                settings = Settings, rows = std::move(Rows), reservation = std::move(Reservation),
                budget = Budget, allowEmpty = AllowEmpty, reduce = std::move(ReduceReservation)]() mutable {
                THnswIndexBuildResult result;
                try {
                    auto index = THnswIndex::Build(settings, rows, budget, result.Error, allowEmpty, cancelled.get());
                    rows.clear();
                    rows.shrink_to_fit();
                    if (index && !cancelled->load(std::memory_order_relaxed)) {
                        if (reduce) { reduce(index->EstimatedMemoryBytes()); }
                        result.MemoryReservation = reservation;
                        result.Index = OwnHnswGraph(std::move(index), std::move(reservation));
                    }
                } catch (const std::exception& error) {
                    result.Error = error.what();
                } catch (...) {
                    result.Error = "Unknown exception while building HNSW graph";
                }
                if (auto notifier = target.lock()) { notifier->Send(std::move(result)); }
            });
            if (!added) {
                THnswIndexBuildResult result;
                result.Error = "HNSW node build queue is full";
                Finish(std::move(result), ctx);
            }
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBuildDone, Handle);
                cFunc(TEvents::TEvPoison::EventType, Cancel);
            }
        }

        bool OnUnhandledException(const std::exception& error) override {
            const auto& ctx = TActivationContext::AsActorContext();
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "HNSW build actor " << SelfId() << ": " << error.what());
            THnswIndexBuildResult result;
            result.Error = error.what();
            Finish(std::move(result), ctx);
            return true;
        }

    private:
        void Handle(TEvBuildDone::TPtr& ev) { Finish(std::move(ev->Get()->Result), TActivationContext::AsActorContext()); }
        void Cancel() {
            Cancelled->store(true, std::memory_order_relaxed);
            THnswIndexBuildResult result;
            result.Error = "HNSW build cancelled";
            Finish(std::move(result), TActivationContext::AsActorContext());
        }
        void Finish(THnswIndexBuildResult&& result, const TActorContext& ctx) {
            if (!Completed) {
                Completed = true;
                Callback(std::move(result), ctx);
            }
            PassAway();
        }

        Ydb::Table::VectorIndexSettings Settings;
        std::vector<std::pair<TString, TString>> Rows;
        std::shared_ptr<void> Reservation;
        ui64 Budget;
        THnswIndexBuildCallback Callback;
        bool AllowEmpty;
        std::shared_ptr<std::atomic<bool>> Cancelled;
        std::function<void(ui64)> ReduceReservation;
        std::shared_ptr<TBuildCompletionTarget> Target;
        bool Completed = false;
    };

    IActor* CreateHnswIndexBuildWorker(const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> rows, std::shared_ptr<void> reservation,
        ui64 budget, THnswIndexBuildCallback callback, bool allowEmpty,
        std::shared_ptr<std::atomic<bool>> cancelled, std::function<void(ui64)> reduceReservation) {
        return new THnswIndexBuildWorker(settings, std::move(rows), std::move(reservation), budget,
            std::move(callback), allowEmpty, std::move(cancelled), std::move(reduceReservation));
    }

    // Retains the existing friendship with TDataShard while adapting the common
    // worker result to DataShard's private lazy-build event.
    class THnswIndexBuildActor {
    public:
        static IActor* Create(const TActorId& replyTo, ui32 localTid, ui32 vectorColumnTag,
                              ui64 rowCountAtBuild,
                              const Ydb::Table::VectorIndexSettings& settings,
                              std::vector<std::pair<TString, TString>> keysAndVectors,
                              std::shared_ptr<void> memoryReservation,
                              ui64 maxMemoryBytes, TRowVersion baseVersion, ui64 buildToken, bool allowEmpty, TPathId indexId, ui64 memoryOwner,
                              std::shared_ptr<std::atomic<bool>> cancelled, std::function<void(ui64)> reduceReservation) {
            return CreateHnswIndexBuildWorker(settings, std::move(keysAndVectors),
                                              std::move(memoryReservation), maxMemoryBytes,
                                              [replyTo, localTid, indexId, memoryOwner, vectorColumnTag, rowCountAtBuild, settings, baseVersion, buildToken](THnswIndexBuildResult&& buildResult, const TActorContext& ctx) mutable {
                                                  auto result = MakeHolder<TDataShard::TEvPrivate::TEvHnswIndexBuildResult>();
                                                  result->LocalTid = localTid;
                                                  result->IndexId = indexId;
                                                  result->VectorColumnTag = vectorColumnTag;
                                                  result->RowCountAtBuild = rowCountAtBuild;
                                                  result->Settings = settings;
                                                  result->BaseVersion = baseVersion;
                                                  result->BuildToken = buildToken;
                                                  result->MemoryOwner = memoryOwner;
                                                  result->Index = std::move(buildResult.Index);
                                                  result->MemoryReservation = std::move(buildResult.MemoryReservation);
                                                  result->Error = std::move(buildResult.Error);
                                                  ctx.Send(replyTo, result.Release());
                                              }, allowEmpty, std::move(cancelled), std::move(reduceReservation));
        }
    };

    IActor* CreateHnswIndexBuildActor(const TActorId& replyTo, ui32 localTid, ui32 vectorColumnTag,
                                      ui64 rowCountAtBuild,
                                      const Ydb::Table::VectorIndexSettings& settings,
                                      std::vector<std::pair<TString, TString>> keysAndVectors,
                                      std::shared_ptr<void> memoryReservation,
                                      ui64 maxMemoryBytes, TRowVersion baseVersion, ui64 buildToken, bool allowEmpty, TPathId indexId, ui64 memoryOwner,
                              std::shared_ptr<std::atomic<bool>> cancelled, std::function<void(ui64)> reduceReservation) {
        return THnswIndexBuildActor::Create(replyTo, localTid, vectorColumnTag,
                                            rowCountAtBuild, settings, std::move(keysAndVectors),
                                            std::move(memoryReservation), maxMemoryBytes, baseVersion, buildToken, allowEmpty, indexId, memoryOwner, std::move(cancelled), std::move(reduceReservation));
    }

    void TDataShard::Handle(TEvPrivate::TEvHnswSnapshotFinished::TPtr& ev, const TActorContext& ctx) {
        auto& scanned = *ev->Get();
        auto* entry = FindHnswIndexEntry(scanned.LocalTid, scanned.IndexId);
        const bool current = IsHnswBuildCurrent(scanned.LocalTid, scanned.BuildToken, scanned.IndexId);
        if (entry && entry->BuildToken == scanned.BuildToken) { entry->ScanId = 0; }
        if (!current || IsFollower() || IsStopping() || State != TShardState::Ready
                || !AppData()->FeatureFlags.GetEnableHnswIndex() || !scanned.Result.Success) {
            auto result = MakeHolder<TEvPrivate::TEvHnswIndexBuildResult>();
            result->LocalTid = scanned.LocalTid;
            result->IndexId = scanned.IndexId;
            result->BuildToken = scanned.BuildToken;
            result->MemoryOwner = scanned.MemoryOwner;
            result->BelowMinRows = scanned.Result.BelowMinRows;
            result->Error = "HNSW snapshot scan cancelled or exceeded its memory budget";
            ctx.Send(SelfId(), result.Release());
            return;
        }
        auto& input = scanned.Result;
        auto* worker = CreateHnswIndexBuildActor(SelfId(), scanned.LocalTid, entry->VectorColumnTag,
            input.Rows.size(), entry->Settings, std::move(input.Rows), std::move(input.MemoryReservation),
            input.ReservedBytes, entry->BuildVersion, scanned.BuildToken, input.AllowEmpty,
            scanned.IndexId, scanned.MemoryOwner, entry->CancelBuild, std::move(input.ReduceReservation));
        const auto actor = TActivationContext::Register(worker, SelfId(), TMailboxType::HTSwap, AppData()->BatchPoolId);
        entry->BuildActor = actor;
        Actors.insert(actor);
    }

    void TDataShard::Handle(TEvPrivate::TEvHnswIndexBuildResult::TPtr& ev, const TActorContext& ctx) {
        Actors.erase(ev->Sender);
        auto* result = ev->Get();
        HnswBuildsInFlight.erase(result->BuildToken);
        if (auto* entry = FindHnswIndexEntry(result->LocalTid, result->IndexId);
                entry && entry->BuildToken == result->BuildToken) { entry->BuildActor = {}; }
        if (!IsHnswBuildCurrent(result->LocalTid, result->BuildToken, result->IndexId)) {
            auto* it = FindHnswIndexEntry(result->LocalTid, result->IndexId);
            if (HnswCacheMemoryTracker && (!it || it->MemoryOwner != result->MemoryOwner || it->BuildToken == result->BuildToken)) {
                HnswCacheMemoryTracker->ResetDemand(result->MemoryOwner);
            }
            if (it && it->BuildToken == result->BuildToken) {
                DeferHnswIndexBuild(result->LocalTid, TDuration::Seconds(1), result->IndexId);
                ScheduleHnswRebuild(result->LocalTid, result->IndexId);
            }
            return;
        }
        if (result->BelowMinRows) {
            // A successful scan of a small partition is not a failed build. Writes
            // and settings changes re-enable it; reads alone must not scan again.
            const auto& entry = *FindHnswIndexEntry(result->LocalTid, result->IndexId);
            if (entry.Changes->CountAfter(entry.BuildVersion)) {
                // The table may have grown while the snapshot scan was running.
                DeferHnswIndexBuild(result->LocalTid, TDuration::Zero(), result->IndexId);
                ScheduleHnswRebuild(result->LocalTid, result->IndexId);
            } else {
                DisableHnswIndexBuild(result->LocalTid, result->IndexId);
            }
            return;
        }
        if (result->Index) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                       TabletID() << " HNSW: lazy build completed for localTid=" << result->LocalTid << " indexId=" << result->IndexId
                                  << " size=" << result->Index->Size()
                                  << " baseVersion=" << result->BaseVersion << " buildToken=" << result->BuildToken);
            SetHnswIndex(result->LocalTid, std::move(result->Index), std::move(result->MemoryReservation),
                         result->RowCountAtBuild, result->VectorColumnTag, result->Settings,
                         result->BaseVersion, result->BuildToken, result->IndexId);
        } else {
            auto* entry = FindHnswIndexEntry(result->LocalTid, result->IndexId);
            DeferHnswIndexBuild(result->LocalTid, TDuration::Seconds(5), result->IndexId);
            if (entry && !entry->Index && !entry->InitialBuildAttemptsLeft) {
                DisableHnswIndexBuild(result->LocalTid, result->IndexId);
            } else {
                ScheduleHnswRebuild(result->LocalTid, result->IndexId);
            }
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                         TabletID() << " HNSW: lazy build failed for localTid=" << result->LocalTid << " indexId=" << result->IndexId
                                    << ": " << result->Error);
        }
    }

} // namespace NKikimr::NDataShard
