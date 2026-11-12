#include "inmemory_metrics.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/thread/lfqueue.h>

namespace NActors {
    namespace {
        struct TEvProcessMetrics : TEventLocal<TEvProcessMetrics, EventSpaceBegin(TEvents::ES_PRIVATE)> {};
    }

    struct TInMemoryMetricsRegistry::TRequest {
        enum class EType { Register, Snapshot, LineSnapshotById, LineSnapshotByKey, Labels };
        EType Type;
        std::shared_ptr<TLineWriterState> State;
        TLineKey Key;
        TLineMeta Meta;
        TVector<TLabel> Labels;
        TActorId Recipient;
        ui64 Cookie = 0;
        ui32 LineId = 0;

        explicit TRequest(EType type) : Type(type) {}
        ~TRequest() {
            if (State) {
                // Queue rejection or forced teardown must not leave a live
                // handle waiting for registration that can never happen.
                auto pending = ELineWriterStatus::Pending;
                State->Status.compare_exchange_strong(pending, ELineWriterStatus::Rejected, std::memory_order_release);
            }
        }
    };

    class TInMemoryMetricsRegistry::TImpl {
    public:
        TLockFreeQueue<std::shared_ptr<TRequest>> Requests;
        std::atomic<ui32> Queued = 0;
        std::atomic<bool> Stopping = false;
        TActorId ManagerId;
        std::atomic<TActorSystem*> System = nullptr;
    };

    // This mailbox is the sole owner of registry metadata and chunk allocation.
    // After executor shutdown, OnAfterStop takes over only final close cleanup.
    class TInMemoryMetricsManagerActor final : public TActorBootstrapped<TInMemoryMetricsManagerActor> {
    public:
        explicit TInMemoryMetricsManagerActor(TInMemoryMetricsRegistry* registry)
            : Registry(registry)
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
            Tick();
        }

        STRICT_STFUNC(StateWork,
            cFunc(TEvProcessMetrics::EventType, Process);
            cFunc(TEvents::TSystem::Wakeup, Tick);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

    private:
        void Process() {
            Registry->ProcessRequests(TActivationContext::ActorSystem(), SelfId());
        }

        void Tick() {
            Process();
            Registry->Backend.UpdateSelfMetrics();
            // Snapshot release returns memory atomically without calling a dead
            // backend. Retry pinned-capacity shortages even with no new writes.
            Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        }

        TInMemoryMetricsRegistry* const Registry;
    };

    TInMemoryMetricsRegistry::TInMemoryMetricsRegistry(TInMemoryMetricsConfig config)
        : Impl(std::make_unique<TImpl>())
        , Backend(std::move(config), [this] { NotifyMaintenance(); })
    {}

    TInMemoryMetricsRegistry::~TInMemoryMetricsRegistry() = default;

    bool TInMemoryMetricsRegistry::Enqueue(std::shared_ptr<TRequest> request) {
        if (Impl->Stopping.load(std::memory_order_acquire)) {
            return false;
        }
        if (Impl->Queued.fetch_add(1, std::memory_order_acq_rel) >= Backend.GetConfig().MaxPendingRequests) {
            Impl->Queued.fetch_sub(1, std::memory_order_release);
            return false;
        }
        Impl->Requests.Enqueue(std::move(request));
        Backend.RequestMaintenance();
        return true;
    }

    std::shared_ptr<IMetricLine> TInMemoryMetricsRegistry::CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) {
        auto request = std::make_shared<TRequest>(TRequest::EType::Register);
        auto state = std::make_shared<TLineWriterState>(&Backend);
        request->State = state;
        request->Key = MakeLineKey(name, labels);
        request->Meta = meta;
        Enqueue(std::move(request));
        return state;
    }

    bool TInMemoryMetricsRegistry::SetCommonLabels(std::span<const TLabel> labels) {
        auto request = std::make_shared<TRequest>(TRequest::EType::Labels);
        request->Labels.assign(labels.begin(), labels.end());
        return Enqueue(std::move(request));
    }

    bool TInMemoryMetricsRegistry::RequestSnapshot(const TActorId& recipient, ui64 cookie) {
        auto request = std::make_shared<TRequest>(TRequest::EType::Snapshot);
        request->Recipient = recipient;
        request->Cookie = cookie;
        return Enqueue(std::move(request));
    }

    bool TInMemoryMetricsRegistry::RequestLineSnapshot(const TActorId& recipient, ui32 lineId, ui64 cookie) {
        auto request = std::make_shared<TRequest>(TRequest::EType::LineSnapshotById);
        request->Recipient = recipient;
        request->Cookie = cookie;
        request->LineId = lineId;
        return Enqueue(std::move(request));
    }

    bool TInMemoryMetricsRegistry::RequestLineSnapshot(const TActorId& recipient, TStringBuf name, std::span<const TLabel> labels, ui64 cookie) {
        auto request = std::make_shared<TRequest>(TRequest::EType::LineSnapshotByKey);
        request->Recipient = recipient;
        request->Cookie = cookie;
        request->Key = MakeLineKey(name, labels);
        return Enqueue(std::move(request));
    }

    void TInMemoryMetricsRegistry::ProcessRequests(TActorSystem* system, const TActorId& sender) {
        Backend.BeginMaintenance();
        // Complete closes before admission, so a closed line can release MaxLines.
        Backend.ProcessMaintenance();
        std::shared_ptr<TRequest> request;
        ui32 budget = NInMemoryMetricsPrivate::MaintenanceBatchSize;
        while (budget && Impl->Requests.Dequeue(&request)) {
            --budget;
            Impl->Queued.fetch_sub(1, std::memory_order_release);
            if (Impl->Stopping.load(std::memory_order_acquire)) {
                request.reset();
                continue;
            }
            switch (request->Type) {
                case TRequest::EType::Register:
                    Backend.RegisterLine(request->State, std::move(request->Key), request->Meta);
                    break;
                case TRequest::EType::Labels:
                    Backend.SetCommonLabels(request->Labels);
                    break;
                case TRequest::EType::LineSnapshotById:
                    system->Send(new IEventHandle(request->Recipient, sender,
                        new TEvInMemoryMetricsSnapshot(Backend.CaptureSnapshot(request->LineId), Backend.GetStats()), 0, request->Cookie));
                    break;
                case TRequest::EType::LineSnapshotByKey:
                    system->Send(new IEventHandle(request->Recipient, sender,
                        new TEvInMemoryMetricsSnapshot(Backend.CaptureSnapshot(request->Key.Name, request->Key.Labels), Backend.GetStats()), 0, request->Cookie));
                    break;
                case TRequest::EType::Snapshot:
                    system->Send(new IEventHandle(request->Recipient, sender,
                        new TEvInMemoryMetricsSnapshot(Backend.CaptureSnapshot(), Backend.GetStats()), 0, request->Cookie));
                    break;
            }
            request.reset();
        }
        Backend.ProcessMaintenance();
        if (!budget) {
            Backend.RequestMaintenance();
        }
    }

    void TInMemoryMetricsRegistry::NotifyMaintenance() {
        if (TActorSystem* system = Impl->System.load(std::memory_order_acquire)) {
            system->Send(new IEventHandle(Impl->ManagerId, TActorId(), new TEvProcessMetrics()));
        }
    }

    void TInMemoryMetricsRegistry::OnAfterStart(TActorSystem& system) {
        Impl->ManagerId = system.Register(new TInMemoryMetricsManagerActor(this), TMailboxType::ReadAsFilled, 0);
        Impl->System.store(&system, std::memory_order_release);
        // Bootstrap may already have run. Kick unconditionally after publishing
        // the endpoint to pick up commands queued before registration completed.
        system.Send(new IEventHandle(Impl->ManagerId, TActorId(), new TEvProcessMetrics()));
    }

    void TInMemoryMetricsRegistry::OnBeforeStop(TActorSystem&) {
        Impl->Stopping.store(true, std::memory_order_release);
        Backend.StopManagement();
    }

    void TInMemoryMetricsRegistry::OnAfterStop(TActorSystem&) {
        // Executors joined: cancel commands and finish closes from DeferredPreStop.
        std::shared_ptr<TRequest> request;
        while (Impl->Requests.Dequeue(&request)) {
            Impl->Queued.fetch_sub(1, std::memory_order_release);
            request.reset();
        }
        Backend.BeginMaintenance();
        Backend.ProcessMaintenance();
    }

    const TInMemoryMetricsConfig& TInMemoryMetricsRegistry::GetConfig() const noexcept {
        return Backend.GetConfig();
    }

    ui64 TInMemoryMetricsRegistry::GetReuseWatermark() const noexcept {
        return Backend.GetReuseWatermark();
    }

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config) {
        return std::make_unique<TInMemoryMetricsRegistry>(std::move(config));
    }

    TInMemoryMetricsRegistry* GetInMemoryMetrics(TActorSystem& system) {
        return system.GetSubSystem<TInMemoryMetricsRegistry>();
    }

    const TInMemoryMetricsRegistry* GetInMemoryMetrics(const TActorSystem& system) {
        return system.GetSubSystem<TInMemoryMetricsRegistry>();
    }

    TInMemoryMetricsRegistry* GetInMemoryMetrics() {
        return TlsActivationContext ? GetInMemoryMetrics(*TActivationContext::ActorSystem()) : nullptr;
    }
} // namespace NActors
