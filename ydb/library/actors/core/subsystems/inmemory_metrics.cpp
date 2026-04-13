#include "inmemory_metrics.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors {

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config) {
        return std::make_unique<TInMemoryMetricsRegistry>(std::move(config));
    }

    TInMemoryMetricsRegistry::TInMemoryMetricsRegistry(TInMemoryMetricsConfig config)
        : Backend(std::move(config))
    {
    }

    TInMemoryMetricsRegistry::~TInMemoryMetricsRegistry() = default;

    void TInMemoryMetricsRegistry::SetCommonLabels(std::span<const TLabel> labels) {
        Backend.SetCommonLabels(labels);
    }

    TVector<TLabel> TInMemoryMetricsRegistry::GetCommonLabels() const {
        return Backend.GetCommonLabels();
    }

    TInMemoryMetricsStats TInMemoryMetricsRegistry::GetStats() const {
        return Backend.GetStats();
    }

    void TInMemoryMetricsRegistry::UpdateSelfMetrics() {
        Backend.UpdateSelfMetrics();
    }

    void TInMemoryMetricsRegistry::ReadSnapshot(const TReadSnapshotCallback& cb) const {
        Backend.ReadSnapshot(cb);
    }

    ui64 TInMemoryMetricsRegistry::GetReuseWatermark() const noexcept {
        return Backend.GetReuseWatermark();
    }

    const TInMemoryMetricsConfig& TInMemoryMetricsRegistry::GetConfig() const noexcept {
        return Backend.GetConfig();
    }

    void TInMemoryMetricsRegistry::OnAfterStart(TActorSystem& actorSystem) {
        actorSystem.Register(
            CreateInMemoryMetricsStatsActor(),
            TMailboxType::ReadAsFilled,
            0);
    }

    void TInMemoryMetricsRegistry::OnBeforeStop(TActorSystem&) {
    }

    TInMemoryMetricsRegistry* GetInMemoryMetrics(TActorSystem& actorSystem) {
        return actorSystem.GetSubSystem<TInMemoryMetricsRegistry>();
    }

    const TInMemoryMetricsRegistry* GetInMemoryMetrics(const TActorSystem& actorSystem) {
        return actorSystem.GetSubSystem<TInMemoryMetricsRegistry>();
    }

    TInMemoryMetricsRegistry* GetInMemoryMetrics() {
        if (!TlsActivationContext) {
            return nullptr;
        }
        return TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
    }

    namespace {
        class TInMemoryMetricsStatsActor final
            : public TActorBootstrapped<TInMemoryMetricsStatsActor>
        {
        public:
            explicit TInMemoryMetricsStatsActor(TDuration interval)
                : Interval(interval)
            {
            }

            void Bootstrap(const TActorContext& ctx) {
                Publish(ctx);
                Become(&TThis::StateWork);
            }

            STRICT_STFUNC(StateWork,
                cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )

        private:
            void HandleWakeup() {
                Publish(TActivationContext::AsActorContext());
            }

            void Publish(const TActorContext& ctx) {
                if (auto* metrics = GetInMemoryMetrics(*ctx.ActorSystem())) {
                    metrics->UpdateSelfMetrics();
                }
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

        private:
            const TDuration Interval;
        };
    } // namespace

    IActor* CreateInMemoryMetricsStatsActor(TDuration interval) {
        return new TInMemoryMetricsStatsActor(interval);
    }

} // namespace NActors
