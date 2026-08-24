#pragma once

#include <ydb/library/actors/core/subsystem.h>
#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>
#include <ydb/library/actors/metrics/lines/raw_line_frontend.h>

#include <memory>

namespace NActors {
    class TActorSystem;
    class TInMemoryMetricsRegistry;
    class IActor;

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
    IActor* CreateInMemoryMetricsStatsActor(TDuration interval = TDuration::Seconds(1));

    class TInMemoryMetricsRegistry : public ISubSystem {
    public:
        explicit TInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
        ~TInMemoryMetricsRegistry() override;

        // Single-writer contract: a line can be created only once for a key.
        // Duplicate CreateLine() calls return a noop line.
        // Line key canonicalizes label order, so the same (name, labels) with different
        // input label ordering still resolves to the same line.
        // Common labels are registry-wide mutable state and are not part of line identity.
        template<class TFrontend = TRawLineFrontend<ui64>>
        TLine<TFrontend> CreateLine(TStringBuf name, std::span<const TLabel> labels, const typename TFrontend::TConfig& config = {}) {
            return Backend.CreateLine<TFrontend>(name, labels, config);
        }
        void SetCommonLabels(std::span<const TLabel> labels);
        TVector<TLabel> GetCommonLabels() const;
        TInMemoryMetricsStats GetStats() const;
        void UpdateSelfMetrics();
        // Common labels and line views are borrowing objects and are valid only during cb().
        void ReadSnapshot(const TReadSnapshotCallback& cb) const;

        ui64 GetReuseWatermark() const noexcept;
        const TInMemoryMetricsConfig& GetConfig() const noexcept;

    private:
        void OnAfterStart(TActorSystem&) override;
        void OnBeforeStop(TActorSystem&) override;

    private:
        TInMemoryMetricsBackend Backend;
    };

    TInMemoryMetricsRegistry* GetInMemoryMetrics(TActorSystem& actorSystem);
    const TInMemoryMetricsRegistry* GetInMemoryMetrics(const TActorSystem& actorSystem);
    TInMemoryMetricsRegistry* GetInMemoryMetrics();

} // namespace NActors
