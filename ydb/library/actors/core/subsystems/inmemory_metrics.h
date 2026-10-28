#pragma once

#include "metric_system.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>
#include <ydb/library/actors/metrics/lines/raw_line_frontend.h>

#include <memory>

namespace NActors {
    class TActorSystem;
    class TInMemoryMetricsManagerActor;

    struct TEvInMemoryMetricsSnapshot
        : TEventLocal<TEvInMemoryMetricsSnapshot, EventSpaceBegin(TEvents::ES_INMEMORY_METRICS)>
    {
        TInMemorySnapshot Snapshot;
        TInMemoryMetricsStats Stats;

        TEvInMemoryMetricsSnapshot(TInMemorySnapshot snapshot, const TInMemoryMetricsStats& stats)
            : Snapshot(std::move(snapshot))
            , Stats(stats)
        {}
    };

    class TInMemoryMetricsRegistry : public IMetricSystem {
    public:
        explicit TInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
        ~TInMemoryMetricsRegistry() override;

        // Nonblocking admission; false if stopped or the command queue is full.
        // Accepted requests are processed in queue order. Snapshot replies carry
        // the cookie and owned data; shutdown may cancel requests without reply.
        bool SetCommonLabels(std::span<const TLabel> labels) override;
        bool RequestSnapshot(const TActorId& recipient, ui64 cookie = 0);
        // Selection happens when the manager processes the request. Stats remain
        // registry-wide; an unknown line produces an empty snapshot.
        bool RequestLineSnapshot(const TActorId& recipient, ui32 lineId, ui64 cookie = 0);
        bool RequestLineSnapshot(const TActorId& recipient, TStringBuf name, std::span<const TLabel> labels, ui64 cookie = 0);
        const TInMemoryMetricsConfig& GetConfig() const noexcept;
        ui64 GetReuseWatermark() const noexcept;

    private:
        friend class TInMemoryMetricsManagerActor;
        struct TRequest;
        class TImpl;
        bool Enqueue(std::shared_ptr<TRequest> request);
        std::shared_ptr<IMetricLine> CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) override;
        void ProcessRequests(TActorSystem* system, const TActorId& sender);
        void OnAfterStart(TActorSystem&) override;
        void OnBeforeStop(TActorSystem&) override;
        void OnAfterStop(TActorSystem&) override;
        void NotifyMaintenance();

        std::unique_ptr<TImpl> Impl;
        TInMemoryMetricsBackend Backend;
    };

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
    TInMemoryMetricsRegistry* GetInMemoryMetrics(TActorSystem& actorSystem);
    const TInMemoryMetricsRegistry* GetInMemoryMetrics(const TActorSystem& actorSystem);
    TInMemoryMetricsRegistry* GetInMemoryMetrics();
} // namespace NActors
