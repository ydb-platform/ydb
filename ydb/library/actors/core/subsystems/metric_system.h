#pragma once

#include <ydb/library/actors/core/subsystem.h>
#include <ydb/library/actors/metrics/line.h>
#include <ydb/library/actors/metrics/lines/raw_line_frontend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>

namespace NActors {
    // Register replacements under IMetricSystem, not their concrete type:
    // setup->RegisterSubSystem<IMetricSystem>(std::make_unique<TMyMetricSystem>());
    // Read/export APIs belong to the implementation; instrumentation uses only
    // this writer contract. The system must outlive all returned line handles.
    class IMetricSystem : public ISubSystem {
    public:
        template<class TFrontend = TRawLineFrontend<ui64>>
        TLine<TFrontend> CreateLine(TStringBuf name, std::span<const TLabel> labels, const typename TFrontend::TConfig& config = {}) {
            return TLine<TFrontend>(CreateLineWithMeta(name, labels, TFrontend::MakeMeta(config)));
        }

        virtual bool SetCommonLabels(std::span<const TLabel> labels) = 0;

    protected:
        // Copy borrowed metadata before returning. A null endpoint rejects the
        // line; implementations may also return a pending endpoint.
        virtual std::shared_ptr<IMetricLine> CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) = 0;
    };

    IMetricSystem* GetMetricSystem(TActorSystem& actorSystem);
    const IMetricSystem* GetMetricSystem(const TActorSystem& actorSystem);
    IMetricSystem* GetMetricSystem();
} // namespace NActors
