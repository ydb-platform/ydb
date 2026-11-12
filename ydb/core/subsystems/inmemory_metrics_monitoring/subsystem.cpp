#include "subsystem.h"
#include "viewer.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>
#include <library/cpp/json/json_writer.h>
#include <util/generic/hash.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

namespace NKikimr::NInMemoryMetricsMonitoring {
namespace {
using namespace NActors;

class TViewer : public TActorBootstrapped<TViewer> {
    struct TRequest {
        TActorId Sender;
        ui64 Cookie = 0;
        int SubRequestId = 0;
        TDuration Period;
        bool IncludeHistory = false;
    };

    THashMap<ui64, TRequest> Pending;
    ui64 NextRequest = 0;

    void Reply(const TRequest& request, TStringBuf status, const TString& body) {
        TStringBuilder response;
        response << "HTTP/1.1 " << status << "\r\nContent-Type: application/json; charset=utf-8\r\n"
            << "Cache-Control: no-store\r\n\r\n" << body;
        Send(request.Sender, new NMon::TEvHttpInfoRes(response, request.SubRequestId,
            NMon::IEvHttpInfoRes::Custom), 0, request.Cookie);
    }

    void Error(const TRequest& request, TStringBuf status, TStringBuf message) {
        NJson::TJsonValue body(NJson::JSON_MAP);
        body["error"] = message;
        Reply(request, status, NJson::WriteJson(body, false));
    }

    void Handle(NMon::TEvHttpInfo::TPtr ev) {
        const auto& params = ev->Get()->Request.GetParams();
        if (params.Get("format") != "json") {
            Send(ev->Sender, new NMon::TEvHttpInfoRes(
                (params.Get("page") == "overview" || params.Get("page") == "dashboard")
                    ? RenderOverviewPage() : RenderPage(),
                ev->Get()->SubRequestId), 0, ev->Cookie);
            return;
        }
        TRequest request;
        request.Sender = ev->Sender;
        request.Cookie = ev->Cookie;
        request.SubRequestId = ev->Get()->SubRequestId;
        request.IncludeHistory = params.Has("line");
        ui32 lineId = 0;
        ui32 seconds = 300;
        if ((request.IncludeHistory && (!TryFromString(params.Get("line"), lineId) || !lineId))
            || (params.Has("seconds") && (!TryFromString(params.Get("seconds"), seconds) || seconds < 1 || seconds > 3600))) {
            Error(request, "400 Bad Request", "Invalid line or time range");
            return;
        }
        request.Period = TDuration::Seconds(seconds);
        if (Pending.size() >= 8) {
            Error(request, "503 Service Unavailable", "Too many snapshot requests");
            return;
        }
        auto* registry = GetInMemoryMetrics();
        const ui64 id = ++NextRequest;
        const bool accepted = registry && (request.IncludeHistory
            ? registry->RequestLineSnapshot(SelfId(), lineId, id)
            : registry->RequestSnapshot(SelfId(), id));
        if (!accepted) {
            Error(request, "503 Service Unavailable", "Metrics registry is busy or stopped");
            return;
        }
        Pending.emplace(id, request);
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(id));
    }

    void Handle(TEvInMemoryMetricsSnapshot::TPtr ev) {
        const auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            return;
        }
        const auto request = it->second;
        Pending.erase(it);
        auto* registry = GetInMemoryMetrics();
        Reply(request, "200 OK", SerializeSnapshot(ev->Get()->Snapshot, ev->Get()->Stats,
            registry->GetConfig(), TInstant::Now(), request.Period, request.IncludeHistory));
    }

    void Handle(TEvents::TEvWakeup::TPtr ev) {
        const auto it = Pending.find(ev->Get()->Tag);
        if (it != Pending.end()) {
            Error(it->second, "504 Gateway Timeout", "Metrics snapshot timed out");
            Pending.erase(it);
        }
    }

    void Stop() {
        for (const auto& [id, request] : Pending) {
            Error(request, "503 Service Unavailable", "Metrics viewer is stopping");
        }
        Pending.clear();
        PassAway();
    }

public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(NMon::TEvHttpInfo, Handle)
        hFunc(TEvInMemoryMetricsSnapshot, Handle)
        hFunc(TEvents::TEvWakeup, Handle)
        cFunc(TEvents::TSystem::Poison, Stop)
    )
};

class TMonitoringSubSystem : public TInMemoryMetricsMonitoring {
    const TConfig Config;
    TActorId Viewer;

public:
    explicit TMonitoringSubSystem(TConfig config)
        : Config(std::move(config))
    {}

    TSubSystemDependencies GetDependencies() const override {
        return DependsOn<TInMemoryMetricsRegistry>();
    }

    void OnAfterStart(TActorSystem& system) override {
        Viewer = system.Register(new TViewer(), TMailboxType::ReadAsFilled, Config.ExecutorPool);
        if (Config.RegisterPage) {
            Config.RegisterPage(system, Viewer);
        }
    }

    void OnBeforeStop(TActorSystem& system) override {
        system.Send(new IEventHandle(Viewer, {}, new TEvents::TEvPoison()));
    }
};
} // namespace

std::unique_ptr<TInMemoryMetricsMonitoring> MakeInMemoryMetricsMonitoring(TConfig config) {
    return std::make_unique<TMonitoringSubSystem>(std::move(config));
}

} // namespace NKikimr::NInMemoryMetricsMonitoring
