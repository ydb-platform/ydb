#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include "viewer.h"
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/health_check/health_check.h>
#include <ydb/core/util/proto_duration.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <util/string/split.h>
#include "healthcheck_record.h"
#include <vector>

namespace NKikimr {
namespace NViewer {

using namespace NActors;

enum HealthCheckResponseFormat {
    JSON,
    PROMETHEUS
};

class TJsonHealthCheck : public TActorBootstrapped<TJsonHealthCheck> {
    IViewer* Viewer;
    static const bool WithRetry = false;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    HealthCheckResponseFormat Format;
    TString Database;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonHealthCheck(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());

        Format = HealthCheckResponseFormat::JSON;
        if (params.Has("format")) {
            auto& format = params.Get("format");
            if (format == "json") {
                Format = HealthCheckResponseFormat::JSON;
            } else if (format == "prometheus") {
                Format = HealthCheckResponseFormat::PROMETHEUS;
            }
        } else if (const auto *header = Event->Get()->Request.GetHeaders().FindHeader("Accept")) {
            THashSet<TString> accept;
            StringSplitter(header->Value()).SplitBySet(", ").SkipEmpty().Collect(&accept);
            if (accept.contains("*/*") || accept.contains("application/json")) {
                Format = HealthCheckResponseFormat::JSON;
            } else if (accept.contains("text/plain")) {
                Format = HealthCheckResponseFormat::PROMETHEUS;
            } else {
                Format = HealthCheckResponseFormat::JSON;
            }
        }
        if (Format == HealthCheckResponseFormat::JSON) {
            JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
            JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        }
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        request->Database = Database = params.Get("tenant");
        request->Request.set_return_verbose_status(FromStringWithDefault<bool>(params.Get("verbose"), false));
        request->Request.set_maximum_level(FromStringWithDefault<ui32>(params.Get("max_level"), 0));
        request->Request.set_merge_records(FromStringWithDefault<bool>(params.Get("merge_records"), false));
        SetDuration(TDuration::MilliSeconds(Timeout), *request->Request.mutable_operation_params()->mutable_operation_timeout());
        if (params.Has("min_status")) {
            Ydb::Monitoring::StatusFlag::Status minStatus;
            if (Ydb::Monitoring::StatusFlag_Status_Parse(params.Get("min_status"), &minStatus)) {
                request->Request.set_minimum_status(minStatus);
            } else {
                Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return PassAway();
            }
        }
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        Timeout += Timeout * 20 / 100; // we prefer to wait for more (+20%) verbose timeout status from HC
        ctx.Schedule(TDuration::Seconds(Timeout), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedInfo);
    }

    STFUNC(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    int GetIssueCount(const Ydb::Monitoring::IssueLog& issueLog) {
        return issueLog.count() == 0 ? 1 : issueLog.count();
    }

    THolder<THashMap<TMetricRecord, ui32>> GetRecordCounters(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        const auto *descriptor = Ydb::Monitoring::StatusFlag_Status_descriptor();
        THashMap<TMetricRecord, ui32> recordCounters;
        for (auto& log : ev->Get()->Result.issue_log()) {
            TMetricRecord record {
                .Database = log.location().database().name(),
                .Message = log.message(),
                .Status = descriptor->FindValueByNumber(log.status())->name(),
                .Type = log.type()
            };

            auto it = recordCounters.find(record);
            if (it != recordCounters.end()) {
                it->second += GetIssueCount(log);
            } else {
                recordCounters[record] = GetIssueCount(log);
            }
        }

        return MakeHolder<THashMap<TMetricRecord, ui32>>(recordCounters);
    }

    void HandleJSON(NHealthCheck::TEvSelfCheckResult::TPtr& ev, const TActorContext &ctx) {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, ev->Get()->Result, JsonSettings);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandlePrometheus(NHealthCheck::TEvSelfCheckResult::TPtr& ev, const TActorContext &ctx) {
        auto recordCounters = GetRecordCounters(ev);

        TStringStream ss;
        IMetricEncoderPtr encoder = EncoderPrometheus(&ss);
        IMetricEncoder* e = encoder.Get();

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();
        auto filterDatabase = Database ? Database : "/" + domain->Name;
        e->OnStreamBegin();
        if (recordCounters->size() > 0) {
            for (auto& recordCounter : *recordCounters) {
                e->OnMetricBegin(EMetricType::IGAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "ydb_healthcheck");
                    e->OnLabel("DOMAIN", domain->Name);
                    e->OnLabel("DATABASE", recordCounter.first.Database ? recordCounter.first.Database : filterDatabase);
                    e->OnLabel("MESSAGE", recordCounter.first.Message);
                    e->OnLabel("STATUS", recordCounter.first.Status);
                    e->OnLabel("TYPE", recordCounter.first.Type);
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::Zero(), recordCounter.second);
                e->OnMetricEnd();
            }
        }
        const auto *descriptor = Ydb::Monitoring::SelfCheck_Result_descriptor();
        auto result = descriptor->FindValueByNumber(ev->Get()->Result.self_check_result())->name();
        e->OnMetricBegin(EMetricType::IGAUGE);
        {
            e->OnLabelsBegin();
            e->OnLabel("sensor", "ydb_healthcheck");
            e->OnLabel("DOMAIN", domain->Name);
            e->OnLabel("DATABASE", filterDatabase);
            e->OnLabel("MESSAGE", result);
            e->OnLabel("STATUS", result);
            e->OnLabel("TYPE", "ALL");
            e->OnLabelsEnd();
        }
        e->OnInt64(TInstant::Zero(), 1);
        e->OnMetricEnd();
        e->OnStreamEnd();

        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKTEXT(Event->Get()) + ss.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev, const TActorContext &ctx) {
        if (Format == HealthCheckResponseFormat::JSON) {
            HandleJSON(ev, ctx);
        } else {
            HandlePrometheus(ev, ctx);
        }
    }

    void HandleTimeout(const TActorContext &ctx) {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonHealthCheck> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<Ydb::Monitoring::SelfCheckResult>();
    }
};

template <>
struct TJsonRequestParameters<TJsonHealthCheck> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            - name: tenant
              in: query
              description: path to database
              required: false
              type: string
            - name: verbose
              in: query
              description: return verbose status
              required: false
              type: boolean
            - name: merge_records
              in: query
              description: merge records
              required: false
              type: boolean
            - name: max_level
              in: query
              description: max depth of issues to return
              required: false
              type: integer
            - name: min_status
              in: query
              description: min status of issues to return
              required: false
              type: string
            - name: format
              in: query
              description: format of reply
              required: false
              type: string
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonHealthCheck> {
    static TString GetSummary() {
        return "Self-check result";
    }
};

template <>
struct TJsonRequestDescription<TJsonHealthCheck> {
    static TString GetDescription() {
        return "Performs self-check and returns result";
    }
};

}
}
