#pragma once

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/services.pb.h>
#include "viewer.h"
#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <ydb/core/health_check/health_check.h>
#include <ydb/core/util/proto_duration.h>
#include <util/string/split.h>
#include "healthcheck_record.h"
#include <vector>

namespace NKikimr {
namespace NViewer {
    
using namespace NActors;
using namespace NMonitoring;

enum HealthCheckResponseFormat {
    JSON,
    PROMETHEUS
};

class THealthCheck : public TActorBootstrapped<THealthCheck> {
    static const bool WithRetry = false;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    HealthCheckResponseFormat Format;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    THealthCheck(IViewer*, NMon::TEvHttpInfo::TPtr& ev)
        : Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Format = HealthCheckResponseFormat::JSON;
        if (const auto *header = Event->Get()->Request.GetHeaders().FindHeader("Accept")) {
            THashSet<TString> accept;
            StringSplitter(header->Value()).SplitBySet(", ").SkipEmpty().Collect(&accept);
            if (accept.contains("*/*") || accept.contains("application/json")) {
                Format = HealthCheckResponseFormat::JSON;
            } else if (accept.contains("text/plain")) {
                Format = HealthCheckResponseFormat::PROMETHEUS;
            } else {
                Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPBADREQUEST_HEADERS, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                Die(ctx);
            }
        }
        const auto& params(Event->Get()->Request.GetParams());
        if (Format == HealthCheckResponseFormat::JSON) {
            JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
            JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        }
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        request->Database = params.Get("tenant");
        request->Request.set_return_verbose_status(FromStringWithDefault<bool>(params.Get("verbose"), false));
        request->Request.set_maximum_level(FromStringWithDefault<ui32>(params.Get("max_level"), 0));
        SetDuration(TDuration::MilliSeconds(Timeout), *request->Request.mutable_operation_params()->mutable_operation_timeout());
        if (params.Has("min_status")) {
            Ydb::Monitoring::StatusFlag::Status minStatus;
            if (Ydb::Monitoring::StatusFlag_Status_Parse(params.Get("min_status"), &minStatus)) {
                request->Request.set_minimum_status(minStatus);
            } else {
                Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPBADREQUEST, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return PassAway();
            }
        }
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        Timeout += Timeout * 20 / 100; // we prefer to wait for more (+20%) verbose timeout status from HC
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
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
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPOKJSON + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandlePrometheus(NHealthCheck::TEvSelfCheckResult::TPtr& ev, const TActorContext &ctx) {
        auto recordCounters = GetRecordCounters(ev);

        TStringStream ss;
        IMetricEncoderPtr encoder = EncoderPrometheus(&ss);
        IMetricEncoder* e = encoder.Get();
        
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
        e->OnStreamBegin();
        for (auto& recordCounter : *recordCounters) {
            e->OnMetricBegin(EMetricType::IGAUGE);
            {
                e->OnLabelsBegin();
                e->OnLabel("sensor", "HC_" + domain->Name);
                if (recordCounter.first.Database) {
                    e->OnLabel("DATABASE", recordCounter.first.Database);
                }
                e->OnLabel("MESSAGE", recordCounter.first.Message);
                e->OnLabel("STATUS", recordCounter.first.Status);
                e->OnLabel("TYPE", recordCounter.first.Type);
                e->OnLabelsEnd();
            }
            e->OnInt64(TInstant::Zero(), recordCounter.second);
            e->OnMetricEnd();
        }

        e->OnStreamEnd();

        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPOKTEXT + ss.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
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
        Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPGATEWAYTIMEOUT, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

}
}
