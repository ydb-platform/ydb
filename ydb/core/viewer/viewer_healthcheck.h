#pragma once
#include "healthcheck_record.h"
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"
#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NMonitoring;

enum HealthCheckResponseFormat {
    JSON,
    PROMETHEUS
};

class TJsonHealthCheck : public TViewerPipeClient {
    using TThis = TJsonHealthCheck;
    using TBase = TViewerPipeClient;
    static const bool WithRetry = false;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    HealthCheckResponseFormat Format;
    TString Database;
    bool Cache = true;
    bool MergeRecords = false;
    std::optional<Ydb::Monitoring::SelfCheckResult> Result;
    std::optional<TRequestResponse<NHealthCheck::TEvSelfCheckResult>> SelfCheckResult;
    Ydb::Monitoring::StatusFlag::Status MinStatus = Ydb::Monitoring::StatusFlag::UNSPECIFIED;

public:
    TJsonHealthCheck(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    THolder<NHealthCheck::TEvSelfCheckRequest> MakeSelfCheckRequest() {
        const auto& params(Event->Get()->Request.GetParams());
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        request->Database = Database;
        if (params.Has("verbose")) {
            request->Request.set_return_verbose_status(FromStringWithDefault<bool>(params.Get("verbose"), false));
        }
        if (params.Has("max_level")) {
            request->Request.set_maximum_level(FromStringWithDefault<ui32>(params.Get("max_level"), 0));
        }
        if (MinStatus != Ydb::Monitoring::StatusFlag::UNSPECIFIED) {
            request->Request.set_minimum_status(MinStatus);
        }
        if (params.Has("merge_records")) {
            request->Request.set_merge_records(MergeRecords);
        }
        SetDuration(TDuration::MilliSeconds(Timeout), *request->Request.mutable_operation_params()->mutable_operation_timeout());
        return request;
    }

    void SendHealthCheckRequest() {
        SelfCheckResult = MakeRequest<NHealthCheck::TEvSelfCheckResult>(NHealthCheck::MakeHealthCheckID(), MakeSelfCheckRequest().Release());
    }

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
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
        Database = params.Get("database");
        if (Database.empty()) {
            Database = params.Get("tenant");
        }
        Cache = FromStringWithDefault<bool>(params.Get("cache"), Cache);
        MergeRecords = FromStringWithDefault<bool>(params.Get("merge_records"), MergeRecords);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);

        if (params.Get("min_status") && !Ydb::Monitoring::StatusFlag_Status_Parse(params.Get("min_status"), &MinStatus)) {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "The field 'min_status' cannot be parsed"));
        }
        if (AppData()->FeatureFlags.GetEnableDbMetadataCache() && Cache && Database && MergeRecords) {
            RequestStateStorageMetadataCacheEndpointsLookup(Database);
        } else {
            SendHealthCheckRequest();
        }
        Timeout += Timeout * 20 / 100; // we prefer to wait for more (+20%) verbose timeout status from HC
        Become(&TThis::StateRequestedInfo, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STFUNC(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, TBase::HandleTimeout);
            hFunc(NHealthCheck::TEvSelfCheckResultProto, Handle);
            cFunc(TEvents::TSystem::Undelivered, SendHealthCheckRequest);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
        }
    }

    int GetIssueCount(const Ydb::Monitoring::IssueLog& issueLog) {
        return issueLog.count() == 0 ? 1 : issueLog.count();
    }

    THolder<THashMap<TMetricRecord, ui32>> GetRecordCounters() {
        const auto *descriptor = Ydb::Monitoring::StatusFlag_Status_descriptor();
        THashMap<TMetricRecord, ui32> recordCounters;
        for (auto& log : Result->issue_log()) {
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

    void HandlePrometheus() {
        auto recordCounters = GetRecordCounters();

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
        auto result = descriptor->FindValueByNumber(Result->self_check_result())->name();
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

        TBase::ReplyAndPassAway(GetHTTPOK("text/plain", ss.Str()));
    }

    void ReplyAndPassAway() override {
        if (!Result) {
            return TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No result"));
        } else {
            if (Format == HealthCheckResponseFormat::PROMETHEUS) {
                return HandlePrometheus();
            } else {
                TStringStream json;
                TProtoToJson::ProtoToJson(json, *Result, JsonSettings);
                return TBase::ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
            }
        }
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        SelfCheckResult->Set(std::move(ev));
        Result = std::move(SelfCheckResult->Get()->Result);
        ReplyAndPassAway();
    }

    void Handle(NHealthCheck::TEvSelfCheckResultProto::TPtr& ev) {
        Result = std::move(ev->Get()->Record);
        NHealthCheck::RemoveUnrequestedEntries(*Result, MakeSelfCheckRequest().Release()->Request);
        ReplyAndPassAway();
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        auto activeNode = TDatabaseMetadataCache::PickActiveNode(ev->Get()->InfoEntries);
        if (activeNode != 0) {
            TActorId cache = MakeDatabaseMetadataCacheId(activeNode);
            auto request = MakeHolder<NHealthCheck::TEvSelfCheckRequestProto>();
            Send(cache, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, activeNode);
        } else {
            SendHealthCheckRequest();
        }
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Self-check result",
            .Description = "Performs self-check and returns result",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "cache",
            .Description = "use cache",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "verbose",
            .Description = "return verbose status",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "merge_records",
            .Description = "merge records",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "max_level",
            .Description = "max depth of issues to return",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "min_status",
            .Description = "min status of issues to return",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "format",
            .Description = "format of reply",
            .Type = "string",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<Ydb::Monitoring::SelfCheckResult>());
        return yaml;
    }
};

}
