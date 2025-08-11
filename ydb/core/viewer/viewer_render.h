#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer_request.h"
#include <ydb/core/graph/api/events.h>
#include <ydb/core/graph/api/service.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NMonitoring;

class TJsonRender : public TViewerPipeClient {
    using TThis = TJsonRender;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    static constexpr bool RunOnDynnode = true;

    std::vector<TString> Metrics;

public:
    TJsonRender(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
    }

    void BootstrapEx() override {
        NKikimrGraph::TEvGetMetrics getRequest;
        if (Params.Has("target")) {
            TString metric;
            size_t num = 0;
            for (;;) {
                metric = Params.Get("target", num);
                if (metric.empty()) {
                    break;
                }
                Metrics.push_back(metric);
                ++num;
            }
            SendGraphRequest();
        } else {
            ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Bad Request"));
            return;
        }

        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NGraph::TEvGraph::TEvMetricsResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendGraphRequest() {
        NKikimrGraph::TEvGetMetrics getRequest;
        if (Metrics.size() > 0) {
            for (const auto& metric : Metrics) {
                getRequest.AddMetrics(metric);
            }
        } else {
            static const TString png1x1 = "\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d\x49\x48\x44\x52\x00\x00\x00\x01\x00\x00\x00\x01\x01"
                                        "\x03\x00\x00\x00\x25\xdb\x56\xca\x00\x00\x00\x03\x50\x4c\x54\x45\x00\x00\x00\xa7\x7a\x3d\xda\x00\x00"
                                        "\x00\x01\x74\x52\x4e\x53\x00\x40\xe6\xd8\x66\x00\x00\x00\x0a\x49\x44\x41\x54\x08\xd7\x63\x60\x00\x00"
                                        "\x00\x02\x00\x01\xe2\x21\xbc\x33\x00\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82";
            ReplyAndPassAway(GetHTTPOK("image/png", png1x1));
            return PassAway();
        }
        if (Params.Has("from")) {
            getRequest.SetTimeFrom(FromStringWithDefault<ui32>(Params.Get("from")));
        }
        if (Params.Has("until")) {
            getRequest.SetTimeTo(FromStringWithDefault<ui32>(Params.Get("until")));
        }
        if (Params.Has("maxDataPoints")) {
            getRequest.SetMaxPoints(FromStringWithDefault<ui32>(Params.Get("maxDataPoints"), 1000));
        }
        Send(NGraph::MakeGraphServiceId(), new NGraph::TEvGraph::TEvGetMetrics(std::move(getRequest)));
    }

    void HandleRenderResponse(NKikimrGraph::TEvMetricsResult& response) {
        NJson::TJsonValue json;

        if (response.GetError()) {
            json["status"] = "error";
            json["error"] = response.GetError();
            return ReplyAndPassAway(GetHTTPOKJSON(json));
        }
        if (response.DataSize() != Metrics.size()) {
            json["status"] = "error";
            json["error"] = "Invalid data size received";
            return ReplyAndPassAway(GetHTTPOKJSON(json));
        }
        for (size_t nMetric = 0; nMetric < response.DataSize(); ++nMetric) {
            const auto& protoMetric(response.GetData(nMetric));
            if (response.TimeSize() != protoMetric.ValuesSize()) {
                json["status"] = "error";
                json["error"] = "Invalid value size received";
                return ReplyAndPassAway(GetHTTPOKJSON(json));
            }
        }
        { // graphite
            json.SetType(NJson::JSON_ARRAY);
            for (size_t nMetric = 0; nMetric < response.DataSize(); ++nMetric) {
                const auto& protoMetric(response.GetData(nMetric));
                NJson::TJsonValue& jsonMetric(json.AppendValue({}));
                jsonMetric["target"] = Metrics[nMetric];
                jsonMetric["title"] = Metrics[nMetric];
                jsonMetric["tags"]["name"] = Metrics[nMetric];
                NJson::TJsonValue& jsonDataPoints(jsonMetric["datapoints"]);
                jsonDataPoints.SetType(NJson::JSON_ARRAY);
                for (size_t nTime = 0; nTime < response.TimeSize(); ++nTime) {
                    NJson::TJsonValue& jsonDataPoint(jsonDataPoints.AppendValue({}));
                    double value = protoMetric.GetValues(nTime);
                    if (isnan(value)) {
                        jsonDataPoint.AppendValue(NJson::TJsonValue(NJson::JSON_NULL));
                    } else {
                        jsonDataPoint.AppendValue(value);
                    }
                    jsonDataPoint.AppendValue(response.GetTime(nTime));
                }
            }
        }

        ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    void Handle(NGraph::TEvGraph::TEvMetricsResult::TPtr& ev) {
        HandleRenderResponse(ev->Get()->Record);
    }

    void ReplyAndPassAway() override {
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Graph data",
            .Description = "Returns graph data in graphite format",
        });
        yaml.AddParameter({
            .Name = "target",
            .Description = "metrics comma delimited",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "from",
            .Description = "time in seconds",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "direct",
            .Description = "force processing query on current node",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "until",
            .Description = "time in seconds",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "maxDataPoints",
            .Description = "maximum number of data points",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "format",
            .Description = "response format",
            .Type = "string",
        });
        return yaml;
    }
};

}
