#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include <ydb/core/graph/api/events.h>
#include <ydb/core/graph/api/service.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonGraph : public TViewerPipeClient {
    using TThis = TJsonGraph;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    std::vector<TString> Metrics;

public:
    TJsonGraph(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        BLOG_TRACE("Graph received request for " << Event->Get()->Request.GetUri());
        const auto& params(Event->Get()->Request.GetParams());
        NKikimrGraph::TEvGetMetrics getRequest;
        if (params.Has("target")) {
            StringSplitter(params.Get("target")).Split(',').SkipEmpty().Collect(&Metrics);
            for (const auto& metric : Metrics) {
                getRequest.AddMetrics(metric);
            }
        } else {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "no 'target' parameter specified"));
        }
        if (params.Has("from")) {
            getRequest.SetTimeFrom(FromStringWithDefault<ui32>(params.Get("from")));
        }
        if (params.Has("until")) {
            getRequest.SetTimeTo(FromStringWithDefault<ui32>(params.Get("until")));
        }
        if (params.Has("maxDataPoints")) {
            getRequest.SetMaxPoints(FromStringWithDefault<ui32>(params.Get("maxDataPoints"), 1000));
        }
        Send(NGraph::MakeGraphServiceId(), new NGraph::TEvGraph::TEvGetMetrics(std::move(getRequest)));
        Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NGraph::TEvGraph::TEvMetricsResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NGraph::TEvGraph::TEvMetricsResult::TPtr& ev) {
        const auto& params(Event->Get()->Request.GetParams());
        const auto& response(ev->Get()->Record);
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
        if (!params.Has("format") || params.Get("format") == "graphite") { // graphite
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
                    jsonDataPoint.AppendValue(response.GetTime(nTime));
                    double value = protoMetric.GetValues(nTime);
                    if (isnan(value)) {
                        jsonDataPoint.AppendValue(NJson::TJsonValue(NJson::JSON_NULL));
                    } else {
                        jsonDataPoint.AppendValue(value);
                    }
                }
            }
        } else { // prometheus
            json["status"] = "success";
            NJson::TJsonValue& jsonData(json["data"]);
            jsonData["resultType"] = "matrix";
            NJson::TJsonValue& jsonResults(jsonData["result"]);
            jsonResults.SetType(NJson::JSON_ARRAY);
            for (size_t nMetric = 0; nMetric < response.DataSize(); ++nMetric) {
                const auto& protoMetric(response.GetData(nMetric));
                NJson::TJsonValue& jsonResult(jsonResults.AppendValue({}));
                jsonResult["metric"]["__name__"] = Metrics[nMetric];
                NJson::TJsonValue& jsonValues(jsonResult["values"]);
                jsonValues.SetType(NJson::JSON_ARRAY);
                for (size_t nTime = 0; nTime < response.TimeSize(); ++nTime) {
                    NJson::TJsonValue& jsonDataPoint(jsonValues.AppendValue({}));
                    jsonDataPoint.AppendValue(response.GetTime(nTime));
                    double value = protoMetric.GetValues(nTime);
                    if (isnan(value)) {
                        jsonDataPoint.AppendValue(NJson::TJsonValue(NJson::JSON_NULL));
                    } else {
                        jsonDataPoint.AppendValue(value);
                    }
                }
            }
        }

        ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    void ReplyAndPassAway() override {}

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Graph data",
            .Description = "Returns graph data",
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
            .Description = "response format, could be prometheus or graphite",
            .Type = "string",
        });
        return yaml;
    }
};

}
