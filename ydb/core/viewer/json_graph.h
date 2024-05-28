#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/events.h>
#include <library/cpp/json/json_writer.h>
#include "viewer.h"
#include "log.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonGraph : public TActorBootstrapped<TJsonGraph> {
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    std::vector<TString> Metrics;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonGraph(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        BLOG_TRACE("Graph received request for " << Event->Get()->Request.GetUri());
        const auto& params(Event->Get()->Request.GetParams());
        NKikimrGraph::TEvGetMetrics getRequest;
        if (params.Has("target")) {
            StringSplitter(params.Get("target")).Split(',').SkipEmpty().Collect(&Metrics);
            for (const auto& metric : Metrics) {
                getRequest.AddMetrics(metric);
            }
        } else {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
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
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(NGraph::TEvGraph::TEvMetricsResult::TPtr& ev) {
        const auto& params(Event->Get()->Request.GetParams());
        const auto& response(ev->Get()->Record);
        NJson::TJsonValue json;

        if (response.GetError()) {
            json["status"] = "error";
            json["error"] = response.GetError();
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + NJson::WriteJson(json, false), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        if (response.DataSize() != Metrics.size()) {
            json["status"] = "error";
            json["error"] = "Invalid data size received";
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + NJson::WriteJson(json, false), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        for (size_t nMetric = 0; nMetric < response.DataSize(); ++nMetric) {
            const auto& protoMetric(response.GetData(nMetric));
            if (response.TimeSize() != protoMetric.ValuesSize()) {
                json["status"] = "error";
                json["error"] = "Invalid value size received";
                Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + NJson::WriteJson(json, false), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return PassAway();
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

        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + NJson::WriteJson(json, false), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void Timeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestParameters<TJsonGraph> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
                - name: target
                  in: query
                  description: metrics comma delimited
                  required: true
                  type: string
                - name: from
                  in: query
                  description: time in seconds
                  required: false
                  type: integer
                - name: until
                  in: query
                  description: time in seconds
                  required: false
                  type: integer
                - name: maxDataPoints
                  in: query
                  description: maximum number of data points
                  required: false
                  type: integer
                - name: format
                  in: query
                  description: response format, could be prometheus or graphite
                  required: false
                  type: string
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonGraph> {
    static TString GetSummary() {
        return "Graph data";
    }
};

template <>
struct TJsonRequestDescription<TJsonGraph> {
    static TString GetDescription() {
        return "Returns graph data";
    }
};

}
}
