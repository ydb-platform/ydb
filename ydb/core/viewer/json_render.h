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

class TJsonRender : public TActorBootstrapped<TJsonRender> {
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    std::vector<TString> Metrics;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonRender(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        auto postData = Event->Get()->Request.GetPostContent();
        BLOG_D("PostData=" << postData);
        NKikimrGraph::TEvGetMetrics getRequest;
        if (postData) {
            TCgiParameters params(postData);
            if (params.Has("target")) {
                TString metric;
                size_t num = 0;
                for (;;) {
                    metric = params.Get("target", num);
                    if (metric.empty()) {
                        break;
                    }
                    Metrics.push_back(metric);
                    ++num;
                }
                //StringSplitter(params.Get("target")).Split(',').SkipEmpty().Collect(&Metrics);
                for (const auto& metric : Metrics) {
                    getRequest.AddMetrics(metric);
                }
            } else {
                static const TString png1x1 = "\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d\x49\x48\x44\x52\x00\x00\x00\x01\x00\x00\x00\x01\x01"
                                            "\x03\x00\x00\x00\x25\xdb\x56\xca\x00\x00\x00\x03\x50\x4c\x54\x45\x00\x00\x00\xa7\x7a\x3d\xda\x00\x00"
                                            "\x00\x01\x74\x52\x4e\x53\x00\x40\xe6\xd8\x66\x00\x00\x00\x0a\x49\x44\x41\x54\x08\xd7\x63\x60\x00\x00"
                                            "\x00\x02\x00\x01\xe2\x21\xbc\x33\x00\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82";
                Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOK(Event->Get(), "image/png", png1x1), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
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
        } else {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
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

        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + NJson::WriteJson(json, false), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void Timeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestParameters<TJsonRender> {
    static TString GetParameters() {
        return R"___([{"name":"target","in":"query","description":"metrics comma delimited","required":true,"type":"string"},
                      {"name":"from","in":"query","description":"time in seconds","required":false,"type":"integer"},
                      {"name":"until","in":"query","description":"time in seconds","required":false,"type":"integer"},
                      {"name":"maxDataPoints","in":"query","description":"maximum number of data points","required":false,"type":"integer"},
                      {"name":"format","in":"query","description":"response format","required":false,"type":"string"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonRender> {
    static TString GetSummary() {
        return "\"Graph data\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonRender> {
    static TString GetDescription() {
        return "\"Returns graph data in graphite format\"";
    }
};

}
}
