#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/events.h>
#include <library/cpp/json/json_writer.h>
#include "viewer_request.h"
#include "viewer.h"
#include "log.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NMonitoring;

class TJsonRender : public TActorBootstrapped<TJsonRender> {
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TEvViewer::TEvViewerRequest::TPtr ViewerRequest;
    std::vector<TString> Metrics;
    TString Database;
    TCgiParameters Params;

    std::optional<TNodeId> SubscribedNodeId;
    std::vector<TNodeId> TenantDynamicNodes;
    bool Direct = false;
    bool MadeProxyRequest = false;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonRender(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {
        UserToken = Event->Get()->UserToken;
    }

    TJsonRender(TEvViewer::TEvViewerRequest::TPtr& ev)
        : ViewerRequest(ev)
    {
        auto& request = ViewerRequest->Get()->Record.GetRenderRequest();

        UserToken = request.GetUserToken();
        Direct = true;
    }

    void Bootstrap() {
        auto postData = Event
            ? Event->Get()->Request.GetPostContent()
            : ViewerRequest->Get()->Record.GetRenderRequest().GetUri();
        BLOG_D("PostData=" << postData);
        NKikimrGraph::TEvGetMetrics getRequest;
        if (postData) {
            Params = TCgiParameters(postData);
            Database = Params.Get("database");
            Direct = FromStringWithDefault<bool>(Params.Get("direct"), Direct);
            if (Database && !Direct) {
                RequestStateStorageEndpointsLookup(Database); // to find some dynamic node and redirect query there
            }
            if (Requests == 0) {
                SendGraphRequest();
            }
        } else {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }


        Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(NGraph::TEvGraph::TEvMetricsResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void SendDynamicNodeRenderRequest() {
        ui64 hash = std::hash<TString>()(Event->Get()->Request.GetRemoteAddr());

        auto itPos = std::next(TenantDynamicNodes.begin(), hash % TenantDynamicNodes.size());
        std::nth_element(TenantDynamicNodes.begin(), itPos, TenantDynamicNodes.end());

        TNodeId nodeId = *itPos;
        SubscribedNodeId = nodeId;
        TActorId viewerServiceId = MakeViewerID(nodeId);

        THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
        request->Record.SetTimeout(Timeout);
        auto queryRequest = request->Record.MutableQueryRequest();
        queryRequest->SetUri(TString(Event->Get()->Request.GetUri()));
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            queryRequest->SetContent(TString(content));
        }
        if (UserToken) {
            queryRequest->SetUserToken(UserToken);
        }

        ViewerWhiteboardCookie cookie(NKikimrViewer::TEvViewerRequest::kQueryRequest, nodeId);
        SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            for (const auto& [actorId, infoEntry] : ev->Get()->InfoEntries) {
                TenantDynamicNodes.emplace_back(actorId.NodeId());
            }
        }
        if (TenantDynamicNodes.empty()) {
            SendGraphRequest();
        } else {
            SendDynamicNodeRenderRequest();
        }
    }

    void SendGraphRequest() {
        if (MadeProxyRequest) {
            return;
        }
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
            //StringSplitter(Params.Get("target")).Split(',').SkipEmpty().Collect(&Metrics);
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

    void HandleRenderResponse(NKikimrKqp::TEvRenderResponse& response) {
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

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (Event) {
            HandleRenderResponse(ev->Get()->Record.GetRef());
        } else {
            TEvViewer::TEvViewerResponse* response = new TEvViewer::TEvViewerResponse();
            response->Record.MutableRenderResponse()->CopyFrom(record);
            Send(Event->Sender, response, 0, NMon::IEvHttpInfoRes::EContentType::Custom);
            PassAway();
        }
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        HandleRenderResponse(*(ev.Get()->Get()->Record.MutableRenderResponse()));
    }

    void Timeout() {
        if (Event) {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        } else {
            auto* response = new TEvViewer::TEvViewerResponse();
            response->Record.MutableRenderResponse()->SetYdbStatus(Ydb::StatusIds::TIMEOUT);
            Send(Event->Sender, response, 0, NMon::IEvHttpInfoRes::EContentType::Custom);
        }
        PassAway();
    }
};

template <>
struct TJsonRequestParameters<TJsonRender> {
    static TString GetParameters() {
        return R"___([{"name":"target","in":"query","description":"metrics comma delimited","required":true,"type":"string"},
                      {"name":"from","in":"query","description":"time in seconds","required":false,"type":"integer"},
                      {"name":"database","in":"query","description":"database name","required":false,"type":"string"},
                      {"name":"direct","in":"query","description":"force processing query on current node","required":false,"type":"boolean"},
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
