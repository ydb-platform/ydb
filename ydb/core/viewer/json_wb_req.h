#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include "wb_merge.h"
#include "wb_group.h"
#include "wb_filter.h"
#include "wb_req.h"
#include "log.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

template<typename TRequestEventType, typename TResponseEventType>
class TJsonWhiteboardRequest : public TWhiteboardRequest<TJsonWhiteboardRequest<TRequestEventType, TResponseEventType>, TRequestEventType, TResponseEventType> {
protected:
    using TThis = TJsonWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TBase = TWhiteboardRequest<TThis, TRequestEventType, TResponseEventType>;
    using TResponseType = typename TResponseEventType::ProtoRecordType;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonWhiteboardRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        SplitIds(params.Get("node_id"), ',', TBase::RequestSettings.FilterNodeIds);
        {
            TString merge = params.Get("merge");
            if (merge.empty() || merge == "1" || merge == "true") {
                TBase::RequestSettings.MergeFields = TWhiteboardInfo<TResponseType>::GetDefaultMergeField();
            } else if (merge == "0" || merge == "false") {
                TBase::RequestSettings.MergeFields.clear();
            } else {
                TBase::RequestSettings.MergeFields = merge;
            }
        }
        TBase::RequestSettings.ChangedSince = FromStringWithDefault<ui64>(params.Get("since"), 0);
        TBase::RequestSettings.AliveOnly = FromStringWithDefault<bool>(params.Get("alive"), TBase::RequestSettings.AliveOnly);
        TBase::RequestSettings.GroupFields = params.Get("group");
        TBase::RequestSettings.FilterFields = params.Get("filter");
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        JsonSettings.EmptyRepeated = FromStringWithDefault<bool>(params.Get("empty_repeated"), false);
        TBase::RequestSettings.AllEnums = FromStringWithDefault<bool>(params.Get("all"), false);
        TBase::RequestSettings.Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        TBase::RequestSettings.Retries = FromStringWithDefault<ui32>(params.Get("retries"), 0);
        TBase::RequestSettings.RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("retry_period"), TBase::RequestSettings.RetryPeriod.MilliSeconds()));
        if (params.Has("static")) {
            TBase::RequestSettings.StaticNodesOnly = FromStringWithDefault<bool>(params.Get("static"), false);
        }
        TBase::RequestSettings.Format = params.Get("format");

        TBase::Bootstrap();
    }

    virtual void FilterResponse(TResponseType& response) {
        if (!TBase::RequestSettings.FilterFields.empty()) {
            FilterWhiteboardResponses(response, TBase::RequestSettings.FilterFields);
        }
        if (!TBase::RequestSettings.GroupFields.empty()) {
            GroupWhiteboardResponses(response, TBase::RequestSettings.GroupFields, TBase::RequestSettings.AllEnums);
        }
    }

    void RenderResponse(TStringStream& json, const TResponseType& response) {
        TProtoToJson::ProtoToJson(json, response, JsonSettings);
    }

    void ReplyAndPassAway() {
        try {
            TStringStream json;
            if (!TBase::RequestSettings.MergeFields.empty()) {
                ui32 errors = 0;
                TString error;
                if (!TBase::RequestSettings.FilterNodeIds.empty()) {
                    for (TNodeId nodeId : TBase::RequestSettings.FilterNodeIds) {
                        auto it = TBase::NodeErrors.find(nodeId);
                        if (it != TBase::NodeErrors.end()) {
                            if (error.empty()) {
                                error = it->second;
                            }
                            errors++;
                        }
                    }
                }
                if (errors > 0 && errors == TBase::RequestSettings.FilterNodeIds.size()) {
                    json << "{\"Error\":\"" << TProtoToJson::EscapeJsonString(error) << "\"}";
                } else {
                    TResponseType response;
                    MergeWhiteboardResponses(response, TBase::PerNodeStateInfo, TBase::RequestSettings.MergeFields); // PerNodeStateInfo will be invalidated
                    FilterResponse(response);
                    RenderResponse(json, response);
                }
            } else {
                json << '{';
                for (auto it = TBase::PerNodeStateInfo.begin(); it != TBase::PerNodeStateInfo.end(); ++it) {
                    if (it != TBase::PerNodeStateInfo.begin()) {
                        json << ',';
                    }
                    json << '"' << it->first << "\":";
                    TResponseType& response = it->second;
                    FilterResponse(response);
                    RenderResponse(json, response);
                }
                json << '}';
            }
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        } catch (const std::exception& e) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + e.what(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        }
        TBase::PassAway();
    }
};

template <typename RequestType, typename ResponseType>
struct TJsonRequestParameters<TJsonWhiteboardRequest<RequestType, ResponseType>> {
    static YAML::Node GetParameters() {
            return YAML::Load(R"___(
                    - name: node_id
                      in: query
                      description: node identifier
                      required: false
                      type: integer
                    - name: merge
                      in: query
                      description: merge information from nodes
                      required: false
                      type: boolean
                    - name: group
                      in: query
                      description: group information by field
                      required: false
                      type: string
                    - name: all
                      in: query
                      description: return all possible key combinations (for enums only)
                      required: false
                      type: boolean
                    - name: filter
                      in: query
                      description: filter information by field
                      required: false
                      type: string
                    - name: alive
                      in: query
                      description: request from alive (connected) nodes only
                      required: false
                      type: boolean
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
                    - name: retries
                      in: query
                      description: number of retries
                      required: false
                      type: integer
                    - name: retry_period
                      in: query
                      description: retry period in ms
                      required: false
                      type: integer
                      default: 500
                    - name: static
                      in: query
                      description: request from static nodes only
                      required: false
                      type: boolean
                    - name: since
                      in: query
                      description: filter by update time
                      required: false
                      type: string
            )___");
    }
};

template <typename RequestType, typename ResponseType>
struct TJsonRequestSchema<TJsonWhiteboardRequest<RequestType, ResponseType>> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<typename ResponseType::ProtoRecordType>();
    }
};

}
}
