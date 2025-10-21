#pragma once
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include "wb_filter.h"
#include "wb_group.h"
#include "wb_merge.h"
#include "wb_req.h"
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

YAML::Node GetWhiteboardRequestParameters();

template<typename TRequestEventType, typename TResponseEventType>
class TJsonWhiteboardRequest : public TWhiteboardRequest<TRequestEventType, TResponseEventType> {
public:
    using TThis = TJsonWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TBase = TWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TResponseType = typename TResponseEventType::ProtoRecordType;
    using TBase::Event;
    using TBase::ReplyAndPassAway;
    TJsonSettings JsonSettings;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonWhiteboardRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        std::vector<TNodeId> nodeIds;
        SplitIds(TBase::Params.Get("node_id"), ',', nodeIds);
        std::replace(nodeIds.begin(),
                     nodeIds.end(),
                     (TNodeId)0,
                     TlsActivationContext->ActorSystem()->NodeId);
        if (!nodeIds.empty()) {
            if (TBase::RequestSettings.FilterNodeIds.empty()) {
                TBase::RequestSettings.FilterNodeIds = nodeIds;
            } else {
                std::sort(nodeIds.begin(), nodeIds.end());
                std::sort(TBase::RequestSettings.FilterNodeIds.begin(), TBase::RequestSettings.FilterNodeIds.end());
                std::vector<TNodeId> intersection;
                std::set_intersection(nodeIds.begin(), nodeIds.end(), TBase::RequestSettings.FilterNodeIds.begin(), TBase::RequestSettings.FilterNodeIds.end(), std::back_inserter(intersection));
                if (intersection.empty()) {
                    TBase::RequestSettings.FilterNodeIds = {0};
                } else {
                    TBase::RequestSettings.FilterNodeIds = intersection;
                }
            }
        }
        if (TBase::IsDatabaseRequest()) {
            auto nodes = TBase::GetDatabaseNodes();
            if (TBase::RequestSettings.FilterNodeIds.empty()) {
                TBase::RequestSettings.FilterNodeIds = std::move(nodes);
            } else {
                auto nodesSet = std::unordered_set<TNodeId>(nodes.begin(), nodes.end());
                TBase::RequestSettings.FilterNodeIds.erase(
                    std::remove_if(TBase::RequestSettings.FilterNodeIds.begin(), TBase::RequestSettings.FilterNodeIds.end(),
                    [&nodesSet](TNodeId nodeId) { return nodesSet.count(nodeId) == 0; }),
                    TBase::RequestSettings.FilterNodeIds.end());
                if (TBase::RequestSettings.FilterNodeIds.empty()) {
                    return ReplyAndPassAway();
                }
            }
        }
        {
            TString merge = TBase::Params.Get("merge");
            if (merge.empty() || merge == "1" || merge == "true") {
                TBase::RequestSettings.MergeFields = TWhiteboardInfo<TResponseType>::GetDefaultMergeField();
            } else if (merge == "0" || merge == "false") {
                TBase::RequestSettings.MergeFields.clear();
            } else {
                TBase::RequestSettings.MergeFields = merge;
            }
        }
        TBase::RequestSettings.ChangedSince = FromStringWithDefault<ui64>(TBase::Params.Get("since"), 0);
        TBase::RequestSettings.AliveOnly = FromStringWithDefault<bool>(TBase::Params.Get("alive"), TBase::RequestSettings.AliveOnly);
        TBase::RequestSettings.GroupFields = TBase::Params.Get("group");
        TBase::RequestSettings.FilterFields = TBase::Params.Get("filter");
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(TBase::Params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(TBase::Params.Get("ui64"), false);
        JsonSettings.EmptyRepeated = FromStringWithDefault<bool>(TBase::Params.Get("empty_repeated"), false);
        TBase::RequestSettings.AllEnums = FromStringWithDefault<bool>(TBase::Params.Get("all"), false);
        TBase::RequestSettings.Timeout = FromStringWithDefault<ui32>(TBase::Params.Get("timeout"), 10000);
        TBase::RequestSettings.Retries = FromStringWithDefault<ui32>(TBase::Params.Get("retries"), 0);
        TBase::RequestSettings.RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(TBase::Params.Get("retry_period"), TBase::RequestSettings.RetryPeriod.MilliSeconds()));
        if (TBase::Params.Has("static")) {
            TBase::RequestSettings.StaticNodesOnly = FromStringWithDefault<bool>(TBase::Params.Get("static"), false);
        }
        if (TBase::Params.Has("fields_required")) {
            if (TBase::Params.Get("fields_required") == "all") {
                TBase::RequestSettings.FieldsRequired = {-1};
            } else {
                SplitIds(TBase::Params.Get("fields_required"), ',', TBase::RequestSettings.FieldsRequired);
            }
        }
        TBase::RequestSettings.Format = TBase::Params.Get("format");
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

    void ReplyAndPassAway() override {
        try {
            auto perNodeStateInfo = TBase::GetPerNodeStateInfo();
            TStringStream json;
            if (!TBase::RequestSettings.MergeFields.empty()) {
                ui32 errors = 0;
                TString error;
                if (!TBase::RequestSettings.FilterNodeIds.empty()) {
                    for (TNodeId nodeId : TBase::RequestSettings.FilterNodeIds) {
                        auto it = TBase::NodeResponses.find(nodeId);
                        if (it != TBase::NodeResponses.end()) {
                            if (it->second.IsError()) {
                                if (error.empty()) {
                                    error = it->second.GetError();
                                }
                                errors++;
                            }
                        }
                    }
                }
                if (errors > 0 && errors == TBase::RequestSettings.FilterNodeIds.size()) {
                    json << "{\"Error\":\"" << TProtoToJson::EscapeJsonString(error) << "\"}";
                } else {
                    TResponseType response;
                    MergeWhiteboardResponses(response, perNodeStateInfo, TBase::RequestSettings.MergeFields);
                    FilterResponse(response);
                    RenderResponse(json, response);
                }
            } else {
                json << '{';
                for (auto it = perNodeStateInfo.begin(); it != perNodeStateInfo.end(); ++it) {
                    if (it != perNodeStateInfo.begin()) {
                        json << ',';
                    }
                    json << '"' << it->first << "\":";
                    TResponseType& response = it->second;
                    FilterResponse(response);
                    RenderResponse(json, response);
                }
                json << '}';
            }
            ReplyAndPassAway(TBase::GetHTTPOKJSON(json.Str()));
        } catch (const std::exception& e) {
            ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", e.what()));
        }
    }
};

}
