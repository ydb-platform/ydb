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
