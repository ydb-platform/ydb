#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include "wb_merge.h"
#include "json_wb_req.h"

namespace NKikimr {
namespace NViewer {

template <>
struct TWhiteboardInfo<NKikimrWhiteboard::TEvNodeStateResponse> {
    using TResponseType = NKikimrWhiteboard::TEvNodeStateResponse;
    using TElementType = NKikimrWhiteboard::TNodeStateInfo;
    using TElementKeyType = TString;

    static constexpr bool StaticNodesOnly = false;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType& response) {
        return *response.MutableNodeStateInfo();
    }

    static const TString& GetElementKey(const TElementType& type) {
        return type.GetPeerName();
    }

    static TString GetDefaultMergeField() {
        return "PeerName";
    }

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields = GetDefaultMergeField()) {
        TWhiteboardMerger<TResponseType>::MergeResponses(result, responses, fields);
    }

    static void InitMerger() {
        const auto* field = NKikimrWhiteboard::TNodeStateInfo::descriptor()->FindFieldByName("ConnectStatus");
        TWhiteboardMergerBase::FieldMerger[field] = &TWhiteboardMergerBase::ProtoMaximizeEnumField;
        field = NKikimrWhiteboard::TNodeStateInfo::descriptor()->FindFieldByName("Connected");
        TWhiteboardMergerBase::FieldMerger[field] = &TWhiteboardMergerBase::ProtoMaximizeBoolField;
    }
};

using TJsonNodeInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvNodeStateRequest, TEvWhiteboard::TEvNodeStateResponse>;

template <>
struct TJsonRequestSummary<TJsonNodeInfo> {
    static TString GetSummary() {
        return "Interconnect information";
    }
};

template <>
struct TJsonRequestDescription<TJsonNodeInfo> {
    static TString GetDescription() {
        return "Returns information about node connections";
    }
};

}
}
