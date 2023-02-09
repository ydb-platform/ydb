#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include "json_wb_req.h"

namespace NKikimr {
namespace NViewer {

template <>
struct TWhiteboardInfo<TEvWhiteboard::TEvPDiskStateResponse> {
    using TResponseType = TEvWhiteboard::TEvPDiskStateResponse;
    using TElementType = NKikimrWhiteboard::TPDiskStateInfo;
    using TElementKeyType = std::pair<ui32, ui32>;

    static constexpr bool StaticNodesOnly = true;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType* response) {
        return *response->Record.MutablePDiskStateInfo();
    }

    static std::pair<ui32, ui32> GetElementKey(const TElementType& type) {
        return std::make_pair(type.GetNodeId(), type.GetPDiskId());
    }

    static TString GetDefaultMergeField() {
        return "NodeId,PDiskId";
    }

    static THolder<TResponseType> MergeResponses(TMap<ui32, THolder<TResponseType>>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            return TWhiteboardMerger<TResponseType>::MergeResponsesElementKey(responses);
        } else {
            return TWhiteboardMerger<TResponseType>::MergeResponses(responses, fields);
        }
    }
};

using TJsonPDiskInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvPDiskStateRequest, TEvWhiteboard::TEvPDiskStateResponse>;

template <>
struct TJsonRequestSummary<TJsonPDiskInfo> {
    static TString GetSummary() {
        return "\"PDisk information\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonPDiskInfo> {
    static TString GetDescription() {
        return "\"Returns PDisk information\"";
    }
};

}
}
