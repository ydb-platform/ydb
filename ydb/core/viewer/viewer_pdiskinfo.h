#pragma once
#include "json_wb_req.h"

namespace NKikimr::NViewer {

template <>
struct TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse> {
    using TResponseType = NKikimrWhiteboard::TEvPDiskStateResponse;
    using TResponseEventType = TEvWhiteboard::TEvPDiskStateResponse;
    using TElementType = NKikimrWhiteboard::TPDiskStateInfo;
    using TElementKeyType = std::pair<ui32, ui32>;

    static constexpr bool StaticNodesOnly = true;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType& response) {
        return *response.MutablePDiskStateInfo();
    }

    static std::pair<ui32, ui32> GetElementKey(const TElementType& type) {
        return std::make_pair(type.GetNodeId(), type.GetPDiskId());
    }

    static TString GetDefaultMergeField() {
        return "NodeId,PDiskId";
    }

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            TWhiteboardMerger<TResponseType>::MergeResponsesElementKey(result, responses);
        } else {
            TWhiteboardMerger<TResponseType>::MergeResponses(result, responses, fields);
        }
    }
};

using TJsonPDiskInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvPDiskStateRequest, TEvWhiteboard::TEvPDiskStateResponse>;

}
