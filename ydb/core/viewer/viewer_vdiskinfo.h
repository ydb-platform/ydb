#pragma once
#include "json_wb_req.h"
#include "viewer_helper.h"

namespace NKikimr::NViewer {

template <>
struct TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse> {
    using TResponseEventType = TEvWhiteboard::TEvVDiskStateResponse;
    using TResponseType = NKikimrWhiteboard::TEvVDiskStateResponse;
    using TElementType = NKikimrWhiteboard::TVDiskStateInfo;
    using TElementKeyType = NKikimrBlobStorage::TVDiskID;

    static constexpr bool StaticNodesOnly = true;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType& response) {
        return *response.MutableVDiskStateInfo();
    }

    static const NKikimrBlobStorage::TVDiskID& GetElementKey(const TElementType& type) {
        return type.GetVDiskId();
    }

    static TString GetDefaultMergeField() {
        return "VDiskId";
    }

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            TWhiteboardMerger<TResponseType>::MergeResponsesElementKey(result, responses);
        } else {
            TWhiteboardMerger<TResponseType>::MergeResponses(result, responses, fields);
        }
    }
};

using TJsonVDiskInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvVDiskStateRequest, TEvWhiteboard::TEvVDiskStateResponse>;

}
