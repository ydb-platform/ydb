#pragma once

#include "json_wb_req.h"

namespace NKikimr::NViewer {

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

private:
    static const TWhiteboardMergerBase::TRegistrator Registrator;
};

using TJsonNodeInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvNodeStateRequest, TEvWhiteboard::TEvNodeStateResponse>;

} // namespace NKikimr::NViewer
