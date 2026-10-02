#pragma once

#include "json_wb_req.h"

namespace NKikimr::NViewer {

template <>
struct TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse> {
    using TResponseType = NKikimrWhiteboard::TEvBSGroupStateResponse;
    using TResponseEventType = TEvWhiteboard::TEvBSGroupStateResponse;
    using TElementType = NKikimrWhiteboard::TBSGroupStateInfo;
    using TElementKeyType = ui32;

    static constexpr bool StaticNodesOnly = true;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType& response) {
        return *response.MutableBSGroupStateInfo();
    }

    static ui32 GetElementKey(const TElementType& type) {
        return type.GetGroupID();
    }

    static TString GetDefaultMergeField() {
        return "GroupID";
    }

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            TWhiteboardMerger<TResponseType>::MergeResponsesElementKey(result, responses);
        } else {
            TWhiteboardMerger<TResponseType>::MergeResponses(result, responses, fields);
        }
    }

private:
    static const TWhiteboardMergerBase::TRegistrator Registrator;
};

template <>
struct TWhiteboardMergerComparator<NKikimrWhiteboard::TBSGroupStateInfo> {
    bool operator ()(const NKikimrWhiteboard::TBSGroupStateInfo& a, const NKikimrWhiteboard::TBSGroupStateInfo& b) const {
        return std::make_tuple(a.GetGroupGeneration(), a.VDiskIdsSize(), a.GetChangeTime())
                < std::make_tuple(b.GetGroupGeneration(), b.VDiskIdsSize(), b.GetChangeTime());
    }
};

using TJsonBSGroupInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvBSGroupStateRequest, TEvWhiteboard::TEvBSGroupStateResponse>;

} // namespace NKikimr::NViewer
