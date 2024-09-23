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

    static void InitMerger() {
        const auto* field = NKikimrWhiteboard::TBSGroupStateInfo::descriptor()->FindFieldByName("Latency");
        TWhiteboardMergerBase::FieldMerger[field] = &TWhiteboardMergerBase::ProtoMaximizeEnumField;
    }

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            TWhiteboardMerger<TResponseType>::MergeResponsesElementKey(result, responses);
        } else {
            TWhiteboardMerger<TResponseType>::MergeResponses(result, responses, fields);
        }
    }
};

template <>
struct TWhiteboardMergerComparator<NKikimrWhiteboard::TBSGroupStateInfo> {
    bool operator ()(const NKikimrWhiteboard::TBSGroupStateInfo& a, const NKikimrWhiteboard::TBSGroupStateInfo& b) const {
        return std::make_tuple(a.GetGroupGeneration(), a.VDiskIdsSize(), a.GetChangeTime())
                < std::make_tuple(b.GetGroupGeneration(), b.VDiskIdsSize(), b.GetChangeTime());
    }
};

using TJsonBSGroupInfo = TJsonWhiteboardRequest<TEvWhiteboard::TEvBSGroupStateRequest, TEvWhiteboard::TEvBSGroupStateResponse>;

template <>
struct TJsonRequestSummary<TJsonBSGroupInfo> {
    static TString GetSummary() {
        return "Storage groups information";
    }
};

template <>
struct TJsonRequestDescription<TJsonBSGroupInfo> {
    static TString GetDescription() {
        return "Returns information about storage groups";
    }
};

}
}
