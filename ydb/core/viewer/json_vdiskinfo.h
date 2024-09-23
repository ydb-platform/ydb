#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/tuples.h>
#include "json_wb_req.h"

namespace std {

template <>
struct equal_to<NKikimrBlobStorage::TVDiskID> {
    static decltype(auto) make_tuple(const NKikimrBlobStorage::TVDiskID& id) {
        return std::make_tuple(
                    id.GetGroupID(),
                    id.GetGroupGeneration(),
                    id.GetRing(),
                    id.GetDomain(),
                    id.GetVDisk()
                    );
    }

    bool operator ()(const NKikimrBlobStorage::TVDiskID& a, const NKikimrBlobStorage::TVDiskID& b) const {
        return make_tuple(a) == make_tuple(b);
    }
};

template <>
struct less<NKikimrBlobStorage::TVDiskID> {
    bool operator ()(const NKikimrBlobStorage::TVDiskID& a, const NKikimrBlobStorage::TVDiskID& b) const {
        return equal_to<NKikimrBlobStorage::TVDiskID>::make_tuple(a) < equal_to<NKikimrBlobStorage::TVDiskID>::make_tuple(b);
    }
};

template <>
struct hash<NKikimrBlobStorage::TVDiskID> {
    size_t operator ()(const NKikimrBlobStorage::TVDiskID& a) const {
        auto tp = equal_to<NKikimrBlobStorage::TVDiskID>::make_tuple(a);
        return hash<decltype(tp)>()(tp);
    }
};

}

namespace NKikimr {
namespace NViewer {

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

template <>
struct TJsonRequestSummary<TJsonVDiskInfo> {
    static TString GetSummary() {
        return "VDisk information";
    }
};

template <>
struct TJsonRequestDescription<TJsonVDiskInfo> {
    static TString GetDescription() {
        return "VDisk information";
    }
};

}
}
