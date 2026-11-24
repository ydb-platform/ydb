#pragma once

#include "defs.h"
#include "events.h"

#include <ydb/core/protos/reassign_tablets.pb.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NHive {

struct TEvReassignTablets {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_REASSIGN_TABLETS),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,
        EvCancelRequest,
        EvCancelResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REASSIGN_TABLETS));

    struct TEvCreateRequest : TEventPB<TEvCreateRequest, NKikimrReassignTablets::TEvCreateRequest, EvCreateRequest> {};

    struct TEvCreateResponse : TEventPB<TEvCreateResponse, NKikimrReassignTablets::TEvCreateResponse, EvCreateResponse> {};

    struct TEvGetRequest : TEventPB<TEvGetRequest, NKikimrReassignTablets::TEvGetRequest, EvGetRequest> {
        TEvGetRequest() = default;

        TEvGetRequest(ui64 reassignId, ui64 hiveGeneration) {
            Record.SetReassignId(reassignId);
            Record.SetHiveGeneration(hiveGeneration);
        }
    };

    struct TEvGetResponse : TEventPB<TEvGetResponse, NKikimrReassignTablets::TEvGetResponse, EvGetResponse> {};

    struct TEvCancelRequest : TEventPB<TEvCancelRequest, NKikimrReassignTablets::TEvCancelRequest, EvCancelRequest> {
        TEvCancelRequest() = default;

        TEvCancelRequest(ui64 reassignId, ui64 hiveGeneration, const TString& userSID) {
            Record.SetReassignId(reassignId);
            Record.SetHiveGeneration(hiveGeneration);
            Record.SetUserSID(userSID);
        }
    };

    struct TEvCancelResponse : TEventPB<TEvCancelResponse, NKikimrReassignTablets::TEvCancelResponse, EvCancelResponse> {};
};

} // namespace NKikimr::NHive
