#pragma once

#include <ydb/core/protos/mon.pb.h>

namespace NActors {

struct TEvMon {
    enum {
        EvMonitoringRequest = NActors::NMon::HttpInfo + 10,
        EvMonitoringResponse,
        EvRegisterHandler,
        EvMonitoringCancelRequest,
        EvCleanupProxy,
        End
    };

    static_assert(EvMonitoringRequest > NMon::End, "expect EvMonitoringRequest > NMon::End");
    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_MON), "expect End < EventSpaceEnd(NActors::TEvents::ES_MON)");

    struct TEvMonitoringRequest : TEventPB<TEvMonitoringRequest, NKikimrMonProto::TEvMonitoringRequest, EvMonitoringRequest> {
        TEvMonitoringRequest() = default;
    };

    struct TEvMonitoringResponse : TEventPB<TEvMonitoringResponse, NKikimrMonProto::TEvMonitoringResponse, EvMonitoringResponse> {
        TEvMonitoringResponse() = default;
    };

    struct TEvRegisterHandler : TEventLocal<TEvRegisterHandler, EvRegisterHandler> {
        TMon::TRegisterHandlerFields Fields;

        TEvRegisterHandler(const TMon::TRegisterHandlerFields& fields)
            : Fields(fields)
        {}
    };

    struct TEvMonitoringCancelRequest : TEventPB<TEvMonitoringCancelRequest, NKikimrMonProto::TEvMonitoringCancelRequest, EvMonitoringCancelRequest> {
        TEvMonitoringCancelRequest() = default;
    };

    struct TEvCleanupProxy : TEventLocal<TEvCleanupProxy, EvCleanupProxy> {
        TString Address;

        TEvCleanupProxy(const TString& address)
            : Address(address)
        {}
    };
};

} // namespace NActors
