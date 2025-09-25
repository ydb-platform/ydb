#pragma once

#include <ydb/core/protos/mon.pb.h>

namespace NMonitoring::NPrivate {

struct TEvMon {
    enum {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvMonitoringRequest = EvBegin,
        EvMonitoringResponse,
        EvRegisterHandler,
        EvMonitoringCancelRequest,
        EvCleanupProxy,

        End
    };

    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect End < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvMonitoringRequest : NActors::TEventPB<TEvMonitoringRequest, NKikimrMonProto::TEvMonitoringRequest, EvMonitoringRequest> {
        TEvMonitoringRequest() = default;
    };

    struct TEvMonitoringResponse : NActors::TEventPB<TEvMonitoringResponse, NKikimrMonProto::TEvMonitoringResponse, EvMonitoringResponse> {
        TEvMonitoringResponse() = default;
    };

    struct TEvRegisterHandler : NActors::TEventLocal<TEvRegisterHandler, EvRegisterHandler> {
        NActors::TMon::TRegisterHandlerFields Fields;

        TEvRegisterHandler(const NActors::TMon::TRegisterHandlerFields& fields)
            : Fields(fields)
        {}
    };

    struct TEvMonitoringCancelRequest : NActors::TEventPB<TEvMonitoringCancelRequest, NKikimrMonProto::TEvMonitoringCancelRequest, EvMonitoringCancelRequest> {
        TEvMonitoringCancelRequest() = default;
    };

    struct TEvCleanupProxy : NActors::TEventLocal<TEvCleanupProxy, EvCleanupProxy> {
        TString Address;

        TEvCleanupProxy(const TString& address)
            : Address(address)
        {}
    };
};

} // namespace NMonitoring::NPrivate
