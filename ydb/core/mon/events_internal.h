#pragma once

#include <ydb/core/protos/mon.pb.h>

namespace NMonitoring::NPrivate {

using namespace NActors;

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

} // namespace NMonImpl
