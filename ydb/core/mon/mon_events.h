#pragma once

#include <ydb/core/protos/mon.pb.h>

namespace NMonitoring {

struct TEvMon {
    enum {
        EvMonitoringRequest = NActors::NMon::HttpInfo + 10,
        EvMonitoringResponse,
        EvRegisterHandler,
        EvMonitoringCancelRequest,
        EvCleanupProxy,
        End
    };

    static_assert(EvMonitoringRequest > NActors::NMon::End, "expect EvMonitoringRequest > NMon::End");
    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_MON), "expect End < EventSpaceEnd(TEvents::ES_MON)");

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

} // namespace NMonitoring
