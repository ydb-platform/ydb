#include "loopback_service.h"

#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/core/protos/services.pb.h>

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_W(stream) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)

namespace NYq {

class TLoopbackService : public NActors::TActorBootstrapped<TLoopbackService> {
public:
    TLoopbackService(
        const ::NMonitoring::TDynamicCounterPtr& counters)
        : ServiceCounters(counters->GetSubgroup("subsystem", "LoopbackService"))
    {
    }

    static constexpr char ActorName[] = "FQ_LOOPBACK_SERVICE";

    void Bootstrap() {
        Become(&TLoopbackService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvInternalService::TEvHealthCheckRequest, Handle)
        hFunc(TEvInternalService::TEvGetTaskRequest, Handle)
        hFunc(TEvInternalService::TEvPingTaskRequest, Handle)
        hFunc(TEvInternalService::TEvWriteResultRequest, Handle)

        hFunc(NYq::TEvControlPlaneStorage::TEvNodesHealthCheckResponse, Handle)
        hFunc(NYq::TEvControlPlaneStorage::TEvGetTaskResponse, Handle)
        hFunc(NYq::TEvControlPlaneStorage::TEvPingTaskResponse, Handle)
        hFunc(NYq::TEvControlPlaneStorage::TEvWriteResultDataResponse, Handle)
    );

    void Handle(TEvInternalService::TEvHealthCheckRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        auto request = ev->Get()->Request;
        Send(NYq::ControlPlaneStorageServiceActorId(), new NYq::TEvControlPlaneStorage::TEvNodesHealthCheckRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NYq::TEvControlPlaneStorage::TEvNodesHealthCheckResponse::TPtr& ev) {
        auto it = Senders.find(ev->Cookie);
        if (it != Senders.end()) {
            if (ev->Get()->Issues.Size() == 0) {
                Send(it->second, new TEvInternalService::TEvHealthCheckResponse(ev->Get()->Record), 0, OriginalCookies[ev->Cookie]);
            } else {
                auto issues = ev->Get()->Issues;
                Send(it->second, new TEvInternalService::TEvHealthCheckResponse(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)), 0, OriginalCookies[ev->Cookie]);
            }
            Senders.erase(it);
            OriginalCookies.erase(ev->Cookie);
        }
    }

    void Handle(TEvInternalService::TEvGetTaskRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        auto request = ev->Get()->Request;
        Send(NYq::ControlPlaneStorageServiceActorId(), new NYq::TEvControlPlaneStorage::TEvGetTaskRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NYq::TEvControlPlaneStorage::TEvGetTaskResponse::TPtr& ev) {
        auto it = Senders.find(ev->Cookie);
        if (it != Senders.end()) {
            if (ev->Get()->Issues.Size() == 0) {
                Send(it->second, new TEvInternalService::TEvGetTaskResponse(ev->Get()->Record), 0, OriginalCookies[ev->Cookie]);
            } else {
                auto issues = ev->Get()->Issues;
                Send(it->second, new TEvInternalService::TEvGetTaskResponse(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)), 0, OriginalCookies[ev->Cookie]);
            }
            Senders.erase(it);
            OriginalCookies.erase(ev->Cookie);
        }
    }

    void Handle(TEvInternalService::TEvPingTaskRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        OriginalCookies[Cookie] = ev->Cookie;
        auto request = ev->Get()->Request;
        Send(NYq::ControlPlaneStorageServiceActorId(), new NYq::TEvControlPlaneStorage::TEvPingTaskRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NYq::TEvControlPlaneStorage::TEvPingTaskResponse::TPtr& ev) {
        auto it = Senders.find(ev->Cookie);
        if (it != Senders.end()) {
            if (ev->Get()->Issues.Size() == 0) {
                Send(it->second, new TEvInternalService::TEvPingTaskResponse(ev->Get()->Record), 0, OriginalCookies[ev->Cookie]);
            } else {
                auto issues = ev->Get()->Issues;
                Send(it->second, new TEvInternalService::TEvPingTaskResponse(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)), 0, OriginalCookies[ev->Cookie]);
            }
            Senders.erase(it);
            OriginalCookies.erase(ev->Cookie);
        }
    }

    void Handle(TEvInternalService::TEvWriteResultRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        auto request = ev->Get()->Request;
        Send(NYq::ControlPlaneStorageServiceActorId(), new NYq::TEvControlPlaneStorage::TEvWriteResultDataRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NYq::TEvControlPlaneStorage::TEvWriteResultDataResponse::TPtr& ev) {
        auto it = Senders.find(ev->Cookie);
        if (it != Senders.end()) {
            if (ev->Get()->Issues.Size() == 0) {
                Send(it->second, new TEvInternalService::TEvWriteResultResponse(ev->Get()->Record), 0, OriginalCookies[ev->Cookie]);
            } else {
                auto issues = ev->Get()->Issues;
                Send(it->second, new TEvInternalService::TEvWriteResultResponse(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)), 0, OriginalCookies[ev->Cookie]);
            }
            Senders.erase(it);
            OriginalCookies.erase(ev->Cookie);
        }
    }

    const ::NMonitoring::TDynamicCounterPtr ServiceCounters;
    ui64 Cookie = 0;
    THashMap<ui64, NActors::TActorId> Senders;
    THashMap<ui64, ui64> OriginalCookies;
};

NActors::IActor* CreateLoopbackServiceActor(
    const ::NMonitoring::TDynamicCounterPtr& counters) {
        return new TLoopbackService(counters);
}

} /* NYq */
