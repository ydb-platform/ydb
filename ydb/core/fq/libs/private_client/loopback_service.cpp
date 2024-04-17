#include "loopback_service.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_config/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_W(stream) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)

namespace NFq {

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
        hFunc(NFq::TEvControlPlaneConfig::TEvGetTenantInfoResponse, Handle)
        hFunc(TEvInternalService::TEvPingTaskRequest, Handle)
        hFunc(TEvInternalService::TEvWriteResultRequest, Handle)
        hFunc(TEvInternalService::TEvCreateRateLimiterResourceRequest, Handle)
        hFunc(TEvInternalService::TEvDeleteRateLimiterResourceRequest, Handle)

        hFunc(NFq::TEvControlPlaneStorage::TEvNodesHealthCheckResponse, Handle)
        hFunc(NFq::TEvControlPlaneStorage::TEvGetTaskResponse, Handle)
        hFunc(NFq::TEvControlPlaneStorage::TEvPingTaskResponse, Handle)
        hFunc(NFq::TEvControlPlaneStorage::TEvWriteResultDataResponse, Handle)
        hFunc(NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse, Handle)
        hFunc(NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse, Handle)
    );

    void Handle(TEvInternalService::TEvHealthCheckRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        OriginalCookies[Cookie] = ev->Cookie;
        auto request = ev->Get()->Request;
        Send(NFq::ControlPlaneStorageServiceActorId(), new NFq::TEvControlPlaneStorage::TEvNodesHealthCheckRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NFq::TEvControlPlaneStorage::TEvNodesHealthCheckResponse::TPtr& ev) {
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
        OriginalCookies[Cookie] = ev->Cookie;
        auto request = ev->Get()->Request;
        GetRequests.emplace(Cookie, std::move(request));
        Send(NFq::ControlPlaneConfigActorId(), new NFq::TEvControlPlaneConfig::TEvGetTenantInfoRequest(), 0, Cookie);
    }

    void Handle(NFq::TEvControlPlaneConfig::TEvGetTenantInfoResponse::TPtr& ev) {
        TenantInfo = ev->Get()->TenantInfo;
        auto it = GetRequests.find(ev->Cookie);
        if (it != GetRequests.end()) {
            auto request = it->second;
            GetRequests.erase(it);
            auto event = std::make_unique<NFq::TEvControlPlaneStorage::TEvGetTaskRequest>(std::move(request));
            event->TenantInfo = TenantInfo;
            Send(NFq::ControlPlaneStorageServiceActorId(), event.release(), 0, ev->Cookie);
        }
    }

    void Handle(NFq::TEvControlPlaneStorage::TEvGetTaskResponse::TPtr& ev) {
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
        auto event = std::make_unique<NFq::TEvControlPlaneStorage::TEvPingTaskRequest>(std::move(request));
        event->TenantInfo = TenantInfo;
        Send(NFq::ControlPlaneStorageServiceActorId(), event.release(), 0, Cookie);
    }

    void Handle(NFq::TEvControlPlaneStorage::TEvPingTaskResponse::TPtr& ev) {
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
        OriginalCookies[Cookie] = ev->Cookie;
        auto request = ev->Get()->Request;
        Send(NFq::ControlPlaneStorageServiceActorId(), new NFq::TEvControlPlaneStorage::TEvWriteResultDataRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NFq::TEvControlPlaneStorage::TEvWriteResultDataResponse::TPtr& ev) {
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

    void Handle(TEvInternalService::TEvCreateRateLimiterResourceRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        OriginalCookies[Cookie] = ev->Cookie;
        auto request = ev->Get()->Request;
        Send(NFq::ControlPlaneStorageServiceActorId(), new NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse::TPtr& ev) {
        auto it = Senders.find(ev->Cookie);
        if (it != Senders.end()) {
            if (ev->Get()->Issues.Size() == 0) {
                Send(it->second, new TEvInternalService::TEvCreateRateLimiterResourceResponse(ev->Get()->Record), 0, OriginalCookies[ev->Cookie]);
            } else {
                auto issues = ev->Get()->Issues;
                Send(it->second, new TEvInternalService::TEvCreateRateLimiterResourceResponse(NYdb::EStatus::BAD_REQUEST, std::move(issues)), 0, OriginalCookies[ev->Cookie]);
            }
            Senders.erase(it);
            OriginalCookies.erase(ev->Cookie);
        }
    }

    void Handle(TEvInternalService::TEvDeleteRateLimiterResourceRequest::TPtr& ev) {
        Cookie++;
        Senders[Cookie] = ev->Sender;
        OriginalCookies[Cookie] = ev->Cookie;
        auto request = ev->Get()->Request;
        Send(NFq::ControlPlaneStorageServiceActorId(), new NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest(std::move(request)), 0, Cookie);
    }

    void Handle(NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse::TPtr& ev) {
        auto it = Senders.find(ev->Cookie);
        if (it != Senders.end()) {
            if (ev->Get()->Issues.Size() == 0) {
                Send(it->second, new TEvInternalService::TEvDeleteRateLimiterResourceResponse(ev->Get()->Record), 0, OriginalCookies[ev->Cookie]);
            } else {
                auto issues = ev->Get()->Issues;
                Send(it->second, new TEvInternalService::TEvDeleteRateLimiterResourceResponse(NYdb::EStatus::BAD_REQUEST, std::move(issues)), 0, OriginalCookies[ev->Cookie]);
            }
            Senders.erase(it);
            OriginalCookies.erase(ev->Cookie);
        }
    }

    const ::NMonitoring::TDynamicCounterPtr ServiceCounters;
    ui64 Cookie = 0;
    THashMap<ui64, NActors::TActorId> Senders;
    THashMap<ui64, ui64> OriginalCookies;
    THashMap<ui64, Fq::Private::GetTaskRequest> GetRequests;
    NFq::TTenantInfo::TPtr TenantInfo;
};

NActors::IActor* CreateLoopbackServiceActor(
    const ::NMonitoring::TDynamicCounterPtr& counters) {
        return new TLoopbackService(counters);
}

} /* NFq */
