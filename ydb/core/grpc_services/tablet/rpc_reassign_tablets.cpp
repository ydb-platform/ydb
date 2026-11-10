#include "rpc_reassign_tablets.h"
#include "service_tablet.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/string/vector.h>

namespace NKikimr::NGRpcService {

template <bool Stop>
using TReassignRequest = std::conditional_t<Stop, TEvStopReassignTabletsRequest, TEvReassignTabletsRequest>;

template <bool Stop>
class TRpcReassignTablets : public TRpcRequestActor<TRpcReassignTablets<Stop>, TReassignRequest<Stop>> {
    using TThis = TRpcReassignTablets<Stop>;
    using TBase = TRpcRequestActor<TThis, TReassignRequest<Stop>>;
    using TResponse = typename TBase::TResponse;
    using TBase::GetProtoRequest;
    using TBase::Reply;
    using TBase::Request;
    using TBase::UserToken;

public:
    using TBase::TBase;

    void Bootstrap() {
        if (!IsAdministrator(AppData(), UserToken.Get())) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, "Cluster administrator access is required");
        }
        const auto& request = *GetProtoRequest();
        if constexpr (Stop) {
            const auto parts = SplitString(request.id(), ":");
            if (parts.size() != 3 || !TryFromString(parts[0], HiveId)
                    || !TryFromString(parts[1], HiveGeneration) || !TryFromString(parts[2], ReassignId)
                    || !HiveId || !HiveGeneration || !ReassignId) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid reassignment id");
            }
            Request->AddAuditLogPart("reassign_id", request.id());
        } else {
            if (request.database().empty()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "database must not be empty");
            }
            if (request.storage_pool().empty() && !request.group()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "storage_pool or group must be specified");
            }
            if (request.has_max_in_flight() && request.max_in_flight() == 0) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "max_in_flight must be positive");
            }
            if (request.has_percent() && request.percent() > 100) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "percent must be between 0 and 100");
            }
            Request->AddAuditLogPart("database", request.database());
            Request->AddAuditLogPart("storage_pool", request.storage_pool());
            Request->AddAuditLogPart("group", ToString(request.group()));
        }

        this->Become(&TThis::StateWork);
        this->Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup);
        if constexpr (Stop) {
            ConnectHive();
        } else {
            auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
            auto& entry = navigate->ResultSet.emplace_back();
            entry.Path = NKikimr::SplitPath(request.database());
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RedirectRequired = false;
            this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
        }
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(this->SelfId(), HivePipeClient);
        TBase::PassAway();
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvHive::TEvReassignTabletsReply, Handle);
            hFunc(TEvHive::TEvStopReassignTabletsReply, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void ConnectHive() {
        Request->AddAuditLogPart("hive_id", ToString(HiveId));
        NTabletPipe::TClientConfig config;
        config.RetryPolicy.RetryLimitCount = 3;
        HivePipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), HiveId, config));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& navigate = *ev->Get()->Request;
        if (navigate.ResultSet.size() != 1) {
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database lookup response");
        }
        const auto& entry = navigate.ResultSet.front();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return Reply(Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve database");
        }
        if (!entry.DomainInfo || !entry.DomainDescription) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "database must name a database root");
        }
        HiveId = entry.DomainInfo->ExtractHive();
        if (!HiveId && !entry.DomainInfo->IsServerless()) {
            HiveId = AppData()->DomainsInfo->GetHive();
        }
        if (!HiveId) {
            return Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Database has no Hive in its metadata");
        }
        ConnectHive();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->ClientId != HivePipeClient) {
            return;
        }
        if (msg->Status != NKikimrProto::OK) {
            return Reply(Ydb::StatusIds::UNAVAILABLE, "Cannot connect to Hive");
        }
        if (Submitted) {
            return;
        }
        using THiveRequest = std::conditional_t<Stop, TEvHive::TEvStopReassignTablets, TEvHive::TEvReassignTablets>;
        auto event = std::make_unique<THiveRequest>();
        auto& record = event->Record;
        if constexpr (Stop) {
            record.SetReassignId(ReassignId);
            record.SetHiveGeneration(HiveGeneration);
        } else {
            const auto& request = *GetProtoRequest();
            record.SetStoragePool(request.storage_pool());
            record.SetGroupId(request.group());
            if (request.has_max_in_flight()) {
                record.SetMaxInFlight(request.max_in_flight());
            }
            if (request.has_percent()) {
                record.SetPercent(request.percent());
            }
            record.SetAsync(request.async());
        }
        if (UserToken) {
            record.SetUserSID(UserToken->GetUserSID());
        }
        Submitted = true;
        NTabletPipe::SendData(this->SelfId(), HivePipeClient, event.release());
    }

    void Handle(TEvHive::TEvReassignTabletsReply::TPtr& ev) {
        if constexpr (!Stop) {
            const auto& record = ev->Get()->Record;
            TResponse response;
            response.set_status(record.GetStatus() == NKikimrProto::OK
                ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED);
            response.set_selected_tablets(record.GetSelectedTablets());
            if (record.GetStatus() == NKikimrProto::OK) {
                const TString id = TStringBuilder() << HiveId << ':' << record.GetHiveGeneration() << ':' << record.GetReassignId();
                response.set_id(id);
            }
            if (record.HasError()) {
                auto* issue = response.add_issues();
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(record.GetError());
            }
            Reply(response, response.status());
        }
    }

    void Handle(TEvHive::TEvStopReassignTabletsReply::TPtr& ev) {
        if constexpr (Stop) {
            if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
                Reply(Ydb::StatusIds::SUCCESS);
            } else {
                Reply(Ydb::StatusIds::NOT_FOUND, "Reassignment is no longer running");
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == HivePipeClient) {
            Reply(Submitted ? Ydb::StatusIds::UNDETERMINED : Ydb::StatusIds::UNAVAILABLE,
                Submitted ? "Hive disconnected; request may have been applied" : "Hive disconnected");
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        Reply(Submitted ? Ydb::StatusIds::UNDETERMINED : Ydb::StatusIds::TIMEOUT,
            Submitted ? "Hive acknowledgement timed out; request may have been applied" : "Database lookup or Hive connection timed out");
    }

    TActorId HivePipeClient;
    ui64 HiveId = 0;
    ui64 HiveGeneration = 0;
    ui64 ReassignId = 0;
    bool Submitted = false;
};

void DoReassignTabletsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcReassignTablets<false>(p.release()));
}

void DoStopReassignTabletsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcReassignTablets<true>(p.release()));
}

template<>
IActor* TEvReassignTabletsRequest::CreateRpcActor(IRequestNoOpCtx* msg) {
    return new TRpcReassignTablets<false>(msg);
}

template<>
IActor* TEvStopReassignTabletsRequest::CreateRpcActor(IRequestNoOpCtx* msg) {
    return new TRpcReassignTablets<true>(msg);
}

} // namespace NKikimr::NGRpcService
