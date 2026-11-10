#include "rpc_reassign_tablets.h"
#include "service_tablet.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/vector.h>

namespace NKikimr::NGRpcService {

class TRpcReassignTablets : public TActorBootstrapped<TRpcReassignTablets> {
public:
    enum class EAction { Start, Stop };

    TRpcReassignTablets(IRequestNoOpCtx* request, EAction action)
        : Request(request)
        , Action(action)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    void Bootstrap() {
        const auto& token = Request->GetInternalToken();
        if (!IsAdministrator(AppData(), token.Get())) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, "Cluster administrator access is required");
        }
        const TString user = token ? token->GetUserSID() : TString();
        if (Action == EAction::Stop) {
            const auto& request = *TEvStopReassignTabletsRequest::GetProtoRequest(Request);
            ui64 generation = 0, id = 0;
            const auto parts = SplitString(request.id(), ":");
            if (parts.size() != 3 || !TryFromString(parts[0], HiveId)
                    || !TryFromString(parts[1], generation) || !TryFromString(parts[2], id)
                    || !HiveId || !generation || !id) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid reassignment id");
            }
            Request->AddAuditLogPart("reassign_id", request.id());
            auto event = std::make_unique<TEvHive::TEvStopReassignTablets>();
            event->Record.SetReassignId(id);
            event->Record.SetHiveGeneration(generation);
            event->Record.SetUserSID(user);
            HiveRequest = std::move(event);
        } else {
            const auto& request = *TEvReassignTabletsRequest::GetProtoRequest(Request);
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
            auto event = std::make_unique<TEvHive::TEvReassignTablets>();
            event->Record.SetStoragePool(request.storage_pool());
            event->Record.SetGroupId(request.group());
            event->Record.SetMaxInFlight(request.has_max_in_flight() ? request.max_in_flight() : 1);
            event->Record.SetPercent(request.has_percent() ? request.percent() : 100);
            event->Record.SetAsync(request.async());
            event->Record.SetUserSID(user);
            HiveRequest = std::move(event);
        }

        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup);
        if (Action == EAction::Stop) {
            ConnectHive();
        } else {
            auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
            auto& entry = navigate->ResultSet.emplace_back();
            entry.Path = NKikimr::SplitPath(TEvReassignTabletsRequest::GetProtoRequest(Request)->database());
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RedirectRequired = false;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
        }
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeClient);
        IActor::PassAway();
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

    void Reply(Ydb::StatusIds::StatusCode status, const TString& error = {}) {
        if (error) {
            Request->RaiseIssue(NYql::TIssue(error));
        }
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }

    void ConnectHive() {
        Request->AddAuditLogPart("hive_id", ToString(HiveId));
        NTabletPipe::TClientConfig config;
        config.RetryPolicy.RetryLimitCount = 3;
        HivePipeClient = Register(NTabletPipe::CreateClient(SelfId(), HiveId, config));
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
        if (ev->Get()->ClientId != HivePipeClient) {
            return;
        }
        if (ev->Get()->Status != NKikimrProto::OK) {
            return Reply(Ydb::StatusIds::UNAVAILABLE, "Cannot connect to Hive");
        }
        if (!Submitted) {
            Submitted = true;
            NTabletPipe::SendData(SelfId(), HivePipeClient, HiveRequest.release());
        }
    }

    void Handle(TEvHive::TEvReassignTabletsReply::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrProto::OK) {
            return Reply(Ydb::StatusIds::PRECONDITION_FAILED, record.GetError());
        }
        Ydb::Tablet::ReassignTabletsResponse response;
        response.set_status(Ydb::StatusIds::SUCCESS);
        response.set_selected_tablets(record.GetSelectedTablets());
        const TString id = TStringBuilder() << HiveId << ':' << record.GetHiveGeneration() << ':' << record.GetReassignId();
        response.set_id(id);
        TProtoResponseHelper::SendProtoResponse(response, response.status(), Request);
        PassAway();
    }

    void Handle(TEvHive::TEvStopReassignTabletsReply::TPtr& ev) {
        if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
            Reply(Ydb::StatusIds::SUCCESS);
        } else {
            Reply(Ydb::StatusIds::NOT_FOUND, "Reassignment is no longer running");
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

    const std::unique_ptr<IRequestNoOpCtx> Request;
    const EAction Action;
    std::unique_ptr<IEventBase> HiveRequest;
    TActorId HivePipeClient;
    ui64 HiveId = 0;
    bool Submitted = false;
};

void DoReassignTabletsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcReassignTablets(p.release(), TRpcReassignTablets::EAction::Start));
}

void DoStopReassignTabletsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcReassignTablets(p.release(), TRpcReassignTablets::EAction::Stop));
}

template<>
IActor* TEvReassignTabletsRequest::CreateRpcActor(IRequestNoOpCtx* msg) {
    return new TRpcReassignTablets(msg, TRpcReassignTablets::EAction::Start);
}

template<>
IActor* TEvStopReassignTabletsRequest::CreateRpcActor(IRequestNoOpCtx* msg) {
    return new TRpcReassignTablets(msg, TRpcReassignTablets::EAction::Stop);
}

} // namespace NKikimr::NGRpcService
