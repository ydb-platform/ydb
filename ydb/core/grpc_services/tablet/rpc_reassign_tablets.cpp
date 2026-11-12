#include "rpc_reassign_tablets.h"
#include "service_tablet.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/reassign_tablets.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

namespace NKikimr::NGRpcService {

class TRpcReassignTablets : public TRpcRequestActor<TRpcReassignTablets, TEvReassignTabletsRequest, true> {
    using TBase = TRpcRequestActor<TRpcReassignTablets, TEvReassignTabletsRequest, true>;

public:
    TRpcReassignTablets(IRequestOpCtx* request)
        : TBase(request)
    {}

    void Bootstrap() {
        const auto& token = Request->GetInternalToken();
        if (!IsAdministrator(AppData(), token.Get())) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, "Cluster administrator access is required");
        }
        const TString user = token ? token->GetUserSID() : TString();
        const auto& request = *GetProtoRequest();
        const auto& database = GetDatabaseName();
        if (database.empty()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "x-ydb-database must not be empty");
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
        Request->AddAuditLogPart("storage_pool", request.storage_pool());
        Request->AddAuditLogPart("group", ToString(request.group()));
        auto event = std::make_unique<NHive::TEvReassignTablets::TEvCreateRequest>();
        event->Record.SetStoragePool(request.storage_pool());
        event->Record.SetGroupId(request.group());
        event->Record.SetMaxInFlight(request.has_max_in_flight() ? request.max_in_flight() : 1);
        event->Record.SetPercent(request.has_percent() ? request.percent() : 100);
        event->Record.SetAsync(request.async());
        event->Record.SetUserSID(user);
        HiveRequest = std::move(event);

        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup);
        auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = NKikimr::SplitPath(database);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeClient);
        TBase::PassAway();
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NHive::TEvReassignTablets::TEvCreateResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
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

    void Handle(NHive::TEvReassignTablets::TEvCreateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Reply(record.GetStatus(), record.GetIssues());
        }
        NOperationId::TOperationId id;
        id.SetKind(NOperationId::TOperationId::REASSIGN_TABLETS);
        id.AddOptionalValue("hive_id", ToString(HiveId));
        id.AddOptionalValue("hive_generation", ToString(record.GetHiveGeneration()));
        id.AddOptionalValue("reassign_id", ToString(record.GetReassignId()));
        Ydb::Tablet::ReassignTabletsResponse response;
        auto& operation = *response.mutable_operation();
        operation.set_id(id.ToString());
        operation.set_ready(false);
        Ydb::Tablet::ReassignTabletsMetadata metadata;
        metadata.set_selected_tablets(record.GetSelectedTablets());
        operation.mutable_metadata()->PackFrom(metadata);
        Reply(response, Ydb::StatusIds::SUCCESS);
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

    std::unique_ptr<IEventBase> HiveRequest;
    TActorId HivePipeClient;
    ui64 HiveId = 0;
    bool Submitted = false;
};

void DoReassignTabletsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcReassignTablets(p.release()));
}

template<>
IActor* TEvReassignTabletsRequest::CreateRpcActor(IRequestOpCtx* msg) {
    return new TRpcReassignTablets(msg);
}

} // namespace NKikimr::NGRpcService
