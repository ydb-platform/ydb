#include "service_cms.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/protos/ydb_cms.pb.h>

namespace NKikimr::NGRpcService {

using TEvGetScaleRecommendationRequest = TGrpcRequestNoOperationCall<Ydb::Cms::GetScaleRecommendationRequest, Ydb::Cms::GetScaleRecommendationResponse>;

class TGetScaleRecommendationRPC : public TRpcRequestActor<TGetScaleRecommendationRPC, TEvGetScaleRecommendationRequest, false> {
public:
    using TRpcRequestActor::TRpcRequestActor;

    void Bootstrap(const TActorContext&) {
        ResolveDatabase(GetProtoRequest()->path());
        this->Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvHive::TEvResponseScaleRecommendation, Handle);

            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

    void ResolveDatabase(const TString& databaseName) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = databaseName;

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(databaseName);

        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        if (request->ResultSet.empty()) {
            return this->Reply(Ydb::StatusIds::SCHEME_ERROR, NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        const auto& entry = request->ResultSet.front();

        if (request->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                return this->Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                return this->Reply(Ydb::StatusIds::SCHEME_ERROR, NKikimrIssues::TIssuesIds::PATH_NOT_EXIST);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                return this->Reply(Ydb::StatusIds::UNAVAILABLE, NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR);
            default:
                return this->Reply(Ydb::StatusIds::SCHEME_ERROR, NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
            }
        }

        if (!this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, NACLib::GenericList)) {
            return;
        }

        auto domainInfo = entry.DomainInfo;
        if (!domainInfo || !domainInfo->Params.HasHive()) {
            return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        SendRequest(domainInfo->DomainKey, domainInfo->Params.GetHive());
    }

    void SendRequest(TPathId domainKey, ui64 hiveId) {
        if (!PipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            PipeClient = this->RegisterWithSameMailbox(NTabletPipe::CreateClient(this->SelfId(), hiveId, config));
        }

        auto ev = std::make_unique<TEvHive::TEvRequestScaleRecommendation>();
        ev->Record.MutableDomainKey()->SetSchemeShard(domainKey.OwnerId);
        ev->Record.MutableDomainKey()->SetPathId(domainKey.LocalPathId);
        NTabletPipe::SendData(this->SelfId(), PipeClient, ev.release());
    }

    void Handle(TEvHive::TEvResponseScaleRecommendation::TPtr& ev) {
        TResponse response;
        
        switch (ev->Get()->Record.GetStatus()) {
        case NKikimrProto::OK: {
            ui32 recommendedNodes = ev->Get()->Record.GetRecommendedNodes();
            response.mutable_recommended_resources()->add_computational_units()->set_count(recommendedNodes);
            response.set_status(Ydb::StatusIds::SUCCESS);
            break;
        }
        case NKikimrProto::NOTREADY:
            response.set_status(Ydb::StatusIds::UNAVAILABLE);
            break;
        default:
            response.set_status(Ydb::StatusIds::INTERNAL_ERROR);
            break;
        }
        
        return this->Reply(response);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            DeliveryProblem();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        DeliveryProblem();
    }

    void DeliveryProblem() {
        this->Reply(Ydb::StatusIds::UNAVAILABLE);
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(this->SelfId(), PipeClient);
        IActor::PassAway();
    }

private:
    TActorId PipeClient;

};

void DoGetScaleRecommendationRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TGetScaleRecommendationRPC(p.release()));
}

} // namespace NKikimr::NGRpcService
