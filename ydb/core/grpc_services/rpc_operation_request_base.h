#pragma once

#include "rpc_request_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr {
namespace NGRpcService {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, GetLogPrefix() << " " << this->SelfId() << " [" << this->TxId << "] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, GetLogPrefix() << " " << this->SelfId() << " [" << this->TxId << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_PROXY, GetLogPrefix() << " " << this->SelfId() << " [" << this->TxId << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, GetLogPrefix() << " " << this->SelfId() << " [" << this->TxId << "] " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_PROXY, GetLogPrefix() << " " << this->SelfId() << " [" << this->TxId << "] " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, GetLogPrefix() << " " << this->SelfId() << " [" << this->TxId << "] " << stream)

template <typename TDerived, typename TEvRequest, bool HasOperation = false>
class TRpcOperationRequestActor: public TRpcRequestActor<TDerived, TEvRequest, HasOperation> {
protected:
    virtual TStringBuf GetLogPrefix() const = 0;
    virtual IEventBase* MakeRequest() = 0;

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);

            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

    void AllocateTxId() {
        LOG_D("Allocate txId");
        this->Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;

        LOG_D("TEvTxUserProxy::TEvAllocateTxIdResult");
        ResolveDatabase();
    }

    void ResolveDatabase() {
        LOG_D("Resolve database"
            << ": name# " << this->GetDatabaseName());

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = this->GetDatabaseName();

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(this->GetDatabaseName());

        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": request# " << (request ? request->ToString(*AppData()->TypeRegistry) : "nullptr"));

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

        if (!this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, NACLib::GenericRead | NACLib::GenericWrite)) {
            return;
        }

        auto domainInfo = entry.DomainInfo;
        if (!domainInfo) {
            LOG_E("Got empty domain info");
            return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        SendRequest(domainInfo->ExtractSchemeShard());
    }

    void SendRequest(ui64 schemeShardId) {
        LOG_D("Send request"
            << ": schemeShardId# " << schemeShardId);

        if (!PipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            PipeClient = this->RegisterWithSameMailbox(NTabletPipe::CreateClient(this->SelfId(), schemeShardId, config));
        }

        NTabletPipe::SendData(this->SelfId(), PipeClient, MakeRequest());
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
        LOG_W("Delivery problem");
        this->Reply(Ydb::StatusIds::UNAVAILABLE);
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(this->SelfId(), PipeClient);
        IActor::PassAway();
    }

public:
    using TRpcRequestActor<TDerived, TEvRequest, HasOperation>::TRpcRequestActor;

protected:
    ui64 TxId = 0;

private:
    TActorId PipeClient;

}; // TRpcOperationRequestActor

} // namespace NGRpcService
} // namespace NKikimr
