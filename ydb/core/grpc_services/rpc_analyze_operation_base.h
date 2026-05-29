#pragma once

#include "rpc_request_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr {
namespace NGRpcService {

// Base actor for analyze-operation gRPC handlers.
// Resolves the Statistics Aggregator tablet (not SchemeShard) for the given database,
// following the same two-hop navigation logic as TAnalyzeActor.
template <typename TDerived, typename TEvRequest, bool HasOperation = false>
class TRpcAnalyzeOperationRequestActor
    : public TRpcRequestActor<TDerived, TEvRequest, HasOperation>
{
protected:
    virtual TStringBuf GetLogPrefix() const = 0;
    virtual IEventBase* MakeRequest() = 0;

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

    // Kick off SA resolution: navigate the database path.
    void ResolveStatisticsAggregator() {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = this->GetDatabaseName();

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = TNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(this->GetDatabaseName());
        entry.RedirectRequired = false;

        this->Send(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()),
            0, FirstRoundCookie);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        if (request->ResultSet.empty() || request->ErrorCount > 0) {
            return this->Reply(Ydb::StatusIds::SCHEME_ERROR,
                NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        const auto& entry = request->ResultSet.front();
        if (!entry.DomainInfo) {
            return this->Reply(Ydb::StatusIds::INTERNAL_ERROR,
                NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        if (!this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, NACLib::GenericRead)) {
            return;
        }

        if (ev->Cookie == SecondRoundCookie) {
            // Second hop: serverless resources domain
            if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
                SendRequest(entry.DomainInfo->Params.GetStatisticsAggregator());
            } else {
                this->Reply(Ydb::StatusIds::INTERNAL_ERROR,
                    NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
            }
            return;
        }

        // First hop
        const auto& domainInfo = entry.DomainInfo;
        if (!domainInfo->IsServerless()) {
            if (domainInfo->Params.HasStatisticsAggregator()) {
                SendRequest(domainInfo->Params.GetStatisticsAggregator());
            } else {
                // Navigate domain key for non-serverless without explicit SA ID
                NavigateDomainKey(domainInfo->DomainKey);
            }
        } else {
            NavigateDomainKey(domainInfo->ResourcesDomainKey);
        }
    }

    void SendRequest(ui64 saTabletId) {
        if (!PipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            PipeClient = this->RegisterWithSameMailbox(
                NTabletPipe::CreateClient(this->SelfId(), saTabletId, config));
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
        this->Reply(Ydb::StatusIds::UNAVAILABLE);
    }

    void PassAway() override {
        NTabletPipe::CloseAndForgetClient(this->SelfId(), PipeClient);
        IActor::PassAway();
    }

public:
    using TRpcRequestActor<TDerived, TEvRequest, HasOperation>::TRpcRequestActor;

private:
    void NavigateDomainKey(const TPathId& domainKey) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = MakeHolder<TNavigate>();
        navigate->DatabaseName = this->GetDatabaseName();

        auto& entry = navigate->ResultSet.emplace_back();
        entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;

        this->Send(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()),
            0, SecondRoundCookie);
    }

    static constexpr ui64 FirstRoundCookie  = 0;
    static constexpr ui64 SecondRoundCookie = 1;

    TActorId PipeClient;
};

} // namespace NGRpcService
} // namespace NKikimr
