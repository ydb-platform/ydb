#pragma once

#include "defs.h"
#include "rpc_deferrable.h"

#include <ydb/public/api/protos/draft/ydb_test_shard.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NGRpcService {

template <typename TDerived, typename TRequest, typename TResultRecord>
class TTestShardRequestBase : public TRpcOperationRequestActor<TDerived, TRequest> {
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TTestShardRequestBase(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();
        TBase::Bootstrap(ctx);

        auto* self = Self();
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;

        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }
        self->Become(&TDerived::StateFunc);
        self->Proceed();
    }

protected:
    bool IsRootDomain(const TString& databaseName) const {
        const auto* appDomainInfo = AppData()->DomainsInfo.Get();
        if (!appDomainInfo || !appDomainInfo->Domain) {
            return false;
        }

        TString rootDomainName = "/" + appDomainInfo->Domain->Name;
        return databaseName == rootDomainName || databaseName == appDomainInfo->Domain->Name;
    }

    void ResolveDatabase(const TString& databaseName) {
        if (IsRootDomain(databaseName)) {
            const auto* appDomainInfo = AppData()->DomainsInfo.Get();
            if (!appDomainInfo || !appDomainInfo->HiveTabletId) {
                NYql::TIssues issues;
                issues.AddIssue("Root domain does not have Hive configured");
                Self()->Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, Self()->ActorContext());
                return;
            }

            HiveId = *appDomainInfo->HiveTabletId;
            Self()->OnDatabaseResolved(true);
            return;
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = databaseName;

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(databaseName);

        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void HandleNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        if (request->ResultSet.empty()) {
            NYql::TIssues issues;
            issues.AddIssue("Failed to resolve database path");
            Self()->Reply(Ydb::StatusIds::SCHEME_ERROR, issues, Self()->ActorContext());
            return;
        }

        const auto& entry = request->ResultSet.front();

        if (request->ErrorCount > 0) {
            NYql::TIssues issues;
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                issues.AddIssue("Access denied to database");
                Self()->Reply(Ydb::StatusIds::UNAUTHORIZED, issues, Self()->ActorContext());
                return;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                issues.AddIssue("Database path does not exist");
                Self()->Reply(Ydb::StatusIds::SCHEME_ERROR, issues, Self()->ActorContext());
                return;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                issues.AddIssue("Failed to lookup database path");
                Self()->Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
                return;
            default:
                issues.AddIssue("Failed to resolve database path");
                Self()->Reply(Ydb::StatusIds::SCHEME_ERROR, issues, Self()->ActorContext());
                return;
            }
        }

        auto domainInfo = entry.DomainInfo;
        if (!domainInfo || !domainInfo->Params.HasHive()) {
            NYql::TIssues issues;
            issues.AddIssue("Database does not have Hive configured");
            Self()->Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, Self()->ActorContext());
            return;
        }

        HiveId = domainInfo->Params.GetHive();
        Self()->OnDatabaseResolved(false, domainInfo.Get(), entry.DomainDescription.Get());
    }

    void SetupHivePipe(ui64 hiveId) {
        const ui64 hiveTabletId = hiveId > 0 ? hiveId : AppData()->DomainsInfo->GetHive();
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};
        HivePipeClient = this->RegisterWithSameMailbox(
            NTabletPipe::CreateClient(this->SelfId(), hiveTabletId, pipeConfig));
    }

    void PassAway() override {
        if (HivePipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), HivePipeClient);
            HivePipeClient = {};
        }
        TBase::PassAway();
    }

    const TDerived* Self() const { return static_cast<const TDerived*>(this); }
    TDerived* Self() { return static_cast<TDerived*>(this); }

protected:
    ui64 HiveId = 0;
    TActorId HivePipeClient;
};

} // namespace NKikimr::NGRpcService
