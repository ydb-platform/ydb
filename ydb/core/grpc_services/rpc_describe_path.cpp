#include "service_scheme.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NSchemeCache;
using namespace Ydb;

using TEvListDirectoryRequest = TGrpcRequestOperationCall<Ydb::Scheme::ListDirectoryRequest,
    Ydb::Scheme::ListDirectoryResponse>;

using TEvDescribePathRequest = TGrpcRequestOperationCall<Ydb::Scheme::DescribePathRequest,
    Ydb::Scheme::DescribePathResponse>;

template <typename TDerived, typename TRequest, typename TResult, bool ListChildren = false>
class TBaseDescribe : public TRpcSchemeRequestActor<TDerived, TRequest> {
    using TBase = TRpcSchemeRequestActor<TDerived, TRequest>;

public:
    TBaseDescribe(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);
        ResolvePath(ctx);
    }

private:
    void ResolvePath(const TActorContext& ctx) {
        auto request = MakeHolder<TSchemeCacheNavigate>();
        request->DatabaseName = NKikimr::CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = TSchemeCacheNavigate::OpList; // we need ListNodeEntry
        entry.Path = NKikimr::SplitPath(this->GetProtoRequest()->path());

        ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        this->Become(&TDerived::StateResolvePath);
    }

    void StateResolvePath(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const auto& request = ev->Get()->Request;
        if (request->ResultSet.size() != 1) {
            return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        const auto& entry = request->ResultSet.front();
        if (entry.Status != TSchemeCacheNavigate::EStatus::Ok) {
            return SendProposeRequest(ctx, this->GetProtoRequest()->path());
        }

        if (entry.Kind != TSchemeCacheNavigate::EKind::KindCdcStream) {
            return SendProposeRequest(ctx, this->GetProtoRequest()->path());
        }

        if (!entry.Self || !entry.ListNodeEntry) {
            return SendProposeRequest(ctx, this->GetProtoRequest()->path());
        }

        if (entry.ListNodeEntry->Children.size() != 1) {
            // not created yet
            return this->Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }

        OverrideName = entry.Self->Info.GetName();
        const auto& topicName = entry.ListNodeEntry->Children.at(0).Name;

        return SendProposeRequest(ctx,
            NKikimr::JoinPath(NKikimr::ChildPath(NKikimr::SplitPath(this->GetProtoRequest()->path()), topicName)));
    }

    void SendProposeRequest(const TActorContext& ctx, const TString& path) {
        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *this->Request_);
        SetDatabase(navigateRequest.get(), *this->Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(path);
        record->MutableOptions()->SetShowPrivateTable(OverrideName.Defined());

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
        this->Become(&TDerived::StateWork);
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();

        if (record.HasReason()) {
            auto issue = NYql::TIssue(record.GetReason());
            this->Request_->RaiseIssue(issue);
        }

        TResult result;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                ConvertDirectoryEntry(pathDescription, result.mutable_self(), true);
                if (OverrideName) {
                    result.mutable_self()->set_name(*OverrideName);
                }
                if constexpr (ListChildren) {
                    for (const auto& child : pathDescription.GetChildren()) {
                        ConvertDirectoryEntry(child, result.add_children(), false);
                    }
                }
                return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
            }
            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError: {
                return this->Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            case NKikimrScheme::StatusAccessDenied: {
                return this->Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }
            case NKikimrScheme::StatusNotAvailable: {
                return this->Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }
            default: {
                return this->Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

private:
    TMaybe<TString> OverrideName;
};

class TListDirectoryRPC : public TBaseDescribe<TListDirectoryRPC, TEvListDirectoryRequest, Ydb::Scheme::ListDirectoryResult, true> {
public:
    using TBaseDescribe<TListDirectoryRPC, TEvListDirectoryRequest, Ydb::Scheme::ListDirectoryResult, true>::TBaseDescribe;
};

class TDescribePathRPC : public TBaseDescribe<TDescribePathRPC, TEvDescribePathRequest, Ydb::Scheme::DescribePathResult> {
public:
    using TBaseDescribe<TDescribePathRPC, TEvDescribePathRequest, Ydb::Scheme::DescribePathResult>::TBaseDescribe;
};

void DoListDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListDirectoryRPC(p.release()));
}

template<>
IActor* TEvListDirectoryRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TListDirectoryRPC(msg);
}

void DoDescribePathRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribePathRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
