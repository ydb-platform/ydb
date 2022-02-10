#include "service_scheme.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_scheme_base.h" 
#include "rpc_common.h"
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
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
 
        SendProposeRequest(ctx);
        this->Become(&TDerived::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *this->Request_);
        SetDatabase(navigateRequest.get(), *this->Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(req->path());

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev, ctx); 
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
};

class TListDirectoryRPC : public TBaseDescribe<TListDirectoryRPC, TEvListDirectoryRequest, Ydb::Scheme::ListDirectoryResult, true> {
public:
    using TBaseDescribe<TListDirectoryRPC, TEvListDirectoryRequest, Ydb::Scheme::ListDirectoryResult, true>::TBaseDescribe;
};

class TDescribePathRPC : public TBaseDescribe<TDescribePathRPC, TEvDescribePathRequest, Ydb::Scheme::DescribePathResult> {
public:
    using TBaseDescribe<TDescribePathRPC, TEvDescribePathRequest, Ydb::Scheme::DescribePathResult>::TBaseDescribe;
};

void DoListDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListDirectoryRPC(p.release()));
}

void DoDescribePathRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDescribePathRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
