#include "service_coordination.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvDescribeCoordinationNode = TGrpcRequestOperationCall<Ydb::Coordination::DescribeNodeRequest,
    Ydb::Coordination::DescribeNodeResponse>;

class TDescribeCoordinationNode : public TRpcSchemeRequestActor<TDescribeCoordinationNode, TEvDescribeCoordinationNode> {
    using TBase = TRpcSchemeRequestActor<TDescribeCoordinationNode, TEvDescribeCoordinationNode>;

public:
    TDescribeCoordinationNode(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TDescribeCoordinationNode::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                const auto pathType = pathDescription.GetSelf().GetPathType();

                if (pathType != NKikimrSchemeOp::EPathTypeKesus) {
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                Ydb::Coordination::DescribeNodeResult result;

                Ydb::Scheme::Entry* selfEntry = result.mutable_self();
                selfEntry->set_name(pathDescription.GetSelf().GetName());
                selfEntry->set_type(static_cast<Ydb::Scheme::Entry::Type>(pathDescription.GetSelf().GetPathType()));
                ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);

                if (pathDescription.HasKesus()) {
                    const auto& kesusDescription = pathDescription.GetKesus();
                    if (kesusDescription.HasConfig()) {
                        result.mutable_config()->CopyFrom(kesusDescription.GetConfig());
                    }
                }

                return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
            }
            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError: {
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            case NKikimrScheme::StatusAccessDenied: {
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }
            case NKikimrScheme::StatusNotAvailable: {
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }
            default: {
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(req->path());

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }
};

void DoDescribeCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeCoordinationNode(p.release()));
}

}
}
