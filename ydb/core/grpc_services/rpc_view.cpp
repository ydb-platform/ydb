#include "rpc_scheme_base.h"
#include "service_view.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>

namespace NKikimr::NGRpcService {

using namespace Ydb;

using TEvDescribeView = TGrpcRequestOperationCall<View::DescribeViewRequest, View::DescribeViewResponse>;

class TDescribeViewRPC : public TRpcSchemeRequestActor<TDescribeViewRPC, TEvDescribeView> {
    using TBase = TRpcSchemeRequestActor<TDescribeViewRPC, TEvDescribeView>;

public:
    using TBase::TBase;

    void Bootstrap() {
        DescribeScheme();
    }

    void PassAway() override {
        TBase::PassAway();
    }

private:
    void DescribeScheme() {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev.get(), *Request_);
        ev->Record.MutableDescribePath()->SetPath(GetProtoRequest()->path());

        Send(MakeTxProxyID(), ev.release());
        Become(&TDescribeViewRPC::StateDescribeScheme);
    }

    STATEFN(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            return TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto& desc = record.GetPathDescription();

        if (record.HasReason()) {
            Request_->RaiseIssue(NYql::TIssue(record.GetReason()));
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess:
                if (desc.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeView) {
                    auto message = TStringBuilder() << "Expected a view, but got: " << desc.GetSelf().GetPathType();
                    Request_->RaiseIssue(NYql::TIssue(message));
                    return Reply(StatusIds::SCHEME_ERROR, ctx);
                }

                ConvertDirectoryEntry(desc.GetSelf(), Result_.mutable_self(), true);
                Result_.set_query_text(desc.GetViewDescription().GetQueryText());

                return ReplyWithResult(StatusIds::SUCCESS, Result_, ctx);

            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError:
                return Reply(StatusIds::SCHEME_ERROR, ctx);

            case NKikimrScheme::StatusAccessDenied:
                return Reply(StatusIds::UNAUTHORIZED, ctx);

            case NKikimrScheme::StatusNotAvailable:
                return Reply(StatusIds::UNAVAILABLE, ctx);

            default:
                return Reply(StatusIds::GENERIC_ERROR, ctx);
        }
    }

private:
    View::DescribeViewResult Result_;
};

void DoDescribeView(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeViewRPC(p.release()));
}

}
