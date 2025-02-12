#include "rpc_scheme_base.h"
#include "service_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvDescribeExternalTableRequest = TGrpcRequestOperationCall<
    Ydb::Table::DescribeExternalTableRequest,
    Ydb::Table::DescribeExternalTableResponse
>;

class TDescribeExternalTableRPC : public TRpcSchemeRequestActor<TDescribeExternalTableRPC, TEvDescribeExternalTableRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeExternalTableRPC, TEvDescribeExternalTableRequest>;

public:
    TDescribeExternalTableRPC(IRequestOpCtx* msg)
        : TBase(msg)
    {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        const auto* request = GetProtoRequest();
        const auto& path = request->path();
        const auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            Request_->RaiseIssue(NYql::TIssue("Invalid path"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        navigate->DatabaseName = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = std::move(paths);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate));
        Become(&TDescribeExternalTableRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const auto* navigate = ev->Get()->Request.Get();

        if (navigate->ResultSet.size() != 1) {
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }
        const auto& entry = navigate->ResultSet.front();

        if (navigate->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            default:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }
        }
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            // an invariant is broken: error count is equal to zero, but the status is not ok
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        // to do: fill the description from the received navigation entry
        Ydb::Table::DescribeExternalTableResult description;
        return ReplyWithResult(Ydb::StatusIds::SUCCESS, description, ctx);
    }
};

void DoDescribeExternalTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeExternalTableRPC(p.release()));
}

}
