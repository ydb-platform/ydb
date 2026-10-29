#include "rpc_scheme_base.h"
#include "service_replication.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

namespace NKikimr::NGRpcService {

using TEvAcquireReplicationBoundary = TGrpcRequestOperationCall<
    Ydb::Replication::AcquireReplicationBoundaryRequest,
    Ydb::Replication::AcquireReplicationBoundaryResponse>;

class TAcquireReplicationBoundaryRPC
    : public TRpcSchemeRequestActor<TAcquireReplicationBoundaryRPC, TEvAcquireReplicationBoundary>
{
    using TBase = TRpcSchemeRequestActor<TAcquireReplicationBoundaryRPC, TEvAcquireReplicationBoundary>;
    using TLongTxEvents = NLongTxService::TEvLongTxService;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        const auto* request = GetProtoRequest();
        Database = Request_->GetDatabaseName().GetOrElse("");
        if (Database.empty() || request->source_paths().empty() || request->commit_interval_ms() == 0) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Database, source_paths and a positive commit_interval_ms are required");
        }
        // Describe the database first, to reject tables belonging to a different
        // subdomain even if their textual paths share the database prefix.
        DescribePath(Database);
    }

private:
    void Fail(Ydb::StatusIds::StatusCode status, const TString& message) {
        Request_->RaiseIssue(NYql::TIssue(message));
        Reply(status, TActivationContext::AsActorContext());
    }

    void DescribePath(const TString& path) {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev.get(), *Request_);
        ev->Record.MutableDescribePath()->SetPath(path);
        Send(MakeTxProxyID(), ev.release());
        Become(&TThis::StateDescribe);
    }

    STATEFN(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            auto status = Ydb::StatusIds::SCHEME_ERROR;
            if (record.GetStatus() == NKikimrScheme::StatusAccessDenied) {
                status = Ydb::StatusIds::UNAUTHORIZED;
            } else if (record.GetStatus() == NKikimrScheme::StatusNotAvailable) {
                status = Ydb::StatusIds::UNAVAILABLE;
            }
            return Fail(status, record.GetReason());
        }
        const auto& desc = record.GetPathDescription();
        if (!desc.HasDomainDescription() || !desc.GetDomainDescription().HasDomainKey()) {
            return Fail(Ydb::StatusIds::UNAVAILABLE, "Source database identity is unavailable");
        }
        const auto& domain = desc.GetDomainDescription().GetDomainKey();
        if (!Domain) {
            Domain = std::make_pair(domain.GetSchemeShard(), domain.GetPathId());
        } else {
            if (*Domain != std::make_pair(domain.GetSchemeShard(), domain.GetPathId())) {
                return Fail(Ydb::StatusIds::BAD_REQUEST, "All source tables must belong to the requested database");
            }
            if (desc.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeTable) {
                return Fail(Ydb::StatusIds::BAD_REQUEST, "Only row tables are supported");
            }
            const auto& table = desc.GetTable();
            if (desc.GetSelf().GetPathState() != NKikimrSchemeOp::EPathStateNoChanges
                || table.GetPartitionConfig().GetFreezeState() != NKikimrSchemeOp::Freeze)
            {
                return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Freeze must be completed for every source table");
            }
            for (const auto& stream : table.GetCdcStreams()) {
                if (stream.GetState() != NKikimrSchemeOp::ECdcStreamStateReady) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Source changefeeds must be Ready before acquiring a boundary");
                }
            }
            for (const auto& index : table.GetTableIndexes()) {
                if (index.GetState() != NKikimrSchemeOp::EIndexStateReady) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Source index operations must be completed");
                }
            }
        }
        if (NextPath < GetProtoRequest()->source_paths_size()) {
            return DescribePath(Request_->NormalizePath(GetProtoRequest()->source_paths(NextPath++)));
        }
        // A fresh request is issued only after all completed Freezes were observed.
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), new TLongTxEvents::TEvAcquireReadSnapshot(Database));
        Become(&TThis::StateSnapshot);
    }

    STATEFN(StateSnapshot) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TLongTxEvents::TEvAcquireReadSnapshotResult, Handle);
        default:
            TBase::StateWork(ev);
        }
    }

    void Handle(TLongTxEvents::TEvAcquireReadSnapshotResult::TPtr& ev) {
        const auto& msg = *ev->Get();
        if (msg.Status != Ydb::StatusIds::SUCCESS) {
            return Reply(msg.Status, msg.Issues, TActivationContext::AsActorContext());
        }
        const ui64 interval = GetProtoRequest()->commit_interval_ms();
        const ui64 intervalNo = msg.Snapshot.Step / interval;
        if (intervalNo >= Max<ui64>() / interval) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Replication boundary overflows uint64");
        }
        Ydb::Replication::AcquireReplicationBoundaryResult result;
        result.mutable_boundary()->set_step((intervalNo + 1) * interval);
        result.mutable_boundary()->set_tx_id(0);
        result.mutable_snapshot()->set_step(msg.Snapshot.Step);
        result.mutable_snapshot()->set_tx_id(msg.Snapshot.TxId);
        result.set_commit_interval_ms(interval);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, TActivationContext::AsActorContext());
    }

    TString Database;
    std::optional<std::pair<ui64, ui64>> Domain;
    int NextPath = 0;
};

void DoAcquireReplicationBoundary(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TAcquireReplicationBoundaryRPC(request.release()));
}

template<>
IActor* TEvAcquireReplicationBoundary::CreateRpcActor(IRequestOpCtx* request) {
    return new TAcquireReplicationBoundaryRPC(request);
}

} // namespace NKikimr::NGRpcService
