#include "service_coordination.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_kh_snapshots.h"
#include "resolve_local_db_table.h"

#include "rpc_common/rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/actorlib_impl/long_timer.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/string/vector.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NGRpcService {

using TEvKikhouseCreateSnapshotRequest = TGrpcRequestOperationCall<Ydb::ClickhouseInternal::CreateSnapshotRequest,
    Ydb::ClickhouseInternal::CreateSnapshotResponse>;
using TEvKikhouseRefreshSnapshotRequest = TGrpcRequestOperationCall<Ydb::ClickhouseInternal::RefreshSnapshotRequest,
    Ydb::ClickhouseInternal::RefreshSnapshotResponse>;
using TEvKikhouseDiscardSnapshotRequest = TGrpcRequestOperationCall<Ydb::ClickhouseInternal::DiscardSnapshotRequest,
    Ydb::ClickhouseInternal::DiscardSnapshotResponse>;

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf SnapshotUriPrefix = "snapshot:///";
static constexpr ui64 SNAPSHOT_TIMEOUT_MS = 30000;
static constexpr ui64 REQUEST_TIMEOUT_MS = 5000;

////////////////////////////////////////////////////////////////////////////////

TString TKikhouseSnapshotId::ToUri() const {
    return TStringBuilder() << "snapshot:///" << Step << "/" << TxId;
}

bool TKikhouseSnapshotId::Parse(const TString& uri) {
    if (!uri.StartsWith(SnapshotUriPrefix)) {
        return false;
    }

    TStringBuf l, r;
    TStringBuf tail(uri.data() + SnapshotUriPrefix.size(), uri.size() - SnapshotUriPrefix.size());
    if (!tail.TrySplit('/', l, r)) {
        return false;
    }

    return TryFromString(l, Step) && TryFromString(r, TxId);
}

////////////////////////////////////////////////////////////////////////////////

namespace {
    static void MergeTimeout(TDuration& dst, const TDuration& src) {
        if (src) {
            if (dst) {
                dst = Min(dst, src);
            } else {
                dst = src;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TKikhouseCreateSnapshotRPC
    : public TRpcOperationRequestActor<TKikhouseCreateSnapshotRPC, TEvKikhouseCreateSnapshotRequest>
{
    using TBase = TRpcOperationRequestActor<TKikhouseCreateSnapshotRPC, TEvKikhouseCreateSnapshotRequest>;

private:
    TDuration SnapshotTimeout = TDuration::MilliSeconds(SNAPSHOT_TIMEOUT_MS);

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        const auto* proto = GetProtoRequest();

        if (proto->path_size() <= 0) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "At least one table path is required"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        TDuration reqTimeout = TDuration::Zero();

        // Let tx proxy try to kill request after cancel timeout
        MergeTimeout(reqTimeout, GetCancelAfter());

        // It's pointless to wait longer than operation timeout
        MergeTimeout(reqTimeout, GetOperationTimeout());

        // Use default timeout in the common case
        if (!reqTimeout) {
            reqTimeout = TDuration::MilliSeconds(REQUEST_TIMEOUT_MS);
        }

        auto req = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        req->Record.SetExecTimeoutPeriod(reqTimeout.MilliSeconds());

        auto token = Request_->GetSerializedToken();
        if (!token.empty()) {
            req->Record.SetUserToken(token);
        }

        SetDatabase(req.Get(), Request());

        auto* tx = req->Record.MutableTransaction()->MutableCreateVolatileSnapshot();
        for (const TString& path : proto->path()) {
            if (proto->ignore_system_views() && TryParseLocalDbPath(::NKikimr::SplitPath(path))) {
                continue;
            }
            tx->AddTables()->SetTablePath(path);
        }
        tx->SetTimeoutMs(SnapshotTimeout.MilliSeconds());
        if (proto->ignore_system_views()) {
            tx->SetIgnoreSystemViews(true);
        }

        ctx.Send(MakeTxProxyID(), req.Release());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            default:
                return StateFuncBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        using EStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;

        const auto* msg = ev->Get();
        const auto status = static_cast<EStatus>(msg->Record.GetStatus());
        auto issues = msg->Record.GetIssues();
        switch (status) {
            case EStatus::ExecComplete:
                if (msg->Record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS) {
                    TKikhouseSnapshotId id(msg->Record.GetStep(), msg->Record.GetTxId());
                    return ReplySuccess(id, ctx);
                }
                break;

            case EStatus::AccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, issues, ctx);

            case EStatus::ResolveError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, issues, ctx);

            case EStatus::Unknown:
            case EStatus::ProxyShardUnknown:
            case EStatus::CoordinatorUnknown:
                return Reply(Ydb::StatusIds::UNDETERMINED, issues, ctx);

            case EStatus::ExecTimeout:
                return Reply(Ydb::StatusIds::TIMEOUT, issues, ctx);

            case EStatus::CoordinatorDeclined:
            case EStatus::ProxyShardNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);

            case EStatus::ProxyShardOverloaded:
                return Reply(Ydb::StatusIds::OVERLOADED, issues, ctx);

            default:
                break;
        }

        // Unexpected error
        NYql::IssueToMessage(
            MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Got unexpected status " << status << " from tx proxy"),
            issues.Add());
        return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
    }

    void ReplySuccess(const TKikhouseSnapshotId& id, const TActorContext& ctx) {
        Ydb::ClickhouseInternal::CreateSnapshotResult result;
        result.set_snapshot_id(id.ToUri());
        result.set_timeout_ms(SnapshotTimeout.MilliSeconds());
        Request().SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKikhouseRefreshSnapshotRPC
    : public TRpcOperationRequestActor<TKikhouseRefreshSnapshotRPC, TEvKikhouseRefreshSnapshotRequest>
{
    using TBase = TRpcOperationRequestActor<TKikhouseRefreshSnapshotRPC, TEvKikhouseRefreshSnapshotRequest>;

private:
    TKikhouseSnapshotId SnapshotId;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        const auto* proto = GetProtoRequest();

        if (!SnapshotId.Parse(proto->snapshot_id())) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Valid snapshot id is required"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        if (proto->path_size() <= 0) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "At least one table path is required"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        TDuration reqTimeout = TDuration::Zero();

        // Let tx proxy try to kill request after cancel timeout
        MergeTimeout(reqTimeout, GetCancelAfter());

        // It's pointless to wait longer than operation timeout
        MergeTimeout(reqTimeout, GetOperationTimeout());

        // Use default timeout in the common case
        if (!reqTimeout) {
            reqTimeout = TDuration::MilliSeconds(REQUEST_TIMEOUT_MS);
        }

        auto req = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        req->Record.SetExecTimeoutPeriod(reqTimeout.MilliSeconds());

        auto token = Request_->GetSerializedToken();
        if (!token.empty()) {
            req->Record.SetUserToken(token);
        }

        SetDatabase(req.Get(), Request());

        auto* tx = req->Record.MutableTransaction()->MutableRefreshVolatileSnapshot();
        for (const TString& path : proto->path()) {
            if (proto->ignore_system_views() && TryParseLocalDbPath(::NKikimr::SplitPath(path))) {
                continue;
            }
            tx->AddTables()->SetTablePath(path);
        }
        tx->SetSnapshotStep(SnapshotId.Step);
        tx->SetSnapshotTxId(SnapshotId.TxId);
        if (proto->ignore_system_views()) {
            tx->SetIgnoreSystemViews(true);
        }

        ctx.Send(MakeTxProxyID(), req.Release());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            default:
                return StateFuncBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        using EStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;

        const auto* msg = ev->Get();
        const auto status = static_cast<EStatus>(msg->Record.GetStatus());
        auto issues = msg->Record.GetIssues();
        switch (status) {
            case EStatus::ExecComplete:
                if (msg->Record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS) {
                    return ReplySuccess(ctx);
                }
                break;

            case EStatus::ExecError:
                if (msg->Record.GetStatusCode() == NKikimrIssues::TStatusIds::PATH_NOT_EXIST) {
                    return Reply(Ydb::StatusIds::NOT_FOUND, issues, ctx);
                }
                break;

            case EStatus::AccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, issues, ctx);

            case EStatus::ResolveError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, issues, ctx);

            case EStatus::Unknown:
            case EStatus::ProxyShardUnknown:
            case EStatus::CoordinatorUnknown:
                return Reply(Ydb::StatusIds::UNDETERMINED, issues, ctx);

            case EStatus::ExecTimeout:
                return Reply(Ydb::StatusIds::TIMEOUT, issues, ctx);

            case EStatus::CoordinatorDeclined:
            case EStatus::ProxyShardNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);

            case EStatus::ProxyShardOverloaded:
                return Reply(Ydb::StatusIds::OVERLOADED, issues, ctx);

            default:
                break;
        }

        // Unexpected error
        NYql::IssueToMessage(
            MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Got unexpected status " << status << " from tx proxy"),
            issues.Add());
        return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
    }

    void ReplySuccess(const TActorContext& ctx) {
        Ydb::ClickhouseInternal::RefreshSnapshotResult result;
        result.set_snapshot_id(SnapshotId.ToUri());
        Request().SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKikhouseDiscardSnapshotRPC
    : public TRpcOperationRequestActor<TKikhouseDiscardSnapshotRPC, TEvKikhouseDiscardSnapshotRequest>
{
    using TBase = TRpcOperationRequestActor<TKikhouseDiscardSnapshotRPC, TEvKikhouseDiscardSnapshotRequest>;

private:
    TKikhouseSnapshotId SnapshotId;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        const auto* proto = GetProtoRequest();

        if (!SnapshotId.Parse(proto->snapshot_id())) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Valid snapshot id is required"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        if (proto->path_size() <= 0) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "At least one table path is required"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        TDuration reqTimeout = TDuration::Zero();

        // Let tx proxy try to kill request after cancel timeout
        MergeTimeout(reqTimeout, GetCancelAfter());

        // It's pointless to wait longer than operation timeout
        MergeTimeout(reqTimeout, GetOperationTimeout());

        // Use default timeout in the common case
        if (!reqTimeout) {
            reqTimeout = TDuration::MilliSeconds(REQUEST_TIMEOUT_MS);
        }

        auto req = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        req->Record.SetExecTimeoutPeriod(reqTimeout.MilliSeconds());

        auto token = Request_->GetSerializedToken();
        if (!token.empty()) {
            req->Record.SetUserToken(token);
        }

        SetDatabase(req.Get(), Request());

        auto* tx = req->Record.MutableTransaction()->MutableDiscardVolatileSnapshot();
        for (const TString& path : proto->path()) {
            if (proto->ignore_system_views() && TryParseLocalDbPath(::NKikimr::SplitPath(path))) {
                continue;
            }
            tx->AddTables()->SetTablePath(path);
        }
        tx->SetSnapshotStep(SnapshotId.Step);
        tx->SetSnapshotTxId(SnapshotId.TxId);
        if (proto->ignore_system_views()) {
            tx->SetIgnoreSystemViews(true);
        }

        ctx.Send(MakeTxProxyID(), req.Release());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            default:
                return StateFuncBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        using EStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;

        const auto* msg = ev->Get();
        const auto status = static_cast<EStatus>(msg->Record.GetStatus());
        auto issues = msg->Record.GetIssues();
        switch (status) {
            case EStatus::ExecComplete:
                if (msg->Record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS) {
                    return ReplySuccess(ctx);
                }
                break;

            case EStatus::ExecAlready:
                return ReplySuccess(ctx);

            case EStatus::AccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, issues, ctx);

            case EStatus::ResolveError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, issues, ctx);

            case EStatus::Unknown:
            case EStatus::ProxyShardUnknown:
            case EStatus::CoordinatorUnknown:
                return Reply(Ydb::StatusIds::UNDETERMINED, issues, ctx);

            case EStatus::ExecTimeout:
                return Reply(Ydb::StatusIds::TIMEOUT, issues, ctx);

            case EStatus::CoordinatorDeclined:
            case EStatus::ProxyShardNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);

            case EStatus::ProxyShardOverloaded:
                return Reply(Ydb::StatusIds::OVERLOADED, issues, ctx);

            default:
                break;
        }

        // Unexpected error
        NYql::IssueToMessage(
            MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Got unexpected status " << status << " from tx proxy"),
            issues.Add());
        return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
    }

    void ReplySuccess(const TActorContext& ctx) {
        Ydb::ClickhouseInternal::DiscardSnapshotResult result;
        result.set_snapshot_id(SnapshotId.ToUri());
        Request().SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

void DoKikhouseCreateSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TKikhouseCreateSnapshotRPC(p.release()));
}

void DoKikhouseRefreshSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TKikhouseRefreshSnapshotRPC(p.release()));
}

void DoKikhouseDiscardSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TKikhouseDiscardSnapshotRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
