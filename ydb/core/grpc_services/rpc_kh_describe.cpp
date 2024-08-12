#include "service_coordination.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common/rpc_common.h"
#include "resolve_local_db_table.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/public/api/protos/ydb_clickhouse_internal.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <util/string/vector.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvKikhouseDescribeTableRequest = TGrpcRequestOperationCall<Ydb::ClickhouseInternal::DescribeTableRequest,
    Ydb::ClickhouseInternal::DescribeTableResponse>;

class TKikhouseDescribeTableRPC : public TActorBootstrapped<TKikhouseDescribeTableRPC> {
    using TBase = TActorBootstrapped<TKikhouseDescribeTableRPC>;

private:
    static constexpr ui32 DEFAULT_TIMEOUT_SEC = 5;

    std::unique_ptr<IRequestOpCtx> Request;
    Ydb::ClickhouseInternal::DescribeTableResult Result;

    TDuration Timeout;
    TActorId TimeoutTimerActorId;

    bool WaitingResolveReply;
    bool Finished;

    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    THolder<NKikimr::TKeyDesc> KeyRange;
    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> ResolveNamesResult;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TKikhouseDescribeTableRPC(std::unique_ptr<IRequestOpCtx>&& request)
        : TBase()
        , Request(std::move(request))
        , Timeout(TDuration::Seconds(DEFAULT_TIMEOUT_SEC))
        , WaitingResolveReply(false)
        , Finished(false)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        ResolveTable(ctx);
    }

    void Die(const NActors::TActorContext& ctx) override {
        Y_ABORT_UNLESS(Finished);
        Y_ABORT_UNLESS(!WaitingResolveReply);

        if (TimeoutTimerActorId) {
            ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }

        TBase::Die(ctx);
    }

private:
    STFUNC(StateWaitResolveTable) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvLocalSchemeTxResponse, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void ResolveTable(const NActors::TActorContext& ctx) {
        const TString table = TEvKikhouseDescribeTableRequest::GetProtoRequest(Request)->path();
        auto path = ::NKikimr::SplitPath(table);
        TMaybe<ui64> tabletId = TryParseLocalDbPath(path);
        if (tabletId) {
            if (Request->GetSerializedToken().empty() || !IsSuperUser(NACLib::TUserToken(Request->GetSerializedToken()), *AppData(ctx))) {
                return ReplyWithError(Ydb::StatusIds::NOT_FOUND, "Invalid table path specified", ctx);
            }

            std::unique_ptr<TEvTablet::TEvLocalSchemeTx> ev(new TEvTablet::TEvLocalSchemeTx());
            ctx.Send(MakePipePerNodeCacheID(true), new TEvPipeCache::TEvForward(ev.release(), *tabletId, true), IEventHandle::FlagTrackDelivery);

            TBase::Become(&TThis::StateWaitResolveTable);
            WaitingResolveReply = true;
        } else {
            TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = std::move(path);
            if (entry.Path.empty()) {
                return ReplyWithError(Ydb::StatusIds::NOT_FOUND, "Invalid table path specified", ctx);
            }
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            request->ResultSet.emplace_back(entry);
            ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

            TimeoutTimerActorId = CreateLongTimer(ctx, Timeout,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));

            TBase::Become(&TThis::StateWaitResolveTable);
            WaitingResolveReply = true;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev,  const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Got TEvDeliveryProblem, TabletId: " << ev->Get()->TabletId
                << ", NotDelivered: " << ev->Get()->NotDelivered);
        return ReplyWithError(Ydb::StatusIds::UNAVAILABLE, "Invalid table path specified", ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, "Request timed out", ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        ResolveNamesResult = ev->Get()->Request;

        return ProceedWithSchema(ctx);
    }

    void Handle(TEvTablet::TEvLocalSchemeTxResponse::TPtr &ev, const TActorContext &ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        ResolveNamesResult = new NSchemeCache::TSchemeCacheNavigate();
        auto &record = ev->Get()->Record;

        const TString table = TEvKikhouseDescribeTableRequest::GetProtoRequest(Request)->path();
        auto path = ::NKikimr::SplitPath(table);
        FillLocalDbTableSchema(*ResolveNamesResult, record.GetFullScheme(), path.back());
        ResolveNamesResult->ResultSet.back().Path = path;

        return ProceedWithSchema(ctx);
    }

    void ProceedWithSchema(const TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveNamesResult->ResultSet.size() == 1);
        const auto& entry = ResolveNamesResult->ResultSet.front();

        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, ToString(entry.Status), ctx);
        }

        TString errorMessage;
        if (!CheckAccess(errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, errorMessage, ctx);
        }

        TVector<TString> keyColumns;
        for (const auto& col : entry.Columns) {
            auto* colMeta = Result.add_columns();
            colMeta->set_name(col.second.Name);
            auto& typeInfo = col.second.PType;
            auto* item = colMeta->mutable_type();
            if (typeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
                auto* typeDesc = typeInfo.GetTypeDesc();
                auto* pg = item->mutable_pg_type();
                pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
                pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
            } else {
                item->mutable_optional_type()->mutable_item()
                    ->set_type_id((Ydb::Type::PrimitiveTypeId)typeInfo.GetTypeId());
            }
            if (col.second.KeyOrder == -1)
                continue;

            keyColumns.resize(Max<size_t>(keyColumns.size(), col.second.KeyOrder + 1));
            keyColumns[col.second.KeyOrder] = col.second.Name;

            KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), col.second.KeyOrder + 1));
            KeyColumnTypes[col.second.KeyOrder] = col.second.PType;
        }

        for (TString k : keyColumns) {
            Result.add_primary_key(k);
        }

        if (!TEvKikhouseDescribeTableRequest::GetProtoRequest(Request)->include_partitions_info()) {
            return ReplySuccess(ctx);
        }

        ResolveShards(ctx);
    }

    bool CheckAccess(TString& errorMessage) {
        if (Request->GetSerializedToken().empty())
            return true;

        NACLib::TUserToken userToken(Request->GetSerializedToken());

        const ui32 access = NACLib::EAccessRights::DescribeSchema;
        for (const NSchemeCache::TSchemeCacheNavigate::TEntry& entry : ResolveNamesResult->ResultSet) {
            if (access != 0 && entry.SecurityObject != nullptr &&
                    !entry.SecurityObject->CheckAccess(access, userToken))
            {
                TStringStream explanation;
                explanation << "Access denied for " << userToken.GetUserSID()
                            << " with access " << NACLib::AccessRightsToString(access)
                            << " to table [" << TEvKikhouseDescribeTableRequest::GetProtoRequest(Request)->path() << "]";

                errorMessage = explanation.Str();
                return false;
            }
        }
        return true;
    }

    void ResolveShards(const NActors::TActorContext& ctx) {
        auto& entry = ResolveNamesResult->ResultSet.front();

        if (entry.TableId.IsSystemView()) {
            // Add fake shard for sys view
            auto* p = Result.add_partitions();
            p->set_tablet_id(1);
            p->set_end_key("");
            p->set_end_key_inclusive(false);

            return ReplySuccess(ctx);
        } else if(TMaybe<ui64> tabletId = TryParseLocalDbPath(entry.Path)) {
            // Add fake shard for sys view
            auto* p = Result.add_partitions();
            p->set_tablet_id(*tabletId);
            p->set_end_key("");
            p->set_end_key_inclusive(false);

            return ReplySuccess(ctx);
        }

        // We are going to access all columns
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& ci : entry.Columns) {
            TKeyDesc::TColumnOp op = { ci.second.Id, TKeyDesc::EColumnOperation::Read, ci.second.PType, 0, 0 };
            columns.push_back(op);
        }

        TVector<TCell> minusInf(KeyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);
        KeyRange.Reset(new TKeyDesc(entry.TableId, range, TKeyDesc::ERowOperation::Read, KeyColumnTypes, columns));

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());

        request->ResultSet.emplace_back(std::move(KeyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        ctx.Send(MakeSchemeCacheID(), resolveReq.Release());

        TBase::Become(&TThis::StateWaitResolveShards);
        WaitingResolveReply = true;
    }

    STFUNC(StateWaitResolveShards) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev, const TActorContext &ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        TEvTxProxySchemeCache::TEvResolveKeySetResult *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Request->ResultSet.size() == 1);
        KeyRange = std::move(msg->Request->ResultSet[0].KeyDescription);

        if (msg->Request->ErrorCount > 0) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Failed to get partitions for table [%s]",
                TEvKikhouseDescribeTableRequest::GetProtoRequest(Request)->path().c_str()), ctx);
        }

        auto getShardsString = [] (const TVector<TKeyDesc::TPartitionInfo>& partitions) {
            TVector<ui64> shards;
            shards.reserve(partitions.size());
            for (auto& partition : partitions) {
                shards.push_back(partition.ShardId);
            }

            return JoinVectorIntoString(shards, ", ");
        };

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Table ["
                    << TEvKikhouseDescribeTableRequest::GetProtoRequest(Request)->path()
                    << "] shards: " << getShardsString(KeyRange->GetPartitions()));

        for (const TKeyDesc::TPartitionInfo& partition : KeyRange->GetPartitions()) {
            auto* p = Result.add_partitions();
            p->set_tablet_id(partition.ShardId);
            p->set_end_key(partition.Range->EndKeyPrefix.GetBuffer());
            p->set_end_key_inclusive(partition.Range->IsInclusive);
        }

        return ReplySuccess(ctx);
    }

    void ReplySuccess(const NActors::TActorContext& ctx) {
        Finished = true;
        ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
    }

    void ReplyWithError(StatusIds::StatusCode status, const TString& message, const TActorContext& ctx) {
        Finished = true;
        Request->RaiseIssue(NYql::TIssue(message));
        Request->ReplyWithYdbStatus(status);

        // We cannot Die() while scheme cache request is in flight because that request has pointer to
        // KeyRange member so we must not destroy it before we get the response
        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }

    void ReplyWithResult(StatusIds::StatusCode status,
                         const Ydb::ClickhouseInternal::DescribeTableResult& result,
                         const TActorContext& ctx) {
        Request->SendResult(result, status);

        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }
};

void DoKikhouseDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TKikhouseDescribeTableRPC(std::move(p)));
}

} // namespace NKikimr
} // namespace NGRpcService
