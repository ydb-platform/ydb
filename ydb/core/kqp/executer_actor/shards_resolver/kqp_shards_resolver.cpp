#include "kqp_shards_resolver.h"
#include "kqp_shards_resolver_events.h"

#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/set.h>


namespace NKikimr::NKqp {

using namespace NActors;
using namespace NYql;

namespace {

constexpr ui32 MAX_RETRIES_COUNT = 3;

class TKqpShardsResolver : public TActorBootstrapped<TKqpShardsResolver> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SHARDS_RESOLVER;
    }

public:
    TKqpShardsResolver(const TActorId& owner, ui64 txId, bool useFollowers, TSet<ui64>&& shardIds)
        : Owner(owner)
        , TxId(txId)
        , ShardIds(std::move(shardIds))
        , TabletResolver(MakePipePerNodeCacheID(useFollowers))
    {}

    void Bootstrap() {
        Y_ASSERT(ShardIds.size() > 0);

        for (ui64 tabletId : ShardIds) {
            LOG_LOG_S(*TlsActivationContext, NActors::NLog::PRI_TRACE, NKikimrServices::KQP_EXECUTER,
                "[ShardsResolver] TxId: " << TxId << ". " << "Send request about tabletId: " << tabletId);
            bool sent = Send(TabletResolver, new TEvPipeCache::TEvGetTabletNode(tabletId));
            Y_DEBUG_ABORT_UNLESS(sent);
        }

        Become(&TKqpShardsResolver::ResolveState);
    }

private:
    STATEFN(ResolveState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvGetTabletNodeResult, HandleResolve);
            cFunc(TEvents::TSystem::Poison, PassAway);
            default: {
                LOG_LOG_S(*TlsActivationContext, NActors::NLog::PRI_CRIT, NKikimrServices::KQP_EXECUTER,
                    "[ShardsResolver] TxId: " << TxId << ". " << "Unexpected event: " << ev->GetTypeRewrite());
                ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event while resolving shards");
            }
        }
    }

    void HandleResolve(TEvPipeCache::TEvGetTabletNodeResult::TPtr& ev) {
        auto* msg = ev->Get();
        LOG_LOG_S(*TlsActivationContext, NActors::NLog::PRI_TRACE, NKikimrServices::KQP_EXECUTER,
            "[ShardsResolver] TxId: " << TxId << ". " << "Got resolve event for tabletId: " << msg->TabletId << ", nodeId: " << msg->NodeId);
        if (msg->NodeId != 0) {
            Result[msg->TabletId] = msg->NodeId;
            if (Result.size() == ShardIds.size()) {
                LOG_LOG_S(*TlsActivationContext, NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER,
                    "[ShardsResolver] TxId: " << TxId << ". " << "Shard resolve complete, resolved shards: " << Result.size());
                return ReplyAndDie();
            }

            return;
        }

        ui32& retryCount = RetryCount[msg->TabletId];
        if (retryCount > MAX_RETRIES_COUNT) {
            TString reply = TStringBuilder() << "Failed to resolve tablet: " << msg->TabletId << " after several retries.";
            LOG_LOG_S(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_EXECUTER,
                "[ShardsResolver] TxId: " << TxId << ". " << reply);
            ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, std::move(reply));
            return;
        }

        ++retryCount;
        Send(TabletResolver, new TEvPipeCache::TEvGetTabletNode(msg->TabletId));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, TString&& message) {
        auto replyEv = std::make_unique<NShardResolver::TEvShardsResolveStatus>();
        replyEv->Status = status;
        replyEv->Issues.AddIssue(TIssue(message));
        Send(Owner, replyEv.release());
        PassAway();
    }

    void ReplyAndDie() {
        auto replyEv = std::make_unique<NShardResolver::TEvShardsResolveStatus>();
        replyEv->ShardsToNodes = std::move(Result);
        Send(Owner, replyEv.release());
        PassAway();
    }

private:
    const TActorId Owner;
    const ui64 TxId;
    const TSet<ui64> ShardIds;
    const TActorId TabletResolver;
    TMap<ui64, ui32> RetryCount;
    TMap<ui64, ui64> Result;
};

} // anonymous namespace

IActor* CreateKqpShardsResolver(const TActorId& owner, ui64 txId, bool useFollowers, TSet<ui64>&& shardIds) {
    return new TKqpShardsResolver(owner, txId, useFollowers, std::move(shardIds));
}

} // namespace NKikimr::NKqp
