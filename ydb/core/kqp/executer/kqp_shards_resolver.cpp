#include "kqp_shards_resolver.h"

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/kqp/executer/kqp_executer.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/set.h>


namespace NKikimr::NKqp {

using namespace NActors;
using namespace NYql;

namespace {

#define LOG__(prio, stream) LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::KQP_EXECUTER, \
    "[ShardsResolver] TxId: " << TxId << ". " << stream)

#define LOG_T(stream) LOG__(NActors::NLog::PRI_TRACE, stream)
#define LOG_D(stream) LOG__(NActors::NLog::PRI_DEBUG, stream)
#define LOG_W(stream) LOG__(NActors::NLog::PRI_WARN, stream)
#define LOG_E(stream) LOG__(NActors::NLog::PRI_ERROR, stream)
#define LOG_C(stream) LOG__(NActors::NLog::PRI_CRIT, stream)

constexpr ui32 MAX_RETRIES_COUNT = 3;

class TKqpShardsResolver : public TActorBootstrapped<TKqpShardsResolver> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SHARDS_RESOLVER; 
    }

public:
    TKqpShardsResolver(const TActorId& owner, ui64 txId, TSet<ui64>&& shardIds, float failRatio) 
        : Owner(owner) 
        , TxId(txId) 
        , ShardIds(std::move(shardIds))
        , MaxFailedShards(ShardIds.size() * failRatio) {}

    void Bootstrap() {
        auto tabletResolver = MakeTabletResolverID();
        auto resolveFlags = GetResolveFlags();

        Y_ASSERT(ShardIds.size() > 0);

        for (ui64 tabletId : ShardIds) {
            LOG_T("Send request about tabletId: " << tabletId);
            bool sent = Send(tabletResolver, new TEvTabletResolver::TEvForward(tabletId, nullptr, resolveFlags));
            Y_VERIFY_DEBUG(sent);
        }

        Become(&TKqpShardsResolver::ResolveState);
    }

private:
    STATEFN(ResolveState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletResolver::TEvForwardResult, HandleResolve);
            cFunc(TEvents::TSystem::Poison, PassAway);
            default: {
                LOG_C("Unexpected event: " << ev->GetTypeRewrite());
                ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event while resolving shards"); 
            }
        }
    }

    void HandleResolve(TEvTabletResolver::TEvForwardResult::TPtr& ev) {
        auto* msg = ev->Get();
        LOG_T("Got resolve event for tabletId: " << msg->TabletID << ": " << NKikimrProto::EReplyStatus_Name(msg->Status)
            << ", nodeId: " << msg->TabletActor.NodeId());

        if (msg->Status == NKikimrProto::EReplyStatus::OK) {
            Result[msg->TabletID] = msg->TabletActor.NodeId();

            if (Result.size() + FailedTablets == ShardIds.size()) {
                LOG_D("Done, success: " << Result.size() << ", failed: " << FailedTablets);
                ReplyAndDie(); 
                return;
            }

            return;
        }

        auto& state = States[msg->TabletID];
        if (state.Retries > MAX_RETRIES_COUNT) {
            ++FailedTablets;
            if (FailedTablets > MaxFailedShards) {
                LOG_W("Too many failed requests: " << FailedTablets << " (" << ShardIds.size() << ")");
                ReplyErrorAndDie(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder() 
                    << "Too many unresolved shards: " << FailedTablets); 
                return;
            }

            if (FailedTablets + Result.size() == ShardIds.size()) {
                LOG_D("Done, success: " << Result.size() << ", failed: " << FailedTablets);
                ReplyAndDie(); 
                return;
            }

            return; // no more retries for this tabletId
        }

        state.Retries++;

        // todo: backoff
        Send(MakeTabletResolverID(), new TEvTabletResolver::TEvForward(msg->TabletID, nullptr, GetResolveFlags()));
    }

    TEvTabletResolver::TEvForward::TResolveFlags GetResolveFlags() {
        TEvTabletResolver::TEvForward::TResolveFlags resolveFlags;
        resolveFlags.SetAllowFollower(false);
        resolveFlags.SetForceFollower(false);
        resolveFlags.SetPreferLocal(true);
        resolveFlags.SetForceLocal(false);

        return resolveFlags;
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, TString&& message) { 
        auto replyEv = MakeHolder<TEvKqpExecuter::TEvShardsResolveStatus>(); 
        replyEv->Status = status; 
        replyEv->Issues.AddIssue(TIssue(message)); 
        Send(Owner, replyEv.Release()); 
        PassAway(); 
    } 
 
    void ReplyAndDie() { 
        auto replyEv = MakeHolder<TEvKqpExecuter::TEvShardsResolveStatus>(); 
        replyEv->ShardNodes = std::move(Result); 
        replyEv->Unresolved = FailedTablets; 
        Send(Owner, replyEv.Release()); 
        PassAway(); 
    } 
 
private:
    const TActorId Owner; 
    const ui64 TxId;
    const TSet<ui64> ShardIds;
    const ui32 MaxFailedShards;

    struct TState {
        ui32 Retries = 0;
    };
    TMap<ui64, TState> States;
    ui32 FailedTablets = 0;

    TMap<ui64, ui64> Result;
};

} // anonymous namespace

IActor* CreateKqpShardsResolver(const TActorId& owner, ui64 txId, TSet<ui64>&& shardIds, float failRatio) { 
    return new TKqpShardsResolver(owner, txId, std::move(shardIds), failRatio); 
}

} // namespace NKikimr::NKqp
