#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

class TTxFindTabletSubDomainPathId : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    TTxFindTabletSubDomainPathId(TSchemeShard* self, TEvSchemeShard::TEvFindTabletSubDomainPathId::TPtr& ev)
        : TTransactionBase(self)
        , Ev(ev)
    { }

    TTxType GetTxType() const override { return TXTYPE_FIND_TABLET_SUBDOMAIN_PATH_ID; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        const auto* msg = Ev->Get();

        const ui64 tabletId = msg->Record.GetTabletId();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "FindTabletSubDomainPathId for tablet " << tabletId);

        auto it1 = Self->TabletIdToShardIdx.find(TTabletId(tabletId));
        if (it1 == Self->TabletIdToShardIdx.end()) {
            Result = MakeHolder<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult>(
                tabletId, NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SHARD_NOT_FOUND);
            return true;
        }

        auto shardIdx = it1->second;
        auto it2 = Self->ShardInfos.find(shardIdx);
        if (it2 == Self->ShardInfos.end()) {
            Result = MakeHolder<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult>(
                tabletId, NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SHARD_NOT_FOUND);
            return true;
        }

        auto& shardInfo = it2->second;
        auto path = TPath::Init(shardInfo.PathId, Self);
        if (!path) {
            Result = MakeHolder<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult>(
                tabletId, NKikimrScheme::TEvFindTabletSubDomainPathIdResult::PATH_NOT_FOUND);
            return true;
        }

        auto domainPathId = path.GetPathIdForDomain();
        Result = MakeHolder<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult>(
            tabletId, domainPathId.OwnerId, domainPathId.LocalPathId);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_VERIFY(Result);
        ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
    }

private:
    TEvSchemeShard::TEvFindTabletSubDomainPathId::TPtr Ev;
    THolder<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult> Result;
};

void TSchemeShard::Handle(TEvSchemeShard::TEvFindTabletSubDomainPathId::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxFindTabletSubDomainPathId(this, ev), ctx);
}

} // namespace NSchemeShard
} // namespace NKikimr
