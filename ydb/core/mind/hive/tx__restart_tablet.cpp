#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxRestartTablet : public TTransactionBase<THive> {
protected:
    TFullTabletId TabletId;
    TNodeId PreferredNodeId;
    TSideEffects SideEffects;
public:
    TTxRestartTablet(TFullTabletId tabletId, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , PreferredNodeId(0)
    {}

    TTxRestartTablet(TFullTabletId tabletId, TNodeId preferredNodeId, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , PreferredNodeId(preferredNodeId)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_RESTART_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        TTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            if (PreferredNodeId == 0) {
                BLOG_D("THive::TTxRestartTablet(" << tablet->ToString() << ")::Execute");
            } else {
                BLOG_D("THive::TTxRestartTablet(" << tablet->ToString() << " to node " << PreferredNodeId << ")::Execute");
            }
            if (!tablet->IsStopped()) {
                NIceDb::TNiceDb db(txc.DB);
                if (tablet->Node != nullptr) {
                    if (tablet->IsLeader()) {
                        db.Table<Schema::Tablet>().Key(tablet->GetLeader().Id).Update<Schema::Tablet::LeaderNode>(0);
                    } else {
                        db.Table<Schema::TabletFollowerTablet>().Key(tablet->GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                    }
                }
                tablet->InitiateStop(SideEffects, PreferredNodeId != 0);
            }
            tablet->InitiateBoot(PreferredNodeId);
            if (tablet->IsLeader()) {
                tablet->AsLeader().InitiateFollowersBoot();
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxRestartTablet(" << TabletId << ")::Complete SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateRestartTablet(TFullTabletId tabletId) {
    return new TTxRestartTablet(tabletId, this);
}

ITransaction* THive::CreateRestartTablet(TFullTabletId tabletId, TNodeId preferredNodeId) {
    return new TTxRestartTablet(tabletId, preferredNodeId, this);
}

} // NHive
} // NKikimr
