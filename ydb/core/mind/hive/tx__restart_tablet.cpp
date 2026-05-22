#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
                YDB_LOG_DEBUG("THive::TTxRestartTablet( )::Execute",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"tablet", tablet->ToString()});
            } else {
                YDB_LOG_DEBUG("THive::TTxRestartTablet( to node )::Execute",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"tablet", tablet->ToString()},
                    {"PreferredNodeId", PreferredNodeId});
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
            if (tablet->IsLeader() && tablet->AsLeader().ChannelProfileNewGroup.any()) {
                tablet->AsLeader().InitiateAssignTabletGroups();
            } else {
                tablet->InitiateBoot(PreferredNodeId);
                if (tablet->IsLeader()) {
                    tablet->AsLeader().InitiateFollowersBoot();
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxRestartTablet( )::Complete",
            {"GetLogPrefix", GetLogPrefix()},
            {"TabletId", TabletId},
            {"SideEffects", SideEffects});
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
