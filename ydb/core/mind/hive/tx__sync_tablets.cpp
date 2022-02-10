#include "hive_impl.h" 
#include "hive_log.h" 
 
namespace NKikimr { 
namespace NHive { 
 
class TTxSyncTablets : public TTransactionBase<THive> { 
    TActorId Local;
    NKikimrLocal::TEvSyncTablets SyncTablets; 
    THashSet<std::pair<TTabletId, TFollowerId>> TabletsToStop;
    THashSet<std::pair<TTabletId, TFollowerId>> TabletsToBoot;
public: 
    TTxSyncTablets(const TActorId &local, NKikimrLocal::TEvSyncTablets& rec, THive* hive)
        : TBase(hive) 
        , Local(local) 
    { 
        SyncTablets.Swap(&rec); 
    } 
 
    TTxType GetTxType() const override { return NHive::TXTYPE_SYNC_TABLETS; } 
 
    bool IsSameTablet(const TTabletInfo* local, const NKikimrLocal::TEvSyncTablets_TTabletInfo& remote) const { 
        if (local->IsFollower()) {
            if (remote.GetBootMode() == NKikimrLocal::EBootMode::BOOT_MODE_FOLLOWER) {
                if (local->AsFollower().Id == remote.GetFollowerId()
                        && (local->IsStopped() || local->IsAliveOnLocal(Local))) { 
                    return true; 
                } 
            } 
        } else { 
            if (remote.GetBootMode() == NKikimrLocal::EBootMode::BOOT_MODE_LEADER) {
                if (remote.GetGeneration() >= local->GetLeader().KnownGeneration) {
                    return true; 
                } 
            } 
        } 
        return false; 
    } 
 
    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxSyncTablets(" << Local << ")::Execute"); 
        NIceDb::TNiceDb db(txc.DB); 
        TNodeInfo& node = Self->GetNode(Local.NodeId()); 
        TabletsToStop.clear(); 
        TabletsToBoot.clear(); 
        for (const auto& t : node.Tablets) { 
            for (TTabletInfo* tablet : t.second) { 
                TabletsToStop.insert(tablet->GetFullTabletId()); 
            } 
        } 
        for (const NKikimrLocal::TEvSyncTablets_TTabletInfo& ti : SyncTablets.GetInbootTablets()) { 
            auto tabletId = std::pair<TTabletId, TFollowerId>(ti.GetTabletId(), ti.GetFollowerId());
            TTabletInfo* tablet = Self->FindTablet(tabletId); 
            if (tablet) { 
                if (IsSameTablet(tablet, ti)) { 
                    if (tablet->IsLeader()) {
                        tablet->GetLeader().KnownGeneration = ti.GetGeneration();
                    } 
                    tablet->BecomeStarting(node.Id); 
                    BLOG_TRACE("THive::TTxSyncTablets(" << Local << ") confirmed starting tablet " << tabletId); 
                    TabletsToStop.erase(tabletId); 
                    if (tablet->GetLeader().IsBootingSuppressed()) {
                        tablet->InitiateStop(); 
                    }
                    continue; 
                } 
            } else { 
                Self->StopTablet(Local, tabletId); 
                BLOG_TRACE("THive::TTxSyncTablets(" << Local << ") rejected unknown starting tablet " << tabletId); 
                TabletsToStop.erase(tabletId); 
            } 
        } 
        for (const NKikimrLocal::TEvSyncTablets_TTabletInfo& ti : SyncTablets.GetOnlineTablets()) { 
            auto tabletId = std::pair<TTabletId, TFollowerId>(ti.GetTabletId(), ti.GetFollowerId());
            TTabletInfo* tablet = Self->FindTablet(tabletId); 
            if (tablet) { 
                if (IsSameTablet(tablet, ti)) { 
                    if (tablet->IsLeader()) {
                        tablet->GetLeader().KnownGeneration = ti.GetGeneration();
                    } 
                    if (tablet->BecomeRunning(node.Id)) { 
                        if (tablet->IsLeader()) {
                            db.Table<Schema::Tablet>().Key(tablet->GetLeader().Id).Update(NIceDb::TUpdate<Schema::Tablet::LeaderNode>(tablet->NodeId),
                                                                                          NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(tablet->GetLeader().KnownGeneration));
                            TabletsToBoot.insert(tabletId); 
                        } else { 
                            db.Table<Schema::TabletFollowerTablet>().Key(tablet->GetFullTabletId()).Update(
                                        NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(tablet->AsFollower().FollowerGroup.Id),
                                        NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(tablet->NodeId));
                        } 
                    } 
                    BLOG_TRACE("THive::TTxSyncTablets(" << Local << ") confirmed running tablet " << tabletId); 
                    TabletsToStop.erase(tabletId); 
                    if (tablet->GetLeader().IsBootingSuppressed()) {
                        tablet->InitiateStop(); 
                    }
                    continue; 
                } else if (ti.GetBootMode() == NKikimrLocal::EBootMode::BOOT_MODE_FOLLOWER) {
                    Self->StopTablet(Local, tabletId); // the tablet is running somewhere else 
                    BLOG_TRACE("THive::TTxSyncTablets(" << Local << ") confirmed and stopped running tablet " << tabletId); 
                    TabletsToBoot.insert(tabletId); 
                    TabletsToStop.erase(tabletId); 
                    continue; 
                } 
            } else { 
                Self->StopTablet(Local, tabletId); 
                BLOG_TRACE("THive::TTxSyncTablets(" << Local << ") rejected unknown running tablet " << tabletId); 
                TabletsToStop.erase(tabletId); 
            } 
        } 
        return true; 
    } 
 
    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxSyncTablets(" << Local << ")::Complete"); 
        for (std::pair<TTabletId, TFollowerId> tabletId : TabletsToStop) {
            Self->Execute(Self->CreateRestartTablet(tabletId), ctx); 
        } 
        for (std::pair<TTabletId, TFollowerId> tabletId : TabletsToBoot) {
            TTabletInfo* tablet = Self->FindTablet(tabletId.first, tabletId.second); 
            if (tablet != nullptr) { 
                tablet->GetLeader().TryToBoot(); // for followers
            } 
        } 
        Self->ProcessBootQueue(); 
    } 
}; 
 
ITransaction* THive::CreateSyncTablets(const TActorId &local, NKikimrLocal::TEvSyncTablets& rec) {
    return new TTxSyncTablets(local, rec, this); 
} 
 
} // NHive 
} // NKikimr 
