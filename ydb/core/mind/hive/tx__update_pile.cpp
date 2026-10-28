#include "hive_impl.h"
#include "hive_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr::NHive {

class TTxUpdatePiles : public TTransactionBase<THive> {
protected:
    std::vector<TTabletInfo*> TabletsToRestart;
public:
    TTxUpdatePiles(THive* hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_PILES; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxUpdatePiles::Execute updating bridge piles",
            {"logPrefix", GetLogPrefix()});
        NIceDb::TNiceDb db(txc.DB);
        bool promotion = false;
        Y_ENSURE(Self->BridgeInfo);
        // INACTIVE pile is not a part of the cluster (the free slot of pile registry is INACTIVE too, and trailing free
        // slots are trimmed); its id may be given to another pile later, which must not inherit anything (like drain)
        // from this one, so it is forgotten
        for (auto it = Self->BridgePiles.begin(); it != Self->BridgePiles.end(); ) {
            const TBridgePileId pileId = it->first;
            if (const auto *pile = Self->BridgeInfo->FindPile(pileId); !pile ||
                    pile->State == NKikimrBridge::TClusterState::INACTIVE) {
                db.Table<Schema::BridgePile>().Key(pileId.GetLocalDb()).Delete();
                it = Self->BridgePiles.erase(it);
            } else {
                ++it;
            }
        }
        for (const auto& wardenPileInfo : Self->BridgeInfo->Piles) {
            auto pileId = wardenPileInfo.BridgePileId;
            if (wardenPileInfo.State == NKikimrBridge::TClusterState::INACTIVE) {
                continue;
            }
            auto [it, inserted] = Self->BridgePiles.try_emplace(pileId, wardenPileInfo);
            auto& pileInfo = it->second;
            if (!inserted && pileInfo == wardenPileInfo) {
                continue;
            }
            if (pileInfo.State != wardenPileInfo.State) {
                if (!NBridge::PileStateTraits(wardenPileInfo.State).AllowsConnection) {
                    for (const auto& [_, nodeInfo] : Self->Nodes) {
                        if (nodeInfo.BridgePileId != pileId) {
                            continue;
                        }
                        for (const auto& [state, tablets] : nodeInfo.Tablets) {
                            // not stopped: a stopped tablet whose instance still runs would be revived in place by the node's resync
                            if (state == TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN) {
                                continue;
                            }
                            TabletsToRestart.reserve(TabletsToRestart.size() + tablets.size());
                            TabletsToRestart.insert(TabletsToRestart.end(), tablets.begin(), tablets.end());
                        }
                    }
                }
            }
            if (!pileInfo.IsPromoted && wardenPileInfo.IsBeingPromoted) {
                promotion = true;
            }
            pileInfo.State = wardenPileInfo.State;
            pileInfo.IsPrimary = wardenPileInfo.IsPrimary;
            pileInfo.IsPromoted = wardenPileInfo.IsBeingPromoted;
            db.Table<Schema::BridgePile>().Key(pileId.GetLocalDb()).Update(
                NIceDb::TUpdate<Schema::BridgePile::State>(pileInfo.State),
                NIceDb::TUpdate<Schema::BridgePile::IsPrimary>(pileInfo.IsPrimary),
                NIceDb::TUpdate<Schema::BridgePile::IsPromoted>(pileInfo.IsPromoted)
            );
        }

        for (auto& [pileId, pileInfo] : Self->BridgePiles) {
            if (promotion && pileInfo.IsPrimary) {
                pileInfo.Drain = true;
                db.Table<Schema::BridgePile>().Key(pileInfo.GetId()).Update<Schema::BridgePile::Drain>(true);
            }
            if (pileInfo.Drain && !pileInfo.DrainActor) {
                pileInfo.DrainActor = Self->StartHiveDrain(pileId, TDrainSettings{.DownPolicy = NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_NO_DOWN, .Forward = false});
            }
        }

        for (auto* tablet : TabletsToRestart) {
            tablet->BecomeStopped(EHiveEventReason::PileUpdate);
            if (tablet->IsReadyToBoot()) {
                tablet->InitiateBoot();
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxUpdatePiles::Complete",
            {"logPrefix", GetLogPrefix()});
    }

};

ITransaction* THive::CreateUpdatePiles() {
    return new TTxUpdatePiles(this);
}
} // namespace NKikimr::NHive
