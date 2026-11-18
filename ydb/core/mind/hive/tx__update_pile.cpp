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
        for (const auto& wardenPileInfo : Self->BridgeInfo->Piles) {
            auto pileId = wardenPileInfo.BridgePileId;
            auto [it, inserted] = Self->BridgePiles.try_emplace(pileId, wardenPileInfo);
            auto& pileInfo = it->second;
            if (!inserted && pileInfo == wardenPileInfo) {
                continue;
            }
            if (pileInfo.State != wardenPileInfo.State) {
                if (!NBridge::PileStateTraits(wardenPileInfo.State).AllowsConnection) {
                    for (auto nodeId : pileInfo.Nodes) {
                        auto* nodeInfo = Self->FindNode(nodeId);
                        if (!nodeInfo) {
                            continue;
                        }
                        for (const auto& [_, tablets] : nodeInfo->Tablets) {
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
            if (pileInfo.Drain) {
                Self->StartHiveDrain(pileId, TDrainSettings{.DownPolicy = NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_NO_DOWN, .Forward = false});
            }
        }

        for (auto* tablet : TabletsToRestart) {
            tablet->BecomeStopped();
            if (tablet->IsReadyToBoot()) {
                tablet->InitiateBoot();
            }
        }

        // a pile may have stopped being synchronized, or BridgeTabletPlacement may have been switched back to
        // PRIMARY_PILE_ONLY: the tablets that are not allowed in their pile anymore are moved away by the pile drain
        Self->MarkPilesWithMisplacedTablets(db);
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxUpdatePiles::Complete",
            {"logPrefix", GetLogPrefix()});
<<<<<<< HEAD
=======
        Self->ProcessPileDrains();
        // the tablets waiting for a node may fit in the piles that became allowed
        Self->ProcessWaitQueue();
>>>>>>> b9ba65f528e (add setting in hive_config to have tablets in non-primary piles (#59308))
    }

};

void THive::MarkPilesWithMisplacedTablets(NIceDb::TNiceDb& db) {
    for (auto& [pileId, pileInfo] : BridgePiles) {
        if (pileInfo.Drain || !NBridge::PileStateTraits(pileInfo.State).AllowsConnection || !HasTabletsToDrain(pileId)) {
            continue;
        }
        YDB_LOG_NOTICE("Pile has tablets its placement does not allow, draining",
            {"logPrefix", GetLogPrefix()},
            {"pileId", pileId});
        pileInfo.Drain = true;
        db.Table<Schema::BridgePile>().Key(pileInfo.GetId()).Update<Schema::BridgePile::Drain>(true);
    }
}

ITransaction* THive::CreateUpdatePiles() {
    return new TTxUpdatePiles(this);
}
} // namespace NKikimr::NHive
