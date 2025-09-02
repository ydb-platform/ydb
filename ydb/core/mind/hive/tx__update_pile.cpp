#include "hive_impl.h"
#include "hive_log.h"

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
        BLOG_D("THive::TTxUpdatePiles()::Execute");
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
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxUpdatePiles()::Complete");
    }

};

ITransaction* THive::CreateUpdatePiles() {
    return new TTxUpdatePiles(this);
}
} // namespace NKikimr::NHive
