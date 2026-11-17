#include "impl.h"
#include "group_geometry_info.h"

#include <google/protobuf/util/message_differencer.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

    // Adopts storage pool pile overrides of the current StorageConfig. Distconf commits StorageConfig on its own, and the
    // groups could change after BSC had validated the proposed config, so this is the only point where the overrides
    // and the groups are known to agree: the check and the adoption can't interleave with the transactions creating
    // or changing groups. The overrides that don't fit are left unadopted (BSC keeps following the previous ones), and
    // the adoption is retried whenever groups or pools change.
    class TBlobStorageController::TTxUpdatePileOverrides : public TTransactionBase<TBlobStorageController> {
        bool Adopted = false;

    public:
        TTxUpdatePileOverrides(TBlobStorageController *self)
            : TTransactionBase(self)
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_PILE_OVERRIDES; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            Y_ABORT_UNLESS(Self->StorageConfig);
            const auto& config = *Self->StorageConfig;
            const auto& rules = config.GetBlobStorageConfig().GetStoragePoolPileOverrides();
            auto& applied = Self->AppliedPileOverrides;

            std::optional<TString> error;
            if (applied && std::equal(applied->GetRule().begin(), applied->GetRule().end(), rules.begin(), rules.end(),
                    [](const auto& x, const auto& y) { return google::protobuf::util::MessageDifferencer::Equals(x, y); })) {
                // nothing to adopt
            } else if (applied && (error = Self->CheckGroupsAgainstPileOverrides(rules))) {
                // the groups follow the adopted rules and don't fit the new ones
            } else {
                // the tablet adopting the overrides for the first time has been following the ones of StorageConfig
                // anyway, so they are taken as they are
                NKikimrBlobStorage::TStoragePoolPileOverrides value;
                value.MutableRule()->CopyFrom(rules);
                value.SetStorageConfigGeneration(config.GetGeneration());
                NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(true).Update<Schema::State::AppliedPileOverrides>(value);
                applied.emplace(std::move(value));
                Adopted = true;

                for (auto& [groupId, group] : Self->GroupMap) {
                    group->CalculateLayoutStatus(Self, [&] {
                        return TGroupGeometryInfo::FromStoragePool(group->Topology->GType,
                            Self->GetStoragePoolForPile(group->StoragePoolId, group->BridgePileId));
                    });
                }

                YDB_LOG_NOTICE("Storage pool pile overrides adopted",
                    {"marker", "BSCPO01"},
                    {"storageConfigGeneration", config.GetGeneration()},
                    {"numRules", rules.size()});
            }

            if (error && error != Self->PileOverridesError) {
                YDB_LOG_ERROR("Storage pool pile overrides of StorageConfig are not applied, the previous ones are in effect",
                    {"marker", "BSCPO02"},
                    {"storageConfigGeneration", config.GetGeneration()},
                    {"appliedStorageConfigGeneration", applied->GetStorageConfigGeneration()},
                    {"error", *error});
            }
            Self->PileOverridesError = std::move(error);
            Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_PILE_OVERRIDES_NOT_APPLIED] =
                Self->PileOverridesError.has_value();

            return true;
        }

        void Complete(const TActorContext&) override {
            if (Adopted) {
                Self->InitializeSelfHealState();
            }
        }
    };

    void TBlobStorageController::UpdatePileOverrides() {
        Execute(new TTxUpdatePileOverrides(this));
    }

} // NKikimr::NBsController
