#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxUpdateGroupLatencies : public TTransactionBase<TBlobStorageController> {
    THashSet<TGroupId> Ids;

public:
    TTxUpdateGroupLatencies(THashSet<TGroupId> ids, TBlobStorageController *controller)
        : TBase(controller)
        , Ids(std::move(ids))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_GROUP_LATENCIES; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        for (const TGroupId groupId : Ids) {
            TGroupInfo *groupInfo = Self->FindGroup(groupId);
            if (!groupInfo) {
                continue;
            }
            TGroupLatencyStats& stats = groupInfo->LatencyStats;

#define UPDATE_CELL(VALUE, COLUMN) \
            if (const auto& x = stats.VALUE) { \
                db.Table<Schema::GroupLatencies>().Key(groupId.GetRawId()).Update(NIceDb::TUpdate<Schema::GroupLatencies::COLUMN>(x->MicroSeconds())); \
            } else { \
                db.Table<Schema::GroupLatencies>().Key(groupId.GetRawId()).Update(NIceDb::TNull<Schema::GroupLatencies::COLUMN>()); \
            }

            UPDATE_CELL(PutTabletLog, PutTabletLogLatencyUs);
            UPDATE_CELL(PutUserData, PutUserDataLatencyUs);
            UPDATE_CELL(GetFast, GetFastLatencyUs);

            Self->SysViewChangedGroups.insert(groupId);
        }
        return true;
    }

    void Complete(const TActorContext&) override
    {}
};

void TBlobStorageController::Handle(TEvControllerCommitGroupLatencies::TPtr& ev) {
    THashSet<TGroupId> ids;

    auto& updates = ev->Get()->Updates;
    if (updates.size() <= GroupMap.size() / 2) {
        for (auto& [key, value] : updates) {
            if (TGroupInfo *group = FindGroup(key)) {
                group->LatencyStats = std::move(value);
                ids.insert(key);
            }
        }
    } else {
        auto updateIt = updates.begin();
        auto groupIt = GroupMap.begin();
        while (updateIt != updates.end() && groupIt != GroupMap.end()) {
            if (updateIt->first < groupIt->first) {
                ++updateIt;
            } else if (groupIt->first < updateIt->first) {
                ++groupIt;
            } else {
                groupIt->second->LatencyStats = std::move(updateIt->second);
                ids.insert(groupIt->first);
                ++groupIt;
                ++updateIt;
            }
        }
    }

    if (ids) {
        Execute(new TTxUpdateGroupLatencies(std::move(ids), this));
    }
}

}
}
