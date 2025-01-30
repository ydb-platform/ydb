#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxUpdateSeenOperational : public TTransactionBase<TBlobStorageController> {
    TVector<TGroupId> GroupIds;

public:
    TTxUpdateSeenOperational(TVector<TGroupId> groupIds, TBlobStorageController *controller)
        : TBase(controller)
        , GroupIds(std::move(groupIds))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_SEEN_OPERATIONAL; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        for (const TGroupId groupId : GroupIds) {
            db.Table<Schema::Group>().Key(groupId.GetRawId()).Update<Schema::Group::SeenOperational>(true);
            Self->SysViewChangedGroups.insert(groupId);
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* TBlobStorageController::CreateTxUpdateSeenOperational(TVector<TGroupId> groupIds) {
    return new TTxUpdateSeenOperational(std::move(groupIds), this);
}

}
}
