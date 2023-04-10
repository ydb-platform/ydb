#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxRemoveWalleTask : public TTransactionBase<TCms> {
public:
    TTxRemoveWalleTask(TCms *self, const TString &id)
        : TBase(self)
        , Id(id)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_WALLE_TASK; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemoveWalleTask Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::WalleTask>().Key(Id).Delete();

        auto it = Self->State->WalleTasks.find(Id);
        if (it != Self->State->WalleTasks.end()) {
            Self->State->WalleRequests.erase(it->second.RequestId);
            Self->State->WalleTasks.erase(it);

            Self->AuditLog(ctx, TStringBuilder() << "Remove wall-e task"
                << ": id# " << Id);
        } else {
            LOG_ERROR(ctx, NKikimrServices::CMS, "Can't find Wall-E task %s", Id.data());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemoveWalleTask Complete");
    }

private:
    TString Id;
};

ITransaction *TCms::CreateTxRemoveWalleTask(const TString &id) {
    return new TTxRemoveWalleTask(this, id);
}

} // namespace NKikimr::NCms
