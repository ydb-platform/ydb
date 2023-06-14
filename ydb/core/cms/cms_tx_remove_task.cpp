#include "cms_impl.h"
#include "scheme.h"

namespace NKikimr::NCms {

template <typename TTable>
class TCms::TTxRemoveTask : public TTransactionBase<TCms> {
public:
    TTxRemoveTask(TCms *self, const TString &id,
            THashMap<TString, TTaskInfo> &tasks,
            THashMap<TString, TString> &requests)
        : TBase(self)
        , Id(id)
        , Tasks(tasks)
        , Requests(requests)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_TASK; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemoveTask Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Table<TTable>().Key(Id).Delete();

        auto it = Tasks.find(Id);
        if (it != Tasks.end()) {
            Requests.erase(it->second.RequestId);
            Tasks.erase(it);

            Self->AuditLog(ctx, TStringBuilder() << "Remove task"
                << ": id# " << Id);
        } else {
            LOG_ERROR(ctx, NKikimrServices::CMS, "Can't find task %s", Id.data());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemoveTask Complete");
    }

private:
    const TString Id;
    THashMap<TString, TTaskInfo> &Tasks;
    THashMap<TString, TString> &Requests;
};

ITransaction *TCms::CreateTxRemoveWalleTask(const TString &id) {
    return new TTxRemoveTask<Schema::WalleTask>(this, id, State->WalleTasks, State->WalleRequests);
}

ITransaction *TCms::CreateTxRemoveMaintenanceTask(const TString &id) {
    return new TTxRemoveTask<Schema::MaintenanceTasks>(this, id, State->MaintenanceTasks, State->MaintenanceRequests);
}

} // namespace NKikimr::NCms
