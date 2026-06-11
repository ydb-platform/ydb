#include "cms_impl.h"
#include "scheme.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

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
        YDB_LOG_DEBUG_CTX(ctx, "TTxRemoveTask Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Table<TTable>().Key(Id).Delete();

        auto it = Tasks.find(Id);
        if (it != Tasks.end()) {
            Requests.erase(it->second.RequestId);
            Tasks.erase(it);

            Self->AuditLog(ctx, TStringBuilder() << "Remove task"
                << ": id# " << Id);
        } else {
            YDB_LOG_ERROR_CTX(ctx, "Can't find task",
                {"id", Id.data()});
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TTxRemoveTask Complete");
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
