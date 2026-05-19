#include "cms_impl.h"
#include "scheme.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NCms {

class TCms::TTxStoreFirstBootTimestamp : public TTransactionBase<TCms> {
public:
    TTxStoreFirstBootTimestamp(TCms *self)
        : TBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_STORE_FIRST_BOOT_TIMESTAMP; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxStoreFirstBootTimestamp Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Param>().Key(Schema::Param::Key)
            .Update<Schema::Param::FirstBootTimestamp>(Self->State->FirstBootTimestamp.MicroSeconds());

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStoreFirstBootTimestamp Complete");
    }
};

ITransaction *TCms::CreateTxStoreFirstBootTimestamp() {
    return new TTxStoreFirstBootTimestamp(this);
}

} // namespace NKikimr::NCms
