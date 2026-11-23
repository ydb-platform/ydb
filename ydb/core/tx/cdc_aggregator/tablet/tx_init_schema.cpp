#include "tablet_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInitSchema : public TTxBase {
public:
    explicit TTxInitSchema(TCdcAggregator* self)
        : TTxBase("TxInitSchema", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT_SCHEMA;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Complete");

        Self->RunTxInit(ctx);
    }
};

void TCdcAggregator::RunTxInitSchema(const TActorContext& ctx) {
    Execute(new TTxInitSchema(this), ctx);
}

} // namespace NKikimr::NCdcAggregator
