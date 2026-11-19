#include "tablet_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInit : public TTxBase {
public:
    explicit TTxInit(TCdcAggregator* self)
        : TTxBase("TxInit", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Execute");

        Self->CdcAggregatorStream.reset();
        NIceDb::TNiceDb db(txc.DB);
        auto [ready, stream] = Self->CdcAggregatorStreamRepository.LoadCdcAggregatorStream(db);
        if (!ready) {
            return false;
        }
        Self->CdcAggregatorStream = std::move(stream);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Complete",
            {"configured", Self->CdcAggregatorStream.has_value()});

        if (Self->CdcAggregatorStream) {
            if (const auto& currentHeartbeat = Self->CdcAggregatorStream->GetCurrentHeartbeat()) {
                Self->BroadcastHeartbeat(*currentHeartbeat, ctx);
            }
        }
        Self->SwitchToWork(ctx);
    }
};

void TCdcAggregator::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

} // namespace NKikimr::NCdcAggregator
