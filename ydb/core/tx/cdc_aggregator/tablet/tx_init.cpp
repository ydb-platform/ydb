#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInit : public TTxBase {
public:
    explicit TTxInit(TCdcAggregator* self)
        : TTxBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
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
        Self->SwitchToWork(ctx);
    }
};

void TCdcAggregator::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

} // namespace NKikimr::NCdcAggregator
