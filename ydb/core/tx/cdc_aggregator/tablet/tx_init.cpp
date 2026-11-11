#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInit : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    explicit TTxInit(TCdcAggregator* self)
        : TTransactionBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext&, const TActorContext&) override {
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
