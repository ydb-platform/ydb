#include "shard_impl.h"
#include "log.h"

namespace NKikimr {
namespace NGraph {

class TTxClearData : public TTransactionBase<TGraphShard> {
public:
    TTxClearData(TGraphShard* shard)
        : TBase(shard)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_CLEAR_DATA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxClearData::Execute");
        TInstant cutline = TActivationContext::Now() - TGraphShard::DURATION_TO_KEEP;
        switch (Self->BackendType) {
            case EBackendType::Memory:
                Self->MemoryBackend.ClearData(cutline, Self->StartTimestamp);
                return true;
            case EBackendType::Local:
                return Self->LocalBackend.ClearData(txc, cutline, Self->StartTimestamp);
            case EBackendType::External:
                break;
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxClearData::Complete");
    }
};

void TGraphShard::ExecuteTxClearData() {
    Execute(new TTxClearData(this));
}

} // NGraph
} // NKikimr

