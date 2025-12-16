#include "schemeshard_forced_compaction.h"
#include "schemeshard_xxport__get.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxGet: public TSchemeShard::TXxport::TTxGet<
        TForcedCompactionInfo,
        TEvForcedCompaction::TEvGetForcedCompactionRequest,
        TEvForcedCompaction::TEvGetForcedCompactionResponse
> {
    using TTxGetBase::TTxGetBase;

    TTxType GetTxType() const override {
        return TXTYPE_GET_FORCED_COMPACTION;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        return DoExecuteImpl(Self->ForcedCompactions, txc, ctx);
    }

}; // TTxGet

ITransaction* TSchemeShard::CreateTxGetForcedCompaction(TEvForcedCompaction::TEvGetForcedCompactionRequest::TPtr& ev) {
    return new TForcedCompaction::TTxGet(this, ev);
}

} // NSchemeShard
} // NKikimr
