#include "composite.h"

namespace NKikimr {
bool TTxCompositeReader::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    AFL_VERIFY(Pos < Children.size());
    while(Pos < Children.size()) {
        AFL_VERIFY(!Children[Pos]->GetIsFinished());
        //A child may by async, but if this composite reader is sync, we get all results from its children synchronously
        while(!Children[Pos]->GetIsFinished()) {
            const auto result = Children[Pos]->Execute(txc, ctx);
            if (!result || GetIsAsync()) {
                if (Children[Pos]->GetIsFinished()) {
                    ++Pos;
                }
                return result;
            }
        }
        ++Pos;
    }
    return true;
}

} //namespace NKikimr