#include "layout_converter_common.h"
#include "tuple.h"

#include <yql/essentials/minikql/mkql_node_visitor.h>

namespace NKikimr::NMiniKQL {

void EnsureNoPgTypes(TType* type) {
    TExploringNodeVisitor explorer;
    std::vector<TNode*> stack;
    explorer.Walk(type, stack);
    for (auto* node : explorer.GetNodes()) {
        MKQL_ENSURE(!node->GetType()->IsType() || !static_cast<TType*>(node)->IsPg(),
            "PG types are not supported in hash join");
    }
}

i64 TPackResult::AllocatedBytes() const {
    return PackedTuples.size() + Overflow.size();
}

void TPackResult::AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout) {
    layout->TupleDeepCopy(tuple.PackedData, tuple.OverflowBegin, PackedTuples, Overflow);
    NTuples++;
}

} // namespace NKikimr::NMiniKQL
