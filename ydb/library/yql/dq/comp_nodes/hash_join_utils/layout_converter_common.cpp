#include "layout_converter_common.h"
#include "tuple.h"

#include <yql/essentials/minikql/mkql_node_visitor.h>

namespace NKikimr::NMiniKQL {

void EnsureNoPgTypes(TType* type) {
    struct TVisitor : TExploringNodeVisitor {
        using TExploringNodeVisitor::Visit;

        void Visit(TPgType&) override {
            HasPg = true;
        }

        bool HasPg = false;
    } visitor;

    std::vector<TNode*> stack;
    visitor.Walk(type, stack);
    MKQL_ENSURE(!visitor.HasPg, "PG types are not supported in hash join");
}

i64 TPackResult::AllocatedBytes() const {
    return PackedTuples.size() + Overflow.size();
}

void TPackResult::AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout) {
    layout->TupleDeepCopy(tuple.PackedData, tuple.OverflowBegin, PackedTuples, Overflow);
    NTuples++;
}

} // namespace NKikimr::NMiniKQL
