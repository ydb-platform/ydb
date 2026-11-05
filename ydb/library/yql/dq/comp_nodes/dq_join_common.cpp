#include "dq_join_common.h"
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

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

TKeyTypes KeyTypesFromColumns(const TMKQLVector<TType*>& types, const TMKQLVector<ui32>& keyIndexes) {
    TKeyTypes kt;
    for (auto typeIndex : keyIndexes) {
        const TType* type = types[typeIndex];
        MKQL_ENSURE(type->IsData(), "exepected data type");
        kt.push_back(std::pair{*AS_TYPE(TDataType, type)->GetDataSlot(), false});
    }
    return kt;
}


} // namespace NKikimr::NMiniKQL