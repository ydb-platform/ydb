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

void EnsureNoResourceKeys(const TVector<TType*>& columnTypes, const TVector<ui32>& keyColumns) {
    struct TVisitor : TExploringNodeVisitor {
        using TExploringNodeVisitor::Visit;

        void Visit(TResourceType&) override {
            HasResource = true;
        }

        bool HasResource = false;
    };

    for (ui32 keyColumn : keyColumns) {
        MKQL_ENSURE(keyColumn < columnTypes.size(), "join key column is out of range");
        TVisitor visitor;
        std::vector<TNode*> stack;
        visitor.Walk(columnTypes[keyColumn], stack);
        MKQL_ENSURE(!visitor.HasResource, "Resource types are not supported as hash join keys");
    }
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