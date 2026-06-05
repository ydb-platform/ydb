#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

static bool IsTwoRowStruct(const TStructExprType* structType) {
    if (structType->GetSize() != 2) {
        return false;
    }
    const TItemExprType* itemNew = nullptr;
    const TItemExprType* itemOld = nullptr;
    for (const auto& item : structType->GetItems()) {
        if (item->GetName() == "new") {
            itemNew = item;
        } else if (item->GetName() == "old") {
            itemOld = item;
        }
    }
    if (!itemNew || !itemOld) {
        return false;
    }
    return itemNew->GetItemType()->GetKind() == ETypeAnnotationKind::Struct
        && itemOld->GetItemType()->GetKind() == ETypeAnnotationKind::Struct;
}

TExprBase KqpRewriteWriteConstraint(const TExprBase& node, TExprContext& ctx) {
    Y_ENSURE(node.Ref().ChildrenSize() == 2, "Invalid children for KqpWriteConstraint");
    auto input = node.Ref().Child(0);
    auto pgNotNullColumns = node.Ref().Child(1);
    Y_ENSURE(pgNotNullColumns->ChildrenSize());

    auto structArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("struct")
    .Done();

    auto structToCheck = structArg.Ptr();
    AFL_ENSURE(input->GetTypeAnn());
    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
        const auto* structType = input->GetTypeAnn()->Cast<TStructExprType>();
        if (IsTwoRowStruct(structType)) {
            structToCheck = Build<TCoMember>(ctx, node.Pos())
                .Struct(structArg)
                .Name().Build("new")
                .Done().Ptr();
        }
    }

    TExprNode::TPtr chainEnsure;
    for (ui32 i = 0; i < pgNotNullColumns->ChildrenSize(); i++) {
        auto column = pgNotNullColumns->ChildPtr(i)->Content();
        auto check = Build<TCoExists>(ctx, node.Pos())
            .Optional<TCoMember>()
                .Struct(i ? chainEnsure : structToCheck)
                .Name().Build(column)
            .Build()
        .Done();

        auto errorMessage = TStringBuilder()
            << "Tried to insert NULL value into NOT NULL column: "
            << column;

        chainEnsure = Build<TKqpEnsure>(ctx, node.Pos())
            .Value(structArg)
            .Predicate(check)
            .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_BAD_COLUMN_TYPE))
            .Message(MakeMessage(errorMessage, node.Pos(), ctx))
        .Done().Ptr();
    }

    return Build<TCoMap>(ctx, node.Pos())
        .Input(input)
        .Lambda()
            .Args(structArg)
            .Body(chainEnsure)
        .Build()
    .Done();
}

} // namespace NKikimr::NKqp::NOpt
