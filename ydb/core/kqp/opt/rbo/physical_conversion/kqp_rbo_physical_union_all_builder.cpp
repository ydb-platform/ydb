#include "kqp_rbo_physical_union_all_builder.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalUnionAllBuilder::ProjectInput(TExprNode::TPtr input, const TIntrusivePtr<IOperator>& inputOp) const {
    const auto inputOutput = inputOp->GetOutputIUs();
    const auto unionOutput = UnionAll->GetOutputIUs();
    THashSet<TInfoUnit, TInfoUnit::THashFunction> inputOutputSet;
    inputOutputSet.insert(inputOutput.begin(), inputOutput.end());

    TVector<std::pair<TString, TString>> renames;
    renames.reserve(UnionAll->Columns.size());
    bool identity = inputOutput.size() == unionOutput.size();
    for (size_t i = 0; i < UnionAll->Columns.size(); ++i) {
        const auto& column = UnionAll->Columns[i];
        Y_ENSURE(inputOutputSet.contains(column), "UnionAll column " << column.GetFullName() << " is not visible");
        renames.emplace_back(column.GetFullName(), column.GetFullName());
        identity = identity && inputOutput[i] == column;
    }

    if (identity) {
        return input;
    }
    return NPhysicalConvertionUtils::BuildRenameMap(input, renames, Ctx);
}

TExprNode::TPtr TPhysicalUnionAllBuilder::BuildPhysicalOp(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) {
    TVector<TExprNode::TPtr> extendArgs{
        ProjectInput(leftInput, UnionAll->GetLeftInput()),
        ProjectInput(rightInput, UnionAll->GetRightInput())
    };

    if (UnionAll->Ordered) {
        // clang-format off
        return Build<TCoOrderedExtend>(Ctx, Pos)
            .Add(extendArgs)
        .Done().Ptr();
        // clang-format on
    }

    // clang-format off
    return Build<TCoExtend>(Ctx, Pos)
        .Add(extendArgs)
    .Done().Ptr();
    // clang-format on
}
