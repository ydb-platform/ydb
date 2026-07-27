#include "kqp_rbo_physical_union_all_builder.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalUnionAllBuilder::ProjectInput(TExprNode::TPtr input, ui32 childIndex) const {
    const auto inputOutput = UnionAll->Children[childIndex]->GetOutputIUs();
    const auto inputLiveOutput = NPhysicalConvertionUtils::GetLiveOutputIUs(*UnionAll->Children[childIndex]);
    const auto inputLiveIn = NPhysicalConvertionUtils::GetLiveInputIUs(*UnionAll, childIndex);
    const auto unionOutput = NPhysicalConvertionUtils::GetLiveOutputIUs(*UnionAll);
    THashSet<TInfoUnit, TInfoUnit::THashFunction> inputLiveInSet;
    inputLiveInSet.insert(inputLiveIn.begin(), inputLiveIn.end());

    TVector<std::pair<TString, TString>> renames;
    renames.reserve(unionOutput.size());
    // Extend inputs must have identical schemas.
    bool identity = inputOutput.size() == unionOutput.size()
        && inputLiveOutput.size() == unionOutput.size()
        && inputLiveIn.size() == unionOutput.size();
    for (size_t i = 0; i < unionOutput.size(); ++i) {
        const auto& column = unionOutput[i];
        Y_ENSURE(inputLiveInSet.contains(column), "UnionAll column " << column.GetFullName() << " is not visible");
        renames.emplace_back(column.GetFullName(), column.GetFullName());
        identity = identity && inputOutput[i] == column && inputLiveOutput[i] == column && inputLiveIn[i] == column;
    }

    if (identity) {
        return input;
    }
    return NPhysicalConvertionUtils::BuildRenameMap(input, renames, Ctx);
}

TExprNode::TPtr TPhysicalUnionAllBuilder::BuildPhysicalOp(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) {
    TVector<TExprNode::TPtr> extendArgs{
        ProjectInput(leftInput, 0),
        ProjectInput(rightInput, 1)
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
