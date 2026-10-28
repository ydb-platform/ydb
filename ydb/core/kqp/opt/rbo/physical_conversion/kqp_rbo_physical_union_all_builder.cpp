#include "kqp_rbo_physical_union_all_builder.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalUnionAllBuilder::ProjectInput(TExprNode::TPtr input, ui32 childIndex) const {
    const auto& liveIn = GetLiveIn(&UnionAll, childIndex);
    const auto outputs = NPhysicalConvertionUtils::GetLiveOutputIUs(UnionAll);
    TVector<std::pair<TString, TString>> renames;
    renames.reserve(outputs.size());
    // Each input exposes its own IDs. Normalize to the fresh UnionAll output
    // IDs before Extend, including repeated inputs and zero-column rows.
    for (const auto output : outputs) {
        const auto source = UnionAll.GetColumns().Find(output)->Inputs.at(childIndex);
        Y_ENSURE(liveIn.Contains(source), "UnionAll input ID " << source << " is not live");
        renames.emplace_back(Names.Get(source), Names.Get(output));
    }
    return NPhysicalConvertionUtils::BuildRenameMap(input, renames, Ctx, UnionAll.Ordered);
}

TExprNode::TPtr TPhysicalUnionAllBuilder::BuildPhysicalOp(const TVector<TExprNode::TPtr>& inputs) {
    Y_ENSURE(inputs.size() == UnionAll.GetChildCount(), "UnionAll input count mismatch");

    TVector<TExprNode::TPtr> extendArgs;
    extendArgs.reserve(inputs.size());
    for (ui32 childIndex = 0; childIndex < inputs.size(); ++childIndex) {
        extendArgs.push_back(ProjectInput(inputs[childIndex], childIndex));
    }

    if (UnionAll.Ordered) {
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
