#include "join_common.h"

namespace NKikimr {
namespace NKqp {
namespace NJoinRules {

void AddUsedIUs(TInfoUnitSet& usedIUs, const TVector<TInfoUnit>& ius) {
    usedIUs.insert(ius.begin(), ius.end());
}

TInfoUnit MakeUniqueInternalIU(int& varIdx, TInfoUnitSet& usedIUs) {
    for (;;) {
        auto iu = TInfoUnit("_rbo_arg_" + std::to_string(varIdx++));
        if (usedIUs.insert(iu).second) {
            return iu;
        }
    }
}

TRenameMap MakeRenameMap(const TVector<TInfoUnit>& ius, int& varIdx) {
    TInfoUnitSet usedIUs;
    return MakeRenameMap(ius, varIdx, usedIUs);
}

TRenameMap MakeRenameMap(const TVector<TInfoUnit>& ius, int& varIdx, TInfoUnitSet& usedIUs) {
    TRenameMap result;
    for (const auto& iu : ius) {
        result[iu] = MakeUniqueInternalIU(varIdx, usedIUs);
    }
    return result;
}

TVector<std::pair<TInfoUnit, TInfoUnit>> RemapRightJoinKeys(
    const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys,
    const TRenameMap& renameMap)
{
    TVector<std::pair<TInfoUnit, TInfoUnit>> result;
    result.reserve(joinKeys.size());

    for (const auto& [leftKey, rightKey] : joinKeys) {
        if (const auto it = renameMap.find(rightKey); it != renameMap.end()) {
            result.emplace_back(leftKey, it->second);
        } else {
            result.emplace_back(leftKey, rightKey);
        }
    }

    return result;
}

TIntrusivePtr<TOpMap> MakeMapFromRenames(
    const TIntrusivePtr<IOperator>& input,
    const TRenameMap& renameMap,
    TPositionHandle pos,
    TExprContext& ctx,
    TPlanProps& props)
{
    TVector<TMapElement> mapElements;
    mapElements.reserve(input->GetOutputIUs().size());

    for (const auto& iu : input->GetOutputIUs()) {
        const auto it = renameMap.find(iu);
        const auto toIU = it == renameMap.end() ? iu : it->second;
        mapElements.emplace_back(toIU, iu, pos, &ctx, &props);
    }

    return MakeIntrusive<TOpMap>(input, pos, mapElements);
}

TIntrusivePtr<TOpJoin> MakeJoinWithRightRenames(
    const TIntrusivePtr<IOperator>& leftInput,
    const TIntrusivePtr<IOperator>& rightInput,
    TPositionHandle pos,
    const TString& joinKind,
    const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys,
    const TVector<TExpression>& joinFilters,
    const TRenameMap& rightRenameMap,
    TExprContext& ctx,
    TPlanProps& props)
{
    auto renamedRightInput = rightInput;
    auto renamedJoinKeys = joinKeys;
    auto renamedJoinFilters = joinFilters;

    if (!rightRenameMap.empty()) {
        renamedRightInput = MakeMapFromRenames(rightInput, rightRenameMap, pos, ctx, props);
        renamedJoinKeys = RemapRightJoinKeys(joinKeys, rightRenameMap);
        for (auto& joinFilter : renamedJoinFilters) {
            joinFilter = joinFilter.ApplyRenames(rightRenameMap);
        }
    }

    return MakeIntrusive<TOpJoin>(leftInput, renamedRightInput, pos, joinKind, renamedJoinKeys, renamedJoinFilters);
}

} // namespace NJoinRules
} // namespace NKqp
} // namespace NKikimr
