#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

namespace NKikimr {
namespace NKqp {
namespace NJoinRules {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

void AddUsedIUs(TInfoUnitSet& usedIUs, const TVector<TInfoUnit>& ius);
TInfoUnit MakeUniqueInternalIU(int& varIdx, TInfoUnitSet& usedIUs);

TRenameMap MakeRenameMap(const TVector<TInfoUnit>& ius, int& varIdx);
TRenameMap MakeRenameMap(const TVector<TInfoUnit>& ius, int& varIdx, TInfoUnitSet& usedIUs);

TVector<std::pair<TInfoUnit, TInfoUnit>> RemapRightJoinKeys(
    const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys,
    const TRenameMap& renameMap);

TIntrusivePtr<TOpMap> MakeMapFromRenames(
    const TIntrusivePtr<IOperator>& input,
    const TRenameMap& renameMap,
    TPositionHandle pos,
    TExprContext& ctx,
    TPlanProps& props);

TIntrusivePtr<TOpJoin> MakeJoinWithRightRenames(
    const TIntrusivePtr<IOperator>& leftInput,
    const TIntrusivePtr<IOperator>& rightInput,
    TPositionHandle pos,
    const TString& joinKind,
    const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys,
    const TVector<TExpression>& joinFilters,
    const TRenameMap& rightRenameMap,
    TExprContext& ctx,
    TPlanProps& props);

} // namespace NJoinRules
} // namespace NKqp
} // namespace NKikimr
