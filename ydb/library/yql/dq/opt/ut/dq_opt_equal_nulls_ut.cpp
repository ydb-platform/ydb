#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TString SettingValue(const TCoNameValueTuple& setting) {
    if (const auto atom = setting.Value().Maybe<TCoAtom>()) {
        return TString(atom.Cast().Value());
    }
    return TString(setting.Value().Cast<TCoUint32>().Literal().Value());
}

TVector<std::pair<TString, TString>> SettingPairs(const TVector<TCoNameValueTuple>& settings) {
    TVector<std::pair<TString, TString>> pairs;
    pairs.reserve(settings.size());
    for (const auto& setting : settings) {
        pairs.emplace_back(TString(setting.Name().Value()), SettingValue(setting));
    }
    return pairs;
}

} // namespace

Y_UNIT_TEST_SUITE(DqOptEqualNulls) {

Y_UNIT_TEST(GraceJoinHasNoSettings) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    UNIT_ASSERT(BuildBlockHashJoinSettings(pos, EJoinAlgoType::GraceJoin, ctx).empty());
}

Y_UNIT_TEST(ReverseJoinKeepsBuildSide) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    UNIT_ASSERT_VALUES_EQUAL(
        SettingPairs(BuildBlockHashJoinSettings(pos, EJoinAlgoType::ReverseBlockJoin, ctx)),
        (TVector<std::pair<TString, TString>>{{"BuildSide", "Left"}}));
}

} // DqOptEqualNulls
