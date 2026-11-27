#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>

#include "kqp_opt_predicate_selectivity.h"

#include <util/generic/hash.h>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYql;
using namespace NYql::NNodes;

using TCountMinSketchPtr = std::shared_ptr<TCountMinSketch>;

// ydb-platform/ydb#55470: the selectivity of a predicate must be a function of the predicate and
// the (per-column) statistics of the base relation, not of the plan node the predicate happens to
// be attached to. Equivalent query forms — a chain of filters vs a single merged filter — place
// the same predicate at nodes with different row estimates, so any node-dependent normalization
// breaks the consistency of cardinality estimates and flips join strategy choices.

std::shared_ptr<TOptimizerStatistics> MakeStats(double nrows, THashMap<TString, TColumnStatistics> columns) {
    auto stats = std::make_shared<TOptimizerStatistics>();
    stats->Nrows = nrows;
    stats->ColumnStatistics = new TOptimizerStatistics::TColumnStatMap(std::move(columns));
    return stats;
}

TCountMinSketchPtr MakeSketch(ui64 width, ui64 depth, const TVector<std::pair<ui64, ui32>>& values) {
    TCountMinSketchPtr sketch(TCountMinSketch::Create(width, depth));
    for (const auto& [value, count] : values) {
        for (ui32 i = 0; i < count; ++i) {
            sketch->Count(reinterpret_cast<const char*>(&value), sizeof(value));
        }
    }
    return sketch;
}

TExprNode::TPtr MakeMember(TExprContext& ctx, TPositionHandle pos, const TString& name) {
    auto structArg = ctx.NewArgument(pos, "row");
    return ctx.NewCallable(pos, "Member", {structArg, ctx.NewAtom(pos, name)});
}

TExprNode::TPtr MakeInt64Literal(TExprContext& ctx, TPositionHandle pos, const TString& value) {
    return ctx.NewCallable(pos, "Int64", {ctx.NewAtom(pos, value)});
}

TExprNode::TPtr MakeEq(TExprContext& ctx, TPositionHandle pos, TExprNode::TPtr left, TExprNode::TPtr right) {
    return ctx.NewCallable(pos, "Eq", {std::move(left), std::move(right)});
}

double ComputeSelectivity(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& predicateBody) {
    TPredicateSelectivityComputer computer(stats);
    return computer.Compute(TExprBase(predicateBody));
}

Y_UNIT_TEST_SUITE(PredicateSelectivityConsistency) {

    // Equality on a column with a count-min sketch: the sketch stores counts over the base
    // relation (GetElementCount rows), so the selectivity must be identical at plan nodes with
    // different row estimates. On the unfixed code the estimate was probe/Nrows(node): the very
    // same literal predicate got different selectivity in a filter chain and in a merged filter,
    // e.g. 366/8.35e6 vs 366/5.76e8 on TPC-DS Q31 (fixture tpcds1000s.json: probe(2000)=366,
    // ElementCount=73049).
    Y_UNIT_TEST(EqualitySelectivityIndependentOfNodeRows) {
        TExprContext ctx;
        auto pos = ctx.AppendPosition({});

        constexpr ui64 Distinct = 201;
        auto sketch = MakeSketch(256, 8, {{2000, 12}, {1, 444}});

        TColumnStatistics dYear;
        dYear.CountMinSketch = sketch;
        dYear.NumUniqueVals = static_cast<double>(Distinct);
        dYear.Type = "Int64";

        THashMap<TString, TColumnStatistics> columns;
        columns["date_dim.d_year"] = dYear;

        auto statsSmall = MakeStats(8.35e6, columns);
        auto statsLarge = MakeStats(5.76e8, columns);

        auto predicate = MakeEq(ctx, pos,
            MakeMember(ctx, pos, "date_dim.d_year"),
            MakeInt64Literal(ctx, pos, "2000"));

        const double sSmall = ComputeSelectivity(statsSmall, predicate);
        const double sLarge = ComputeSelectivity(statsLarge, predicate);

        const ui64 value = 2000;
        const double expected = static_cast<double>(sketch->Probe(reinterpret_cast<const char*>(&value), sizeof(value)))
            / static_cast<double>(sketch->GetElementCount());

        UNIT_ASSERT_DOUBLES_EQUAL(expected, sSmall, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(expected, sLarge, 1e-12);
    }

    // Without a sketch, the equality default must come from the column's distinct count, which is
    // a property of the relation, not of the plan node.
    Y_UNIT_TEST(EqualityDefaultUsesDistinctCount) {
        TExprContext ctx;
        auto pos = ctx.AppendPosition({});

        constexpr double Distinct = 201.0;

        TColumnStatistics dYear;
        dYear.NumUniqueVals = Distinct;
        dYear.Type = "Int64";

        THashMap<TString, TColumnStatistics> columns;
        columns["date_dim.d_year"] = dYear;

        auto statsSmall = MakeStats(8.35e6, columns);
        auto statsLarge = MakeStats(5.76e8, columns);

        auto predicate = MakeEq(ctx, pos,
            MakeMember(ctx, pos, "date_dim.d_year"),
            MakeInt64Literal(ctx, pos, "2000"));

        const double sSmall = ComputeSelectivity(statsSmall, predicate);
        const double sLarge = ComputeSelectivity(statsLarge, predicate);

        UNIT_ASSERT_DOUBLES_EQUAL(1.0 / Distinct, sSmall, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(sSmall, sLarge, 1e-12);
    }

    // Composition: the selectivity of a conjunction must equal the product of the selectivities of
    // its conjuncts, evaluated against the same statistics, and the whole estimate must not depend
    // on the row estimate of the node.
    Y_UNIT_TEST(ConjunctionProductInvariantAcrossNodes) {
        TExprContext ctx;
        auto pos = ctx.AppendPosition({});

        auto sketchYear = MakeSketch(256, 8, {{2000, 12}, {1, 444}});
        auto sketchQoy = MakeSketch(256, 8, {{1, 114}, {2, 114}, {3, 114}, {4, 114}});

        TColumnStatistics dYear;
        dYear.CountMinSketch = sketchYear;
        dYear.NumUniqueVals = 201.0;
        dYear.Type = "Int64";

        TColumnStatistics dQoy;
        dQoy.CountMinSketch = sketchQoy;
        dQoy.NumUniqueVals = 4.0;
        dQoy.Type = "Int64";

        THashMap<TString, TColumnStatistics> columns;
        columns["date_dim.d_year"] = dYear;
        columns["date_dim.d_qoy"] = dQoy;

        auto statsSmall = MakeStats(8.35e6, columns);
        auto statsLarge = MakeStats(5.76e8, columns);

        auto pYear = MakeEq(ctx, pos,
            MakeMember(ctx, pos, "date_dim.d_year"),
            MakeInt64Literal(ctx, pos, "2000"));
        auto pQoy = MakeEq(ctx, pos,
            MakeMember(ctx, pos, "date_dim.d_qoy"),
            MakeInt64Literal(ctx, pos, "1"));
        auto pConj = ctx.NewCallable(pos, "And", {pYear, pQoy});

        const double sConjSmall = ComputeSelectivity(statsSmall, pConj);
        const double sConjLarge = ComputeSelectivity(statsLarge, pConj);

        const double sYear = ComputeSelectivity(statsSmall, pYear);
        const double sQoy = ComputeSelectivity(statsSmall, pQoy);

        UNIT_ASSERT_DOUBLES_EQUAL(sConjSmall, sYear * sQoy, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(sConjSmall, sConjLarge, 1e-12);
    }

}

} // namespace
