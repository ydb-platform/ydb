#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include "kqp_rbo_test_helpers.h"

namespace NKikimr::NKqp {
namespace {

using NTests::TIdTestContext;

void ReduceLeftJoins(TOpRoot& root, TRBOContext& ctx) {
    TVector<std::unique_ptr<IRule>> rules;
    rules.emplace_back(std::make_unique<TReduceLeftJoinRule>());
    TRuleBasedStage("Reduce left joins", std::move(rules)).RunStage(root, ctx);
}

struct TLeftJoinTest : TIdTestContext {
    const TInfoUnitId A = Id("a"), B = Id("b"), B2 = Id("b2"), C = Id("c");
    const TIntrusivePtr<TOpJoin> Left = MakeIntrusive<TOpJoin>(Read({A}), Read({B, B2}), Pos, "Left", TJoinIUs{{A, B}});

    void Check(TIntrusivePtr<IOperator> input, bool reduced) {
        auto root = Root(std::move(input), {});
        const auto outputs = Left->GetOutputIUs();
        ReduceLeftJoins(*root, RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(Left->JoinKind, reduced ? "Inner" : "Left");
        UNIT_ASSERT(Left->GetOutputIUs() == outputs);
        NTests::AssertIdInvariants(*root->GetInput(), root->PlanProps);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpRboReduceLeftJoin) {
    Y_UNIT_TEST(AncestorJoinKeysRespectPreservedInputs) {
        for (const TString kind : {"Inner", "Cross", "Left", "Right", "Full", "LeftSemi", "RightSemi", "LeftOnly", "RightOnly"}) {
            for (const bool onRight : {false, true}) {
                TLeftJoinTest f;
                TJoinIUs keys{{onRight ? f.C : f.B, onRight ? f.B : f.C}};
                auto parent = MakeIntrusive<TOpJoin>(onRight ? TIntrusivePtr<IOperator>(f.Read({f.C})) : f.Left,
                    onRight ? TIntrusivePtr<IOperator>(f.Left) : f.Read({f.C}), f.Pos, kind, std::move(keys));
                const bool expected = kind == "Inner" || kind == "Cross" || kind == "LeftSemi" || kind == "RightSemi" ||
                    (onRight && (kind == "Left" || kind == "LeftOnly")) ||
                    (!onRight && (kind == "Right" || kind == "RightOnly"));
                f.Check(parent, expected);
            }
        }
    }

    Y_UNIT_TEST(NullSafeKeyDoesNotRejectUnmatchedRows) {
        TLeftJoinTest f;
        auto parent = MakeIntrusive<TOpJoin>(f.Left, f.Read({f.C}), f.Pos, "Inner",
            TJoinIUs{{f.A, f.C}, {f.B, f.C, true}});
        f.Check(parent, false);
    }

    Y_UNIT_TEST(PreservedSideKeyDoesNotReduceLeftJoin) {
        TLeftJoinTest f;
        f.Check(MakeIntrusive<TOpJoin>(f.Left, f.Read({f.C}), f.Pos, "Inner", TJoinIUs{{f.A, f.C}}), false);
    }

    Y_UNIT_TEST(JoinFiltersAlsoRejectNulls) {
        TLeftJoinTest f;
        auto predicate = MakeBinaryPredicate(">", f.Column(f.B), f.Column(f.C));
        f.Check(MakeIntrusive<TOpJoin>(f.Left, f.Read({f.C}), f.Pos, "Inner", TJoinIUs{{f.A, f.C}},
            TVector<TExpression>{predicate}), true);
    }

    Y_UNIT_TEST(CascadingReductionsReachFixedPoint) {
        TLeftJoinTest f;
        const auto d = f.Id("d");
        auto upper = MakeIntrusive<TOpJoin>(f.Left, f.Read({f.C}), f.Pos, "Left", TJoinIUs{{f.B, f.C}});
        auto parent = MakeIntrusive<TOpJoin>(upper, f.Read({d}), f.Pos, "Inner", TJoinIUs{{f.C, d}});
        f.Check(parent, true);
        UNIT_ASSERT_VALUES_EQUAL(upper->JoinKind, "Inner");
    }

    Y_UNIT_TEST(FilterBooleanSemantics) {
        // The same nullable input has two columns: either can prove rejection
        // in a separate OR branch. A branch using only A must prevent reduction.
        for (size_t i = 0; i < 12; ++i) {
            TLeftJoinTest f;
            auto b = MakeBinaryPredicate(">", f.Column(f.B), f.Constant());
            auto b2 = MakeBinaryPredicate("!=", f.Column(f.B2), f.Constant());
            auto a = MakeBinaryPredicate("==", f.Column(f.A), f.Constant());
            auto exists = MakeUnaryCallable("Exists", f.Column(f.B));
            auto yes = MakeConstant("Bool", "true", f.Pos, &f.ExprCtx);
            auto no = MakeConstant("Bool", "false", f.Pos, &f.ExprCtx);
            const TVector<std::pair<TExpression, bool>> cases{
                {b, true},
                {MakeNegation(b), true},
                {MakeBinaryPredicate("And", b, a), true},
                {MakeBinaryPredicate("Or", b, b2), true},
                {MakeBinaryPredicate("Or", b, a), false},
                {exists, true},
                {MakeNegation(exists), false},
                {MakeBinaryPredicate("Coalesce", b, no), true},
                {MakeBinaryPredicate("Coalesce", b, yes), false},
                {MakeNegation(MakeBinaryPredicate("And", b, a)), false},
                {MakeNegation(MakeBinaryPredicate("Or", b, a)), true},
                {MakeUnaryCallable("UnknownFunction", f.Column(f.B)), false},
            };
            f.Check(MakeIntrusive<TOpFilter>(f.Left, f.Pos, cases[i].first), cases[i].second);
        }
    }

    Y_UNIT_TEST(MapPropagatesNullsButCoalesceCanMaskThem) {
        for (const bool coalesce : {false, true}) {
            TLeftJoinTest f;
            const auto computed = f.Id("computed");
            TMapIUs elements;
            elements.Add(computed, MakeBinaryPredicate(coalesce ? "Coalesce" : "+", f.Column(f.B), f.Constant()));
            auto map = MakeIntrusive<TOpMap>(f.Left, f.Pos, std::move(elements));
            f.Check(MakeIntrusive<TOpJoin>(map, f.Read({f.C}), f.Pos, "Inner", TJoinIUs{{computed, f.C}}), !coalesce);
        }
    }

    Y_UNIT_TEST(AncestorFilterCanCrossAnotherOuterJoin) {
        TLeftJoinTest f;
        auto parent = MakeIntrusive<TOpJoin>(f.Left, f.Read({f.C}), f.Pos, "Left", TJoinIUs{{f.A, f.C}});
        f.Check(MakeIntrusive<TOpFilter>(parent, f.Pos, MakeUnaryCallable("Exists", f.Column(f.B))), true);
        UNIT_ASSERT_VALUES_EQUAL(parent->JoinKind, "Left");
    }

    Y_UNIT_TEST(OuterJoinPropagatesNullsFromRejectedMatches) {
        for (const bool onRight : {false, true}) {
            for (const bool rejects : {false, true}) {
                TLeftJoinTest f;
                const auto key = rejects ? f.B : f.A;
                auto parent = MakeIntrusive<TOpJoin>(onRight ? TIntrusivePtr<IOperator>(f.Read({f.C})) : f.Left,
                    onRight ? TIntrusivePtr<IOperator>(f.Left) : f.Read({f.C}), f.Pos, "Full",
                    TJoinIUs{{onRight ? f.C : key, onRight ? key : f.C}});
                f.Check(MakeIntrusive<TOpFilter>(parent, f.Pos, MakeUnaryCallable("Exists", f.Column(f.C))), rejects);
                UNIT_ASSERT_VALUES_EQUAL(parent->JoinKind, "Full");
            }
        }
    }

    Y_UNIT_TEST(LimitTopSortAndAggregationAreBoundaries) {
        for (size_t i = 0; i < 4; ++i) {
            TLeftJoinTest f;
            TIntrusivePtr<IOperator> boundary;
            switch (i) {
                case 0:
                    boundary = MakeIntrusive<TOpLimit>(f.Left, f.Pos, f.Constant(), EOpPhase::Undefined);
                    break;
                case 1:
                    boundary = MakeIntrusive<TOpSort>(f.Left, f.Pos, TSortIUs{}, f.Constant());
                    break;
                case 2:
                    boundary = MakeIntrusive<TOpAggregate>(f.Left, TAggregationIUs{}, TOrderedIUs<>{f.B},
                        EOpPhase::Undefined, false, f.Pos);
                    break;
                default:
                    boundary = MakeIntrusive<TOpSort>(f.Left, f.Pos, TSortIUs{});
                    break;
            }
            f.Check(MakeIntrusive<TOpFilter>(boundary, f.Pos, MakeUnaryCallable("Exists", f.Column(f.B))), i == 3);
        }
    }

    Y_UNIT_TEST(SharedProducerIsNotSpecializedForOneConsumer) {
        TLeftJoinTest f;
        auto hub = TReplicate::Create(f.Left, f.Pos, f.Props.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        const auto otherB = second->GetRebindings().At(f.B);
        auto filtered = MakeIntrusive<TOpFilter>(first, f.Pos, MakeUnaryCallable("Exists", f.Column(f.B)));
        auto root = f.Root(MakeIntrusive<TOpJoin>(filtered, second, f.Pos, "Cross", TJoinIUs{}),
            {{f.B, "filtered"}, {otherB, "unfiltered"}});
        ReduceLeftJoins(*root, f.RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(f.Left->JoinKind, "Left");
    }

    Y_UNIT_TEST(SqlPreservesDuplicatesAndNullSafeMatches) {
        NKikimrConfig::TAppConfig config;
        auto* service = config.MutableTableServiceConfig();
        service->SetEnableNewRBO(true);
        service->SetEnableFallbackToYqlOptimizer(false);
        service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        // This rule is independent of hypergraph enumeration.
        service->SetDefaultCostBasedOptimizationLevel(0);
        NKikimr::NKqp::TKikimrRunner kikimr(NKqp::TKikimrSettings(config).SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schema = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/A` (id Int64 NOT NULL, x Int64, PRIMARY KEY(id));
            CREATE TABLE `/Root/B` (id Int64 NOT NULL, a_id Int64, y Int64, PRIMARY KEY(id));
            CREATE TABLE `/Root/C` (id Int64 NOT NULL, x Int64, y Int64, PRIMARY KEY(id));
        )").GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        auto insert = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/A` (id,x) VALUES (1,10),(2,20),(3,30);
            UPSERT INTO `/Root/B` (id,a_id,y) VALUES (10,1,7),(11,1,7),(12,3,NULL);
            UPSERT INTO `/Root/C` (id,x,y) VALUES (10,10,7),(11,20,NULL),(12,30,NULL);
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const bool nullSafe : {false, true}) {
            const TString query = TStringBuilder() << R"(
                PRAGMA YqlSelect = 'force';
                SELECT a.id FROM `/Root/A` AS a
                LEFT JOIN `/Root/B` AS b ON a.id=b.a_id
                INNER JOIN `/Root/C` AS c ON a.x=c.x AND b.y )"
                << (nullSafe ? "IS NOT DISTINCT FROM" : "=") << " c.y ORDER BY a.id;";
            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                nullSafe ? "[[1];[1];[2];[3]]" : "[[1];[1]]");
        }
    }
}

} // namespace NKikimr::NKqp
