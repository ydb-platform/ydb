#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include "kqp_rbo_test_helpers.h"

namespace NKikimr::NKqp {
namespace {

using NTests::TIdTestContext;

void ReduceOuterJoins(TOpRoot& root, TRBOContext& ctx) {
    TVector<std::unique_ptr<IRule>> rules;
    rules.emplace_back(std::make_unique<TReduceOuterJoinRule>());
    TRuleBasedStage("Reduce outer joins", std::move(rules)).RunStage(root, ctx);
}

struct TOuterJoinTest : TIdTestContext {
    const TInfoUnitId A = Id("a"), B = Id("b"), B2 = Id("b2"), C = Id("c");
    const TIntrusivePtr<TOpJoin> Join;

    explicit TOuterJoinTest(const TString& kind = "Left")
        : Join(MakeIntrusive<TOpJoin>(Read({A}), Read({B, B2}), Pos, kind, TJoinIUs{{A, B}})) {}

    void Check(TIntrusivePtr<IOperator> input, TStringBuf expectedKind) {
        auto root = Root(std::move(input), {});
        const auto outputs = Join->GetOutputIUs();
        ReduceOuterJoins(*root, RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(Join->JoinKind, expectedKind);
        UNIT_ASSERT(Join->GetOutputIUs() == outputs);
        NTests::AssertIdInvariants(*root->GetInput(), root->PlanProps);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpRboReduceOuterJoin) {
    Y_UNIT_TEST(FilterReducesEachOuterJoinSide) {
        // Predicates reject neither NULL-extended input, A, B, or both.
        const TVector<std::pair<TString, TVector<TString>>> cases{
            {"Left", {"Left", "Left", "Inner", "Inner"}},
            {"Right", {"Right", "Inner", "Right", "Inner"}},
            {"Full", {"Full", "Left", "Right", "Inner"}},
        };
        for (const auto& [kind, expected] : cases) {
            for (size_t mask = 0; mask < expected.size(); ++mask) {
                TOuterJoinTest f(kind);
                auto predicate = MakeConstant("Bool", "true", f.Pos, &f.ExprCtx);
                if (mask & 1) {
                    predicate = MakeUnaryCallable("Exists", f.Column(f.A));
                }
                if (mask & 2) {
                    predicate = MakeBinaryPredicate("And", predicate, MakeUnaryCallable("Exists", f.Column(f.B)));
                }
                f.Check(MakeIntrusive<TOpFilter>(f.Join, f.Pos, predicate), expected[mask]);
            }
        }
    }

    Y_UNIT_TEST(FullJoinAncestorKeysReduceEachSide) {
        const TVector<TString> expected{"Left", "Right", "Inner"};
        for (size_t mask = 1; mask <= 3; ++mask) {
            TOuterJoinTest f("Full");
            TJoinIUs keys;
            if (mask & 1) {
                keys.Add(f.A, f.C);
            }
            if (mask & 2) {
                keys.Add(f.B, f.C);
            }
            f.Check(MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Inner", std::move(keys)), expected[mask - 1]);
        }
    }

    Y_UNIT_TEST(NullSafeKeysPreserveRightAndFullJoins) {
        for (const TString kind : {"Right", "Full"}) {
            TOuterJoinTest f(kind);
            f.Check(MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Inner",
                TJoinIUs{{f.A, f.C, true}, {f.B, f.C, true}}), kind);
        }
    }

    Y_UNIT_TEST(FullJoinOrKeepsBothUnmatchedSides) {
        TOuterJoinTest f("Full");
        auto predicate = MakeBinaryPredicate("Or", MakeUnaryCallable("Exists", f.Column(f.A)),
            MakeUnaryCallable("Exists", f.Column(f.B)));
        f.Check(MakeIntrusive<TOpFilter>(f.Join, f.Pos, predicate), "Full");
    }

    Y_UNIT_TEST(FullJoinReductionExposesRightJoinReduction) {
        TOuterJoinTest f("Right");
        auto parent = MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Full", TJoinIUs{{f.A, f.C}});
        f.Check(MakeIntrusive<TOpFilter>(parent, f.Pos, MakeUnaryCallable("Exists", f.Column(f.C))), "Inner");
        UNIT_ASSERT_VALUES_EQUAL(parent->JoinKind, "Right");
    }

    Y_UNIT_TEST(AncestorJoinKeysRespectPreservedInputs) {
        for (const TString kind : {"Inner", "Cross", "Left", "Right", "Full", "LeftSemi", "RightSemi", "LeftOnly", "RightOnly"}) {
            for (const bool onRight : {false, true}) {
                TOuterJoinTest f;
                TJoinIUs keys{{onRight ? f.C : f.B, onRight ? f.B : f.C}};
                auto parent = MakeIntrusive<TOpJoin>(onRight ? TIntrusivePtr<IOperator>(f.Read({f.C})) : f.Join,
                    onRight ? TIntrusivePtr<IOperator>(f.Join) : f.Read({f.C}), f.Pos, kind, std::move(keys));
                const bool expected = kind == "Inner" || kind == "Cross" || kind == "LeftSemi" || kind == "RightSemi" ||
                    (onRight && (kind == "Left" || kind == "LeftOnly")) ||
                    (!onRight && (kind == "Right" || kind == "RightOnly"));
                f.Check(parent, expected ? "Inner" : "Left");
            }
        }
    }

    Y_UNIT_TEST(NullSafeKeyDoesNotRejectUnmatchedRows) {
        TOuterJoinTest f;
        auto parent = MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Inner",
            TJoinIUs{{f.A, f.C}, {f.B, f.C, true}});
        f.Check(parent, "Left");
    }

    Y_UNIT_TEST(PreservedSideKeyDoesNotReduceLeftJoin) {
        TOuterJoinTest f;
        f.Check(MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Inner", TJoinIUs{{f.A, f.C}}), "Left");
    }

    Y_UNIT_TEST(JoinFiltersAlsoRejectNulls) {
        TOuterJoinTest f;
        auto predicate = MakeBinaryPredicate(">", f.Column(f.B), f.Column(f.C));
        f.Check(MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Inner", TJoinIUs{{f.A, f.C}},
            TVector<TExpression>{predicate}), "Inner");
    }

    Y_UNIT_TEST(CascadingReductionsReachFixedPoint) {
        TOuterJoinTest f;
        const auto d = f.Id("d");
        auto upper = MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Left", TJoinIUs{{f.B, f.C}});
        auto parent = MakeIntrusive<TOpJoin>(upper, f.Read({d}), f.Pos, "Inner", TJoinIUs{{f.C, d}});
        f.Check(parent, "Inner");
        UNIT_ASSERT_VALUES_EQUAL(upper->JoinKind, "Inner");
    }

    Y_UNIT_TEST(FilterBooleanSemantics) {
        // The same nullable input has two columns: either can prove rejection
        // in a separate OR branch. A branch using only A must prevent reduction.
        for (size_t i = 0; i < 12; ++i) {
            TOuterJoinTest f;
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
            f.Check(MakeIntrusive<TOpFilter>(f.Join, f.Pos, cases[i].first), cases[i].second ? "Inner" : "Left");
        }
    }

    Y_UNIT_TEST(MapPropagatesNullsButCoalesceCanMaskThem) {
        for (const bool coalesce : {false, true}) {
            TOuterJoinTest f;
            const auto computed = f.Id("computed");
            TMapIUs elements;
            elements.Add(computed, MakeBinaryPredicate(coalesce ? "Coalesce" : "+", f.Column(f.B), f.Constant()));
            auto map = MakeIntrusive<TOpMap>(f.Join, f.Pos, std::move(elements));
            f.Check(MakeIntrusive<TOpJoin>(map, f.Read({f.C}), f.Pos, "Inner", TJoinIUs{{computed, f.C}}), coalesce ? "Left" : "Inner");
        }
    }

    Y_UNIT_TEST(AncestorFilterCanCrossAnotherOuterJoin) {
        TOuterJoinTest f;
        auto parent = MakeIntrusive<TOpJoin>(f.Join, f.Read({f.C}), f.Pos, "Left", TJoinIUs{{f.A, f.C}});
        f.Check(MakeIntrusive<TOpFilter>(parent, f.Pos, MakeUnaryCallable("Exists", f.Column(f.B))), "Inner");
        UNIT_ASSERT_VALUES_EQUAL(parent->JoinKind, "Left");
    }

    Y_UNIT_TEST(OuterJoinPropagatesNullsFromRejectedMatches) {
        for (const bool onRight : {false, true}) {
            for (const bool rejects : {false, true}) {
                TOuterJoinTest f;
                const auto key = rejects ? f.B : f.A;
                auto parent = MakeIntrusive<TOpJoin>(onRight ? TIntrusivePtr<IOperator>(f.Read({f.C})) : f.Join,
                    onRight ? TIntrusivePtr<IOperator>(f.Join) : f.Read({f.C}), f.Pos, "Full",
                    TJoinIUs{{onRight ? f.C : key, onRight ? key : f.C}});
                f.Check(MakeIntrusive<TOpFilter>(parent, f.Pos, MakeUnaryCallable("Exists", f.Column(f.C))), rejects ? "Inner" : "Left");
                UNIT_ASSERT_VALUES_EQUAL(parent->JoinKind, onRight ? "Left" : "Right");
            }
        }
    }

    Y_UNIT_TEST(LimitTopSortAndAggregationAreBoundaries) {
        for (const TString kind : {"Left", "Right", "Full"}) {
            for (size_t i = 0; i < 4; ++i) {
                TOuterJoinTest f(kind);
                TIntrusivePtr<IOperator> boundary;
                switch (i) {
                    case 0:
                        boundary = MakeIntrusive<TOpLimit>(f.Join, f.Pos, f.Constant(), EOpPhase::Undefined);
                        break;
                    case 1:
                        boundary = MakeIntrusive<TOpSort>(f.Join, f.Pos, TSortIUs{}, f.Constant());
                        break;
                    case 2:
                        boundary = MakeIntrusive<TOpAggregate>(f.Join, TAggregationIUs{}, TOrderedIUs<>{f.A, f.B},
                            EOpPhase::Undefined, false, f.Pos);
                        break;
                    default:
                        boundary = MakeIntrusive<TOpSort>(f.Join, f.Pos, TSortIUs{});
                        break;
                }
                auto predicate = MakeBinaryPredicate("And", MakeUnaryCallable("Exists", f.Column(f.A)),
                    MakeUnaryCallable("Exists", f.Column(f.B)));
                f.Check(MakeIntrusive<TOpFilter>(boundary, f.Pos, predicate), i == 3 ? "Inner" : kind);
            }
        }
    }

    Y_UNIT_TEST(SharedProducerIsNotSpecializedForOneConsumer) {
        for (const TString kind : {"Left", "Right", "Full"}) {
            TOuterJoinTest f(kind);
            auto hub = TReplicate::Create(f.Join, f.Pos, f.Props.InfoUnitRegistry);
            auto first = hub->AddOutput(), second = hub->AddOutput();
            const auto otherB = second->GetRebindings().At(f.B);
            auto predicate = MakeBinaryPredicate("And", MakeUnaryCallable("Exists", f.Column(f.A)),
                MakeUnaryCallable("Exists", f.Column(f.B)));
            auto filtered = MakeIntrusive<TOpFilter>(first, f.Pos, predicate);
            auto root = f.Root(MakeIntrusive<TOpJoin>(filtered, second, f.Pos, "Cross", TJoinIUs{}),
                {{f.B, "filtered"}, {otherB, "unfiltered"}});
            ReduceOuterJoins(*root, f.RboCtx);
            UNIT_ASSERT_VALUES_EQUAL(f.Join->JoinKind, kind);
        }
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

    Y_UNIT_TEST(SqlOuterJoinReductionsPreserveUnmatchedRows) {
        NKikimrConfig::TAppConfig config;
        auto* service = config.MutableTableServiceConfig();
        service->SetEnableNewRBO(true);
        service->SetEnableFallbackToYqlOptimizer(false);
        service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        service->SetDefaultCostBasedOptimizationLevel(0);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(config).SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schema = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/A` (id Int64 NOT NULL, k Int64, PRIMARY KEY(id));
            CREATE TABLE `/Root/B` (id Int64 NOT NULL, k Int64, PRIMARY KEY(id));
        )").GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        auto insert = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/A` (id,k) VALUES (1,10),(2,20),(3,NULL);
            UPSERT INTO `/Root/B` (id,k) VALUES (10,10),(11,10),(12,30),(13,NULL);
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        struct TCase {
            TString Kind;
            TString Condition;
            TString Predicate;
            TString Expected;
        };
        const TVector<TCase> cases{
            {"FULL", "a.k=b.k", "TRUE", "[[-1;12];[-1;13];[1;10];[1;11];[2;-1];[3;-1]]"},
            {"FULL", "a.k=b.k", "a.id IS NOT NULL", "[[1;10];[1;11];[2;-1];[3;-1]]"},
            {"FULL", "a.k=b.k", "b.id IS NOT NULL", "[[-1;12];[-1;13];[1;10];[1;11]]"},
            {"FULL", "a.k=b.k", "a.id IS NOT NULL AND b.id IS NOT NULL", "[[1;10];[1;11]]"},
            {"FULL", "a.k=b.k", "a.id IS NOT NULL OR b.id IS NOT NULL", "[[-1;12];[-1;13];[1;10];[1;11];[2;-1];[3;-1]]"},
            {"RIGHT", "a.k=b.k", "a.id IS NOT NULL", "[[1;10];[1;11]]"},
            {"RIGHT", "a.k=b.k", "b.id IS NOT NULL", "[[-1;12];[-1;13];[1;10];[1;11]]"},
            {"FULL", "a.k IS NOT DISTINCT FROM b.k", "a.id IS NOT NULL AND b.id IS NOT NULL", "[[1;10];[1;11];[3;13]]"},
        };
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& test : cases) {
            const TString query = TStringBuilder() << R"(
                PRAGMA YqlSelect = 'force';
                SELECT COALESCE(a.id, -1) AS aid, COALESCE(b.id, -1) AS bid
                FROM `/Root/A` AS a )" << test.Kind << " JOIN `/Root/B` AS b ON " << test.Condition
                << " WHERE " << test.Predicate << " ORDER BY aid, bid;";
            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(NYdb::FormatResultSetYson(result.GetResultSet(0)), test.Expected, query);
        }
    }
}

} // namespace NKikimr::NKqp
