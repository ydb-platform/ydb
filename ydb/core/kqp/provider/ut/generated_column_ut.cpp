#include <ydb/core/kqp/opt/kqp_opt_generated_columns.h>
#include <ydb/core/kqp/provider/generated_column.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway_ut_helpers.h>
#include <ydb/core/cms/console/configs_dispatcher.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <util/generic/scope.h>

namespace NKikimr::NKqp::NOpt {
namespace {

struct TPlanningFixture {
    NYql::TExprContext Ctx;
    NYql::TKikimrTableDescription Table;

    TPlanningFixture() {
        Table.Metadata = MakeIntrusive<NYql::TKikimrTableMetadata>();
        for (const TString name : {"k", "a", "b"}) {
            AddColumn(name);
        }
        Table.Metadata->KeyColumnNames = {"k"};
    }

    NYql::TKikimrColumnMetadata& AddColumn(const TString& name) {
        Table.Metadata->ColumnOrder.push_back(name);
        auto& column = Table.Metadata->Columns[name];
        column.Name = name;
        column.Type = "Int32";
        return column;
    }

    void Generated(const TString& name, bool stored, TVector<TString> dependencies) {
        auto& column = AddColumn(name);
        column.SetDefaultFromExpression();
        column.DefaultExpression = NYql::TDefaultExpressionColumnInfo{
            .Dependencies = std::move(dependencies),
            .Stored = stored,
        };
    }

    TGeneratedColumnEvaluationPlan Plan(const TVector<TString>& requested, const THashSet<TString>& recompute = {}) {
        UNIT_ASSERT(Table.Load(Ctx));
        auto plan = BuildGeneratedColumnEvaluationPlan(Table, requested, recompute, {}, Ctx);
        UNIT_ASSERT_C(plan, Ctx.IssueManager.GetIssues().ToString());
        return *plan;
    }
};

TVector<TString> Names(const TGeneratedColumnEvaluationPlan& plan) {
    TVector<TString> result;
    for (const auto* column : plan.EvaluationOrder) {
        result.push_back(column->Name);
    }
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(GeneratedColumnAdmission) {
    Y_UNIT_TEST(AcceptsBackportedScalarSyntax) {
        TVector<TString> dependencies = {"a"};
        TString error;
        UNIT_ASSERT_C(NYql::ValidateGeneratedExpressionDependencies(
            "Udf(String::AsciiToUpper, a AS Depends)(a)", "g", dependencies, error), error);
        UNIT_ASSERT_VALUES_EQUAL(dependencies, (TVector<TString>{"a"}));
    }

    Y_UNIT_TEST(AcceptsPersistedLegacyScalarSyntax) {
        TVector<TString> dependencies = {"a"};
        TString error;
        UNIT_ASSERT_C(NYql::ValidateGeneratedExpressionDependencies("a NOT NULL", "g", dependencies, error), error);
        UNIT_ASSERT_VALUES_EQUAL(dependencies, (TVector<TString>{"a"}));
    }
}

Y_UNIT_TEST_SUITE(GeneratedColumnOwnership) {

    Y_UNIT_TEST(DeferredCreateReleasesCompilationOwnersBeforeCompletion) {
        struct TTrackedSession final : NYql::TKikimrSessionContext {
            bool& Destroyed;
            TTrackedSession(bool& destroyed, const NMiniKQL::IFunctionRegistry* registry,
                TIntrusivePtr<NYql::TKikimrConfiguration> config)
                : NYql::TKikimrSessionContext(registry, std::move(config), CreateDefaultTimeProvider(),
                    CreateDeterministicRandomProvider(1), nullptr)
                , Destroyed(destroyed)
            {}
            ~TTrackedSession() override { Destroyed = true; }
        };
        struct TTrackedMetadata final : NYql::TKikimrTableMetadata {
            bool& Destroyed;
            explicit TTrackedMetadata(bool& destroyed)
                : NYql::TKikimrTableMetadata(NYql::NGatewayTest::TestCluster, "/Root/DeferredCreate")
                , Destroyed(destroyed)
            {}
            ~TTrackedMetadata() override { Destroyed = true; }
        };

        bool sessionDestroyed = false;
        bool metadataDestroyed = false;
        bool metadataReleasedAtPublication = false;
        bool queryReleasedAtPublication = false;
        TKikimrRunner runner(TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false));
        auto& runtime = *runner.GetTestServer().GetRuntime();
        auto gateway = NYql::NGatewayTest::GetIcGateway(runner.GetTestServer());
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        auto session = MakeIntrusive<TTrackedSession>(sessionDestroyed, registry.Get(),
            MakeIntrusive<NYql::TKikimrConfiguration>());
        session->SetCluster(NYql::NGatewayTest::TestCluster);
        session->SetDatabase("/Root");
        session->Query().Type = NYql::EKikimrQueryType::Query;
        session->Query().PrepareOnly = true;
        session->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto query = session->QueryPtr();
        auto metadata = MakeIntrusive<TTrackedMetadata>(metadataDestroyed);
        metadata->Columns["k"] = NYql::TKikimrColumnMetadata("k", 0, "Int32", true);
        metadata->ColumnOrder = {"k"};
        metadata->KeyColumnNames = {"k"};

        TAutoPtr<NActors::IEventHandle> profilesResponse;
        NActors::TActorId profilesRequester;
        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
            if (event->GetTypeRewrite() == NConsole::TEvConfigsDispatcher::TEvGetConfigRequest::EventType) {
                const auto& kinds = event->Get<NConsole::TEvConfigsDispatcher::TEvGetConfigRequest>()->ConfigItemKinds;
                if (kinds == TVector<ui32>{NKikimrConsole::TConfigItem::TableProfilesConfigItem}) {
                    profilesRequester = event->Sender;
                }
            }
            if (event->GetTypeRewrite() == NConsole::TEvConfigsDispatcher::TEvGetConfigResponse::EventType
                && event->Recipient == profilesRequester && !profilesResponse)
            {
                profilesResponse.Swap(event);
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            return NActors::TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(previousObserver); };

        auto proxy = CreateKqpGatewayProxy(gateway, session, runtime.GetAnyNodeActorSystem());
        auto future = proxy->CreateTable(metadata, true);
        runtime.WaitFor("delayed CREATE table profiles", [&] { return bool(profilesResponse); });
        UNIT_ASSERT(!future.HasValue());
        proxy.Reset();
        session.Reset();
        metadata.Reset();
        const bool releasedBeforeProfiles = sessionDestroyed;

        future.Subscribe([&](const auto&) {
            metadataReleasedAtPublication = metadataDestroyed;
            queryReleasedAtPublication = query->RefCount() == 1;
        });
        runtime.SetObserverFunc(previousObserver);
        runtime.Send(profilesResponse.Release());
        runtime.WaitFor("deferred CREATE completion", [&] { return future.HasValue(); });
        UNIT_ASSERT_C(future.GetValue().Success(), future.GetValue().Issues().ToString());
        UNIT_ASSERT_C(releasedBeforeProfiles, "profiles callback retained the compilation session");
        UNIT_ASSERT_C(metadataReleasedAtPublication, "CREATE published completion while retaining table metadata");
        UNIT_ASSERT_C(queryReleasedAtPublication, "CREATE published completion while retaining its prepared query context");
        UNIT_ASSERT_VALUES_EQUAL(query->PreparingQuery->GetPhysicalQuery().TransactionsSize(), 1);
    }
    Y_UNIT_TEST(CreateMetadataDoesNotRetainCompiledExpressions) {
        // Context outlives every owner even when the regression assertion fails.
        NYql::TExprContext ctx;
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        auto config = MakeIntrusive<NYql::TKikimrConfiguration>();
        config->SetEnableIndexStreamWrite(true);
        config->FeatureFlags.SetEnableGeneratedStored(true);
        auto session = MakeIntrusive<NYql::TKikimrSessionContext>(registry.Get(), config,
            CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
        session->SetCluster("db");
        session->SetDatabase("/Root");
        session->Query().Type = NYql::EKikimrQueryType::Query;
        session->Tables().GetOrAddTable("db", "/Root", "/Root/T").Metadata =
            MakeIntrusive<NYql::TKikimrTableMetadata>("db", "/Root/T");
        const auto pos = NYql::TPositionHandle{};
        auto atom = [&](TStringBuf value) { return ctx.NewAtom(pos, value); };
        auto list = [&](NYql::TExprNode::TListType items) { return ctx.NewList(pos, std::move(items)); };
        auto column = [&](TStringBuf name, TStringBuf expression = {}) {
            auto type = ctx.NewCallable(pos, "OptionalType", {ctx.NewCallable(pos, "DataType", {atom("Int32")})});
            type->SetTypeAnn(ctx.MakeType<NYql::TTypeExprType>(
                ctx.MakeType<NYql::TOptionalExprType>(ctx.MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int32))));
            NYql::TExprNode::TListType fields = {atom(name), type};
            if (expression) {
                fields.push_back(list({atom("constraints"), list({list({atom("generated"),
                    list({atom("unused"), atom(expression), atom("stored")})})})}));
            }
            return list(std::move(fields));
        };
        auto world = ctx.NewWorld(pos);
        world->SetTypeAnn(ctx.MakeType<NYql::TWorldExprType>());
        auto create = ctx.NewCallable(pos, "KiCreateTable!", {
            world, ctx.NewCallable(pos, "DataSink", {atom("kikimr"), atom("db")}), atom("/Root/T"),
            list({column("k"), column("a"), column("g2", "g"), column("g", "a")}), list({atom("k")}),
            list({}), list({}), list({}), list({}), list({}), list({}), atom("table"),
            atom("false"), atom("0"), atom("0"), list({})});
        NYql::TTypeAnnotationContext types;
        auto sink = NYql::CreateKiSinkTypeAnnotationTransformer(nullptr, session, types);
        NYql::TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL_C(sink->Transform(create, output, ctx), NYql::IGraphTransformer::TStatus::Ok,
            ctx.IssueManager.GetIssues().ToString());
        const auto metadata = session->Tables().GetTable("db", "/Root/T").Metadata;
        for (const auto& name : {"g", "g2"}) {
            const auto& expression = *metadata->Columns.at(name).DefaultExpression;
            UNIT_ASSERT(expression.Stored);
            UNIT_ASSERT_VALUES_EQUAL(expression.ExprText, name == TStringBuf("g") ? "a" : "g");
            UNIT_ASSERT_VALUES_EQUAL(expression.Dependencies,
                (TVector<TString>{name == TStringBuf("g") ? "a" : "g"}));
            UNIT_ASSERT_C(!expression.Expr, "CREATE metadata must survive asynchronous completion without retaining AST nodes");
        }
    }
}

Y_UNIT_TEST_SUITE(GeneratedColumnPlanning) {
    Y_UNIT_TEST(MissingCompiledExpressionReturnsFailure) {
        TPlanningFixture fixture;
        fixture.Generated("v", false, {"a"});
        auto plan = fixture.Plan({"v"});
        NYql::NNodes::TExprBase row(fixture.Ctx.NewArgument(NYql::TPositionHandle{}, "row"));
        UNIT_ASSERT(!BuildGeneratedColumnMembers(plan, {"v"}, row, fixture.Table, {}, fixture.Ctx));
        NYql::NNodes::TCoAtomList logical(fixture.Ctx.NewList(NYql::TPositionHandle{}, {
            fixture.Ctx.NewAtom(NYql::TPositionHandle{}, "v")}));
        UNIT_ASSERT(!BuildVirtualGeneratedColumnProjection(row, logical, fixture.Table, {}, fixture.Ctx));
        UNIT_ASSERT_STRING_CONTAINS(fixture.Ctx.IssueManager.GetIssues().ToString(), "has no compiled expression");
    }

    Y_UNIT_TEST(InvalidPlanDoesNotBuildRead) {
        TPlanningFixture fixture;
        fixture.Generated("v", false, {"s"});
        fixture.Generated("s", true, {"v"});
        NYql::NNodes::TCoAtomList logical(fixture.Ctx.NewList(NYql::TPositionHandle{}, {
            fixture.Ctx.NewAtom(NYql::TPositionHandle{}, "k")}));
        bool builtRead = false;
        auto result = BuildReadWithVirtualGeneratedColumns(logical, fixture.Table, {}, fixture.Ctx,
            [&](const NYql::NNodes::TCoAtomList&) -> NYql::NNodes::TExprBase {
                builtRead = true;
                return NYql::NNodes::TExprBase(fixture.Ctx.NewArgument(NYql::TPositionHandle{}, "unexpected_read"));
            });
        UNIT_ASSERT(!result);
        UNIT_ASSERT(!builtRead);
    }

    Y_UNIT_TEST(ReuseGraphUntilSchemaReload) {
        TPlanningFixture fixture;
        fixture.Generated("v", false, {"a"});
        UNIT_ASSERT(fixture.Table.Load(fixture.Ctx));
        auto initial = BuildGeneratedColumnEvaluationPlan(fixture.Table, {"v"}, {}, {}, fixture.Ctx);
        UNIT_ASSERT(initial);
        UNIT_ASSERT_VALUES_EQUAL(initial->PhysicalColumns, (TVector<TString>{"a"}));
        // Loading a replacement schema is the invalidation boundary; changing its
        // pending metadata must not replace the graph of an already loaded schema.
        fixture.Table.Metadata->Columns.at("v").DefaultExpression->Dependencies = {"b"};
        auto cached = BuildGeneratedColumnEvaluationPlan(fixture.Table, {"v"}, {}, {}, fixture.Ctx);
        UNIT_ASSERT(cached);
        UNIT_ASSERT_VALUES_EQUAL(cached->PhysicalColumns, (TVector<TString>{"a"}));
        UNIT_ASSERT(fixture.Table.Load(fixture.Ctx));
        auto reloaded = BuildGeneratedColumnEvaluationPlan(fixture.Table, {"v"}, {}, {}, fixture.Ctx);
        UNIT_ASSERT(reloaded);
        UNIT_ASSERT_VALUES_EQUAL(reloaded->PhysicalColumns, (TVector<TString>{"b"}));
    }

    Y_UNIT_TEST(RejectInconsistentGeneratedKind) {
        TPlanningFixture fixture;
        fixture.Table.Metadata->Columns.at("a").DefaultExpression = NYql::TDefaultExpressionColumnInfo{};
        UNIT_ASSERT(!BuildGeneratedColumnEvaluationPlan(fixture.Table, {"k"}, {}, {}, fixture.Ctx));
        UNIT_ASSERT_STRING_CONTAINS(fixture.Ctx.IssueManager.GetIssues().ToString(), "inconsistent generated metadata");
    }

    Y_UNIT_TEST(VirtualChainUsesPhysicalLeaves) {
        TPlanningFixture fixture;
        fixture.Generated("v2", false, {"v1"});
        fixture.Generated("v1", false, {"a"});
        const auto plan = fixture.Plan({"v2"});
        UNIT_ASSERT_VALUES_EQUAL(plan.PhysicalColumns, (TVector<TString>{"a"}));
        UNIT_ASSERT_VALUES_EQUAL(Names(plan), (TVector<TString>{"v1", "v2"}));
    }

    Y_UNIT_TEST(StoredPredecessorIsBoundary) {
        TPlanningFixture fixture;
        fixture.Generated("v", false, {"a"});
        fixture.Generated("s", true, {"v"});
        fixture.Generated("out", false, {"s", "b"});
        const auto plan = fixture.Plan({"out"});
        UNIT_ASSERT_VALUES_EQUAL(plan.PhysicalColumns, (TVector<TString>{"b", "s"}));
        UNIT_ASSERT_VALUES_EQUAL(Names(plan), (TVector<TString>{"out"}));
    }

    Y_UNIT_TEST(SelectedStoredRecomputationOpensBoundary) {
        TPlanningFixture fixture;
        fixture.Generated("v", false, {"a"});
        fixture.Generated("s", true, {"v"});
        fixture.Generated("out", false, {"s"});
        const auto plan = fixture.Plan({"out"}, {"s"});
        UNIT_ASSERT_VALUES_EQUAL(plan.PhysicalColumns, (TVector<TString>{"a"}));
        UNIT_ASSERT_VALUES_EQUAL(Names(plan), (TVector<TString>{"v", "s", "out"}));
    }

    Y_UNIT_TEST(UnselectedStoredPredecessorStaysPhysical) {
        TPlanningFixture fixture;
        fixture.Generated("s1", true, {"a"});
        fixture.Generated("v", false, {"s1", "b"});
        fixture.Generated("s2", true, {"v"});
        const auto plan = fixture.Plan({"s2"}, {"s2"});
        UNIT_ASSERT_VALUES_EQUAL(plan.PhysicalColumns, (TVector<TString>{"b", "s1"}));
        UNIT_ASSERT_VALUES_EQUAL(Names(plan), (TVector<TString>{"v", "s2"}));
    }

    Y_UNIT_TEST(SharedPrerequisitesAppearOnce) {
        TPlanningFixture fixture;
        fixture.Generated("root", false, {"a"});
        fixture.Generated("lhs", false, {"root"});
        fixture.Generated("rhs", false, {"root"});
        fixture.Generated("leaf", false, {"lhs", "rhs"});
        const auto plan = fixture.Plan({"leaf", "rhs", "root"});
        UNIT_ASSERT_VALUES_EQUAL(plan.PhysicalColumns, (TVector<TString>{"a"}));
        THashMap<TString, size_t> positions;
        for (size_t i = 0; i < plan.EvaluationOrder.size(); ++i) {
            UNIT_ASSERT(positions.emplace(plan.EvaluationOrder[i]->Name, i).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(positions.size(), 4);
        UNIT_ASSERT(positions.at("root") < positions.at("lhs"));
        UNIT_ASSERT(positions.at("root") < positions.at("rhs"));
        UNIT_ASSERT(positions.at("lhs") < positions.at("leaf"));
        UNIT_ASSERT(positions.at("rhs") < positions.at("leaf"));
    }

    Y_UNIT_TEST(InvalidGraphsProduceIssues) {
        TPlanningFixture cycle;
        cycle.Generated("s", true, {"v"});
        cycle.Generated("v", false, {"s"});
        UNIT_ASSERT(!BuildGeneratedColumnEvaluationPlan(cycle.Table, {"k"}, {}, {}, cycle.Ctx));
        UNIT_ASSERT_STRING_CONTAINS(cycle.Ctx.IssueManager.GetIssues().ToString(), "Generated column dependency cycle:");

        TPlanningFixture unknown;
        unknown.Generated("v", false, {"missing"});
        UNIT_ASSERT(!BuildGeneratedColumnEvaluationPlan(unknown.Table, {"k"}, {}, {}, unknown.Ctx));
        UNIT_ASSERT_STRING_CONTAINS(unknown.Ctx.IssueManager.GetIssues().ToString(), "unknown column");
    }
}

} // namespace NKikimr::NKqp::NOpt
