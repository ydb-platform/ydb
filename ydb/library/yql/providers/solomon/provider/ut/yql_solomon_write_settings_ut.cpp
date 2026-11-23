#include <ydb/library/yql/providers/solomon/common/constants.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_dq_integration.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider_impl.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <yql/essentials/core/project_settings/yql_project_settings.h>

namespace NYql {
namespace {

struct TWriteSettingsFixture {
    TExprContext Ctx;
    const TSolomonState::TPtr State = MakeIntrusive<TSolomonState>();

    TWriteSettingsFixture() {
        auto& config = State->Configuration->ClusterConfigs["test"];
        config.SetCluster("localhost:1");
        config.SetClusterType(TSolomonClusterConfig::SCT_SOLOMON);
        State->Configuration->SetValidClusters(TVector<TString>{ "test" });
        State->Configuration->FreezeDefaults();
    }

    bool Set(const TString& name, const TMaybe<TString>& value, TString& error) {
        return State->Configuration->Dispatch(
            NCommon::ALL_CLUSTERS, name, value, NCommon::ISettingDispatcher::EStage::STATIC, [&](const TString& message, bool isError) {
                if (isError) {
                    error = message;
                }
                return !isError;
            });
    }

    void Set(const TString& name, const TString& value) {
        TString error;
        UNIT_ASSERT_C(Set(name, MakeMaybe(value), error), error);
    }

    const TStructExprType* RowType(ui32 sensors) {
        TVector<const TItemExprType*> items;
        items.push_back(Ctx.MakeType<TItemExprType>("timestamp", Ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Timestamp)));
        for (ui32 i = 0; i < sensors; ++i) {
            items.push_back(
                Ctx.MakeType<TItemExprType>(TStringBuilder() << "sensor" << i, Ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)));
        }
        Sort(items.begin(), items.end(), TStructExprType::TItemLess{});
        return Ctx.MakeType<TStructExprType>(items);
    }

    NSo::NProto::TDqSolomonShard SinkSettings(ui32 sensors = 1) {
        const TPositionHandle pos;
        auto rowType = Ctx.NewCallable(pos, "StructType", {});
        rowType->SetTypeAnn(Ctx.MakeType<TTypeExprType>(RowType(sensors)));
        auto shard = Ctx.NewCallable(pos, "SoShard",
            { Ctx.NewAtom(pos, "test"), Ctx.NewAtom(pos, "project"), Ctx.NewAtom(pos, "cluster"), Ctx.NewAtom(pos, "service"), rowType });
        auto dataSink = Ctx.NewCallable(pos, "DataSink", { Ctx.NewAtom(pos, "solomon"), Ctx.NewAtom(pos, "test") });
        auto sink = Ctx.NewCallable(pos, "DqSink", { Ctx.NewAtom(pos, "0"), dataSink, shard });
        google::protobuf::Any settings;
        TString sinkType;
        auto integration = CreateSolomonDqIntegration(State);
        integration->FillSinkSettings(*sink, settings, sinkType);
        UNIT_ASSERT_VALUES_EQUAL(sinkType, "SolomonSink");
        NSo::NProto::TDqSolomonShard result;
        UNIT_ASSERT(settings.UnpackTo(&result));
        return result;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TSolomonWriteSettingsTest) {
    Y_UNIT_TEST(DefaultLimitsReachSinkSettings) {
        TWriteSettingsFixture fixture;
        const auto settings = fixture.SinkSettings();
        UNIT_ASSERT(settings.HasMaxRequestsInflight());
        UNIT_ASSERT(settings.HasMaxPointsPerRequest());
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxRequestsInflight(), NSo::NConstants::DefaultWriteMaxRequestsInflight);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxPointsPerRequest(), NSo::NConstants::DefaultWriteMaxPointsPerRequest);
    }

    Y_UNIT_TEST(WritePragmasReachSinkSettings) {
        TWriteSettingsFixture fixture;
        fixture.Set("WriteMaxRequestsInflight", "8");
        fixture.Set("WriteMaxPointsPerRequest", "2500");
        const auto settings = fixture.SinkSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxRequestsInflight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxPointsPerRequest(), 2500);
    }

    Y_UNIT_TEST(ProjectPragmasReachSinkSettings) {
        TWriteSettingsFixture fixture;
        TProjectSettings pragmas;
        pragmas.AddPragma("solomon.WriteMaxRequestsInflight", "2");
        pragmas.AddPragma("solomon.WriteMaxPointsPerRequest", "7");
        fixture.State->Configuration->ApplyProjectSettings(pragmas);
        const auto settings = fixture.SinkSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxRequestsInflight(), 2);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxPointsPerRequest(), 7);
    }

    Y_UNIT_TEST(WritePragmasCanBeReset) {
        TWriteSettingsFixture fixture;
        fixture.Set("WriteMaxRequestsInflight", "1");
        fixture.Set("WriteMaxPointsPerRequest", "2");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SinkSettings().GetMaxPointsPerRequest(), 2);
        TString error;
        UNIT_ASSERT_C(fixture.Set("WriteMaxRequestsInflight", Nothing(), error), error);
        UNIT_ASSERT_C(fixture.Set("WriteMaxPointsPerRequest", Nothing(), error), error);
        const auto settings = fixture.SinkSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxRequestsInflight(), NSo::NConstants::DefaultWriteMaxRequestsInflight);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxPointsPerRequest(), NSo::NConstants::DefaultWriteMaxPointsPerRequest);
    }

    Y_UNIT_TEST(InvalidWritePragmasAreRejected) {
        TWriteSettingsFixture fixture;
        for (const TString& name : { TString("WriteMaxRequestsInflight"), TString("WriteMaxPointsPerRequest") }) {
            for (const TString& value : { TString("0"), TString("-1"), TString("invalid") }) {
                TString error;
                UNIT_ASSERT(!fixture.Set(name, MakeMaybe(value), error));
                UNIT_ASSERT_STRING_CONTAINS(error, name);
            }
        }
        TString error;
        UNIT_ASSERT(!fixture.Set("WriteMaxPointsPerRequest", MakeMaybe(TString("10001")), error));
        UNIT_ASSERT_STRING_CONTAINS(error, "WriteMaxPointsPerRequest");
    }

    Y_UNIT_TEST(PointLimitMustFitAllSensorsInRow) {
        TWriteSettingsFixture fixture;
        fixture.Set("WriteMaxPointsPerRequest", "2");
        UNIT_ASSERT_EXCEPTION_CONTAINS(fixture.SinkSettings(3), yexception, "number of sensors");
        const auto settings = fixture.SinkSettings(2);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetScheme().SensorsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetMaxPointsPerRequest(), 2);
    }

    Y_UNIT_TEST(TypeAnnotationRejectsPointLimitSmallerThanRow) {
        TWriteSettingsFixture fixture;
        fixture.Set("WriteMaxPointsPerRequest", "2");
        const TPositionHandle pos;
        auto world = fixture.Ctx.NewWorld(pos);
        world->SetTypeAnn(fixture.Ctx.MakeType<TWorldExprType>());
        auto rows = fixture.Ctx.NewCallable(pos, "InputRows", {});
        rows->SetTypeAnn(fixture.Ctx.MakeType<TListExprType>(fixture.RowType(3)));
        auto dataSink = fixture.Ctx.NewCallable(pos, "DataSink", { fixture.Ctx.NewAtom(pos, "solomon"), fixture.Ctx.NewAtom(pos, "test") });
        dataSink->SetTypeAnn(fixture.Ctx.MakeType<TUnitExprType>());
        auto write =
            fixture.Ctx.NewCallable(pos, "SoWriteToShard", { world, dataSink, fixture.Ctx.NewAtom(pos, "project/cluster/service"), rows });
        TExprNode::TPtr output;
        auto transformer = CreateSolomonDataSinkTypeAnnotationTransformer(fixture.State);
        const auto status = transformer->Transform(write, output, fixture.Ctx);
        UNIT_ASSERT(status.Level == IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(fixture.Ctx.IssueManager.GetIssues().ToString(), "WriteMaxPointsPerRequest");
    }
}

}   // namespace NYql
