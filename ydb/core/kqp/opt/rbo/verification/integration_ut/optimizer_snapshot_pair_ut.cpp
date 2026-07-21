#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

#include <mutex>

namespace NKikimr::NKqp {
namespace {

constexpr const char* TestCluster = "local_ut";

class TRecordingSemanticSnapshotSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        std::lock_guard guard(Mutex);
        Results.push_back(std::move(result));
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Extract() {
        std::lock_guard guard(Mutex);
        return std::move(Results);
    }

private:
    std::mutex Mutex;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

TIntrusivePtr<IKqpGateway> MakeGateway(Tests::TServer& server) {
    auto counters = MakeIntrusive<TKqpRequestCounters>();
    counters->Counters = new TKqpCounters(server.GetRuntime()->GetAppData(0).Counters);
    counters->TxProxyMon = new NTxProxy::TTxProxyMon(server.GetRuntime()->GetAppData(0).Counters);
    auto loader = std::make_shared<TKqpTableMetadataLoader>(
        TestCluster,
        server.GetRuntime()->GetAnyNodeActorSystem(),
        TIntrusivePtr<NYql::TKikimrConfiguration>(),
        false);
    return CreateKikimrIcGateway(
        TestCluster,
        NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY,
        "/Root",
        "/Root",
        std::move(loader),
        server.GetRuntime()->GetAnyNodeActorSystem(),
        server.GetRuntime()->GetNodeId(0),
        counters,
        server.GetSettings().AppConfig->GetQueryServiceConfig());
}

NYql::TKikimrConfiguration::TPtr MakeConfiguration() {
    auto config = MakeIntrusive<NYql::TKikimrConfiguration>();
    const auto defaultsData = NResource::Find("kqp_default_settings.txt");
    TStringInput defaultsStream(defaultsData);
    NKikimrKqp::TKqpDefaultSettings defaults;
    UNIT_ASSERT(TryParseFromTextFormat(defaultsStream, defaults));
    config->Init(
        defaults.GetDefaultSettings(),
        TestCluster,
        TVector<NKikimrKqp::TKqpSetting>{},
        true);
    config->SetEnableNewRBO(true);
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->_ResultRowsLimit.Clear();
    return config;
}

TIntrusivePtr<IKqpHost> MakeHost(
    Tests::TServer& server,
    NYql::IModuleResolver::TPtr moduleResolver,
    std::shared_ptr<IRBOSemanticSnapshotSink> sink)
{
    return CreateKqpHost(
        MakeGateway(server),
        TestCluster,
        "/Root",
        MakeConfiguration(),
        std::move(moduleResolver),
        std::nullopt,
        nullptr,
        nullptr,
        server.GetSettings().AppConfig->GetQueryServiceConfig(),
        {},
        server.GetFunctionRegistry(),
        true,  // keepConfigChanges: preserve the cleared result-row cap.
        false, // isInternalCall
        nullptr,
        server.GetRuntime()->GetAnyNodeActorSystem(),
        nullptr,
        nullptr,
        false,
        std::move(sink));
}

NJson::TJsonValue ParseSnapshot(const TRBOSemanticSnapshotBoundaryResultV1& result) {
    UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    NJson::TJsonValue snapshot;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.Json, &snapshot, true), result.Json);
    return snapshot;
}

NJson::TJsonValue BuildVerificationProblem(
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final)
{
    TTempDir tempDir;
    const auto initialPath = tempDir.Path() / "initial.json";
    const auto finalPath = tempDir.Path() / "final.json";
    const auto formulaPath = tempDir.Path() / "problem.smt2";
    TFileOutput(initialPath.GetPath()).Write(initial.Json);
    TFileOutput(finalPath.GetPath()).Write(final.Json);

    TShellCommand command(BinaryPath(
        "ydb/core/kqp/opt/rbo/verification/bin/kqp_rbo_verify"));
    command
        << initialPath.GetPath()
        << finalPath.GetPath()
        << "--rows" << "2"
        << "--timeout-ms" << "10000"
        << "--emit-smt" << formulaPath.GetPath();

    const auto solver = TryGetEnv("RBO_Z3");
    if (solver) {
        command << "--solver" << *solver;
    }
    command.Run();
    UNIT_ASSERT_C(
        command.GetExitCode().Defined() && command.GetExitCode().GetRef() == 0,
        command.GetError() << command.GetOutput());
    UNIT_ASSERT(formulaPath.Exists());

    NJson::TJsonValue verdict;
    UNIT_ASSERT_C(
        NJson::ReadJsonTree(command.GetOutput(), &verdict, true),
        command.GetOutput());
    return verdict;
}

} // namespace

Y_UNIT_TEST_SUITE(TRBOSemanticSnapshotIntegration) {
    Y_UNIT_TEST(RealHostProducesInitialAndFinalSnapshots) {
        TKikimrRunner kikimr;

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Key FROM `/Root/KeyValue`;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);

        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);
        UNIT_ASSERT_VALUES_EQUAL(initial["format"].GetStringSafe(), "ydb-rbo-semantic-snapshot");
        UNIT_ASSERT_VALUES_EQUAL(final["format"].GetStringSafe(), "ydb-rbo-semantic-snapshot");
        UNIT_ASSERT_VALUES_EQUAL(initial["version"].GetIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(final["version"].GetIntegerSafe(), 1);
        UNIT_ASSERT(initial["stage_graph"].IsNull());
        UNIT_ASSERT(final["stage_graph"].IsMap());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }
}

} // namespace NKikimr::NKqp
