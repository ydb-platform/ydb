#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/system/fs.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;


namespace {

NKikimrConfig::TAppConfig AppCfg() {
    NKikimrConfig::TAppConfig appCfg;

    auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
    rm->SetChannelBufferSize(100);
    rm->SetMinChannelBufferSize(100);
    rm->SetMkqlLightProgramMemoryLimit(100 << 20);
    rm->SetMkqlHeavyProgramMemoryLimit(100 << 20);

    appCfg.MutableTableServiceConfig()->SetEnableQueryServiceSpilling(true);

    auto* spilling = appCfg.MutableTableServiceConfig()->MutableSpillingServiceConfig()->MutableLocalFileConfig();
    spilling->SetEnable(true);
    spilling->SetRoot("./spilling/");

    return appCfg;
}

NKikimrConfig::TAppConfig AppCfgLowComputeLimits(double reasonableTreshold, bool enableSpilling=true) {
    NKikimrConfig::TAppConfig appCfg;

    auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
    rm->SetMkqlLightProgramMemoryLimit(100);
    rm->SetMkqlHeavyProgramMemoryLimit(300);
    rm->SetSpillingPercent(reasonableTreshold);
    appCfg.MutableTableServiceConfig()->SetEnableQueryServiceSpilling(true);

    auto* spilling = appCfg.MutableTableServiceConfig()->MutableSpillingServiceConfig()->MutableLocalFileConfig();

    spilling->SetEnable(enableSpilling);
    spilling->SetRoot("./spilling/");

    return appCfg;
}

void FillTableWithData(NQuery::TQueryClient& db, ui64 numRows=300) {
    for (ui32 i = 0; i < numRows; ++i) {
        auto result = db.ExecuteQuery(Sprintf(R"(
            --!syntax_v1
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (%d, "%s")
        )", i, TString(200000 + i, 'a' + (i % 26)).c_str()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

constexpr auto SimpleGraceJoinWithSpillingQuery = R"(
        --!syntax_v1
        PRAGMA ydb.EnableSpillingNodes="GraceJoin";
        PRAGMA ydb.CostBasedOptimizationLevel='0';
        PRAGMA ydb.HashJoinMode='graceandself';
        select t1.Key, t1.Value, t2.Key, t2.Value
        from `/Root/KeyValue` as t1 full join `/Root/KeyValue` as t2 on t1.Value = t2.Value
        order by t1.Value
    )";


} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpScanSpilling) {

Y_UNIT_TEST(SpillingPragmaParseError) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfg());

    auto db = kikimr.GetQueryClient();
    auto query = R"(
        --!syntax_v1
        PRAGMA ydb.EnableSpillingNodes="GraceJoin1";
        select t1.Key, t1.Value, t2.Key, t2.Value
        from `/Root/KeyValue` as t1 full join `/Root/KeyValue` as t2 on t1.Value = t2.Value
        order by t1.Value
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::GENERIC_ERROR, planres.GetIssues().ToString());
}

Y_UNIT_TEST_TWIN(SpillingInRuntimeNodes, EnabledSpilling) {
    double reasonableTreshold = EnabledSpilling ? 0.01 : 100;
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfgLowComputeLimits(reasonableTreshold));

    auto db = kikimr.GetQueryClient();

    FillTableWithData(db);

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(SimpleGraceJoinWithSpillingQuery, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());

    Cerr << planres.GetStats()->GetAst() << Endl;

    auto result = db.ExecuteQuery(SimpleGraceJoinWithSpillingQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    if (EnabledSpilling) {
        UNIT_ASSERT(counters.SpillingWriteBlobs->Val() > 0);
        UNIT_ASSERT(counters.SpillingReadBlobs->Val() > 0);
    } else {
        UNIT_ASSERT(counters.SpillingWriteBlobs->Val() == 0);
        UNIT_ASSERT(counters.SpillingReadBlobs->Val() == 0);
    }
}

Y_UNIT_TEST(HandleErrorsCorrectly) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfgLowComputeLimits(0.01, false));

    auto db = kikimr.GetQueryClient();

    FillTableWithData(db);

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(SimpleGraceJoinWithSpillingQuery, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());

    Cerr << planres.GetStats()->GetAst() << Endl;

    auto result = db.ExecuteQuery(SimpleGraceJoinWithSpillingQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
}

Y_UNIT_TEST(SelfJoinQueryService) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;

    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = 20;
    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1;

    Y_DEFER {
        NYql::NDq::GetDqExecutionSettingsForTests().Reset();
    };

    TKikimrRunner kikimr(AppCfg());

    auto db = kikimr.GetQueryClient();

    for (ui32 i = 0; i < 10; ++i) {
        auto result = db.ExecuteQuery(Sprintf(R"(
            --!syntax_v1
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (%d, "%s")
        )", i, TString(20, 'a' + i).c_str()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto query = R"(
        --!syntax_v1
        PRAGMA ydb.CostBasedOptimizationLevel='0';
        select t1.Key, t1.Value, t2.Key, t2.Value
        from `/Root/KeyValue` as t1 join `/Root/KeyValue` as t2 on t1.Value = t2.Value
        order by t1.Key
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());

    Cerr << planres.GetStats()->GetAst() << Endl;

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    CompareYson(R"([
        [[0u];["aaaaaaaaaaaaaaaaaaaa"];[0u];["aaaaaaaaaaaaaaaaaaaa"]];
        [[1u];["bbbbbbbbbbbbbbbbbbbb"];[1u];["bbbbbbbbbbbbbbbbbbbb"]];
        [[2u];["cccccccccccccccccccc"];[2u];["cccccccccccccccccccc"]];
        [[3u];["dddddddddddddddddddd"];[3u];["dddddddddddddddddddd"]];
        [[4u];["eeeeeeeeeeeeeeeeeeee"];[4u];["eeeeeeeeeeeeeeeeeeee"]];
        [[5u];["ffffffffffffffffffff"];[5u];["ffffffffffffffffffff"]];
        [[6u];["gggggggggggggggggggg"];[6u];["gggggggggggggggggggg"]];
        [[7u];["hhhhhhhhhhhhhhhhhhhh"];[7u];["hhhhhhhhhhhhhhhhhhhh"]];
        [[8u];["iiiiiiiiiiiiiiiiiiii"];[8u];["iiiiiiiiiiiiiiiiiiii"]];
        [[9u];["jjjjjjjjjjjjjjjjjjjj"];[9u];["jjjjjjjjjjjjjjjjjjjj"]]
    ])", FormatResultSetYson(result.GetResultSet(0)));

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    UNIT_ASSERT(counters.SpillingWriteBlobs->Val() > 0);
    UNIT_ASSERT(counters.SpillingReadBlobs->Val() > 0);
}

Y_UNIT_TEST(SelfJoin) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;

    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = 20;
    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1;

    Y_DEFER {
        NYql::NDq::GetDqExecutionSettingsForTests().Reset();
    };

    TKikimrRunner kikimr(AppCfg());
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);

    auto db = kikimr.GetTableClient();

    auto session = db.CreateSession().GetValueSync().GetSession();

    for (ui32 i = 0; i < 10; ++i) {
        auto result = session.ExecuteDataQuery(Sprintf(R"(
            --!syntax_v1
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (%d, "%s")
        )", i, TString(20, 'a' + i).c_str()), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto query = R"(
        --!syntax_v1
        PRAGMA ydb.CostBasedOptimizationLevel='0';
        select t1.Key, t1.Value, t2.Key, t2.Value
        from `/Root/KeyValue` as t1 join `/Root/KeyValue` as t2 on t1.Key = t2.Key
        order by t1.Key
    )";

    auto it = db.StreamExecuteScanQuery(query).GetValueSync();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    CompareYson(R"([
        [[0u];["aaaaaaaaaaaaaaaaaaaa"];[0u];["aaaaaaaaaaaaaaaaaaaa"]];
        [[1u];["bbbbbbbbbbbbbbbbbbbb"];[1u];["bbbbbbbbbbbbbbbbbbbb"]];
        [[2u];["cccccccccccccccccccc"];[2u];["cccccccccccccccccccc"]];
        [[3u];["dddddddddddddddddddd"];[3u];["dddddddddddddddddddd"]];
        [[4u];["eeeeeeeeeeeeeeeeeeee"];[4u];["eeeeeeeeeeeeeeeeeeee"]];
        [[5u];["ffffffffffffffffffff"];[5u];["ffffffffffffffffffff"]];
        [[6u];["gggggggggggggggggggg"];[6u];["gggggggggggggggggggg"]];
        [[7u];["hhhhhhhhhhhhhhhhhhhh"];[7u];["hhhhhhhhhhhhhhhhhhhh"]];
        [[8u];["iiiiiiiiiiiiiiiiiiii"];[8u];["iiiiiiiiiiiiiiiiiiii"]];
        [[9u];["jjjjjjjjjjjjjjjjjjjj"];[9u];["jjjjjjjjjjjjjjjjjjjj"]]
    ])", StreamResultToYson(it));

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    UNIT_ASSERT(counters.SpillingWriteBlobs->Val() > 0);
    UNIT_ASSERT(counters.SpillingReadBlobs->Val() > 0);
}

} // suite

} // namespace NKqp
} // namespace NKikimr
