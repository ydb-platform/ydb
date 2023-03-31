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

    auto* spilling = appCfg.MutableTableServiceConfig()->MutableSpillingServiceConfig()->MutableLocalFileConfig();
    spilling->SetEnable(true);
    spilling->SetRoot("./spilling/");

    return appCfg;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpScanSpilling) {

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

    auto in = [](int pattern, std::vector<int> vals) {
        for (auto&& val : vals) {
            if (pattern == val) {
                return true;
            }
        }
        return false;
    };

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    UNIT_ASSERT(in(counters.SpillingWriteBlobs->Val(), {3, 6, 14}));
    UNIT_ASSERT(in(counters.SpillingReadBlobs->Val(), {3, 6, 14}));
    UNIT_ASSERT(in(counters.SpillingStoredBlobs->Val(), {0, 14}));
}

} // suite

} // namespace NKqp
} // namespace NKikimr
