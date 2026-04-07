#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/testlib/common/test_utils.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/tests/tools/fqrun/src/fq_setup.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;
using namespace NFqRun;
using namespace NKikimrRun;

namespace {

class TTestFuxture : public NUnitTest::TBaseFixture {
protected:
    using TBase = NUnitTest::TBaseFixture;

    static constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(15);

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        NTestUtils::SetupSignalHandlers();

        TFqSetupSettings settings;
        settings.VerbosityLevel = TFqSetupSettings::EVerbosity::InitLogs;
        settings.EnableYdbCompute = true;

        auto& logConfig = *settings.AppConfig.MutableLogConfig();
        logConfig.SetDefaultLevel(NLog::EPriority::PRI_WARN);
        ModifyLogPriorities({
            {NKikimrServices::EServiceKikimr::YQ_CONTROL_PLANE_STORAGE, NLog::EPriority::PRI_DEBUG},
            {NKikimrServices::EServiceKikimr::FQ_RUN_ACTOR, NLog::EPriority::PRI_TRACE}
        }, logConfig);

        auto& fqConfig = *settings.AppConfig.MutableFederatedQueryConfig();
        auto& cpStorageConfig = *fqConfig.MutableControlPlaneStorage();
        cpStorageConfig.SetEnabled(true);
        cpStorageConfig.SetUseInMemory(true);

        auto& privateApiConfig = *fqConfig.MutablePrivateApi();
        privateApiConfig.SetEnabled(true);
        privateApiConfig.SetLoopback(true);

        fqConfig.SetEnableDynamicNameservice(true);
        fqConfig.MutableControlPlaneProxy()->SetEnabled(true);
        fqConfig.MutableDbPool()->SetEnabled(true);
        fqConfig.MutableNodesManager()->SetEnabled(true);
        fqConfig.MutablePendingFetcher()->SetEnabled(true);
        fqConfig.MutablePrivateProxy()->SetEnabled(true);
        fqConfig.MutableGateways()->SetEnabled(true);
        fqConfig.MutableResourceManager()->SetEnabled(true);
        fqConfig.MutableCommon()->SetIdsPrefix("ut");

        auto& computeConfig = *fqConfig.MutableCompute();
        computeConfig.SetDefaultCompute(NFq::NConfig::IN_PLACE);

        auto& computeMapping = *computeConfig.AddComputeMapping();
        computeMapping.SetQueryType(FederatedQuery::QueryContent::ANALYTICS);
        computeMapping.SetCompute(NFq::NConfig::YDB);
        computeMapping.MutableActivation()->SetPercentage(100);

        FqSetup = std::make_unique<TFqSetup>(settings);
    }

    static void CheckSuccess(const TRequestResult& result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::SUCCESS, result.Issues.ToOneLineString());
    }

    TExecutionMeta WaitQueryExecution(const TString& queryId, TDuration timeout = WAIT_TIMEOUT) const {
        using EStatus = FederatedQuery::QueryMeta;

        const TInstant start = TInstant::Now();
        while (TInstant::Now() - start <= timeout) {
            TExecutionMeta meta;
            CheckSuccess(FqSetup->DescribeQuery(queryId, {}, meta));

            if (!IsIn({EStatus::FAILED, EStatus::COMPLETED, EStatus::ABORTED_BY_USER, EStatus::ABORTED_BY_SYSTEM}, meta.Status)) {
                Cerr << "Wait query execution " << TInstant::Now() - start << ": " << EStatus::ComputeStatus_Name(meta.Status) << "\n";
                Sleep(TDuration::Seconds(1));
                continue;
            }

            UNIT_ASSERT_C(meta.Status == EStatus::COMPLETED, "issues: " << meta.Issues.ToOneLineString() << ", transient issues: " << meta.TransientIssues.ToOneLineString());
            return meta;
        }

        UNIT_ASSERT_C(false, "Waiting query execution timeout. Spent time " << TInstant::Now() - start << " exceeds limit " << timeout);
        return {};
    }

public:
    std::unique_ptr<TFqSetup> FqSetup;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(InMemoryControlPlaneStorage) {
    void CheckQueryResults(const TTestFuxture& ctx, const TString& queryId) {
        UNIT_ASSERT(queryId);

        const auto meta = ctx.WaitQueryExecution(queryId);
        UNIT_ASSERT_VALUES_EQUAL(meta.ResultSetSizes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(meta.ResultSetSizes[0], 1);

        Ydb::ResultSet resultSet;
        ctx.CheckSuccess(ctx.FqSetup->FetchQueryResults(queryId, 0, {}, resultSet));

        NYdb::TResultSetParser parser(resultSet);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("result_value").GetInt32(), 42);
    }

    Y_UNIT_TEST_F(ExecuteSimpleStreamQuery, TTestFuxture) {
        TString queryId;
        CheckSuccess(FqSetup->QueryRequest({
            .Query = "SELECT 42 AS result_value",
            .Action = FederatedQuery::ExecuteMode::RUN,
            .Type = FederatedQuery::QueryContent::STREAMING
        }, queryId));

        CheckQueryResults(*this, queryId);
    }

    Y_UNIT_TEST_F(ExecuteSimpleAnalyticsQuery, TTestFuxture) {
        TString queryId;
        CheckSuccess(FqSetup->QueryRequest({
            .Query = "SELECT 42 AS result_value",
            .Action = FederatedQuery::ExecuteMode::RUN,
            .Type = FederatedQuery::QueryContent::ANALYTICS
        }, queryId));

        CheckQueryResults(*this, queryId);
    }
}

} // NFq
