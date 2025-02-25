#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
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

        SetupSignalActions();
        EnableYDBBacktraceFormat();

        TFqSetupSettings settings;
        settings.VerboseLevel = TFqSetupSettings::EVerbose::Max;

        settings.LogConfig.SetDefaultLevel(NLog::EPriority::PRI_WARN);
        ModifyLogPriorities({{NKikimrServices::EServiceKikimr::YQ_CONTROL_PLANE_STORAGE, NLog::EPriority::PRI_DEBUG}}, settings.LogConfig);

        auto& cpStorageConfig = *settings.FqConfig.MutableControlPlaneStorage();
        cpStorageConfig.SetEnabled(true);
        cpStorageConfig.SetUseInMemory(true);

        auto& privateApiConfig = *settings.FqConfig.MutablePrivateApi();
        privateApiConfig.SetEnabled(true);
        privateApiConfig.SetLoopback(true);

        settings.FqConfig.SetEnableDynamicNameservice(true);
        settings.FqConfig.MutableControlPlaneProxy()->SetEnabled(true);
        settings.FqConfig.MutableDbPool()->SetEnabled(true);
        settings.FqConfig.MutableNodesManager()->SetEnabled(true);
        settings.FqConfig.MutablePendingFetcher()->SetEnabled(true);
        settings.FqConfig.MutablePrivateProxy()->SetEnabled(true);
        settings.FqConfig.MutableGateways()->SetEnabled(true);
        settings.FqConfig.MutableResourceManager()->SetEnabled(true);
        settings.FqConfig.MutableCommon()->SetIdsPrefix("ut");

        FqSetup = std::make_unique<TFqSetup>(settings);
    }

protected:
    static void CheckSuccess(const TRequestResult& result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::SUCCESS, result.Issues.ToOneLineString());
    }

    TExecutionMeta WaitQueryExecution(const TString& queryId, TDuration timeout = WAIT_TIMEOUT) const {
        const TInstant start = TInstant::Now();
        while (TInstant::Now() - start <= timeout) {
            TExecutionMeta meta;
            CheckSuccess(FqSetup->DescribeQuery(queryId, meta));

            if (!IsFinalStatus(meta.Status)) {
                Cerr << "Wait query execution " << TInstant::Now() - start << ": " << FederatedQuery::QueryMeta::ComputeStatus_Name(meta.Status) << "\n";
                Sleep(TDuration::Seconds(1));
                continue;
            }

            UNIT_ASSERT_C(meta.Status == FederatedQuery::QueryMeta::COMPLETED, "issues: " << meta.Issues.ToOneLineString() << ", transient issues: " << meta.TransientIssues.ToOneLineString());
            return meta;
        }

        UNIT_ASSERT_C(false, "Waiting query execution timeout. Spent time " << TInstant::Now() - start << " exceeds limit " << timeout);
        return {};
    }

protected:
    std::unique_ptr<TFqSetup> FqSetup;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(InMemoryControlPlaneStorage) {
    Y_UNIT_TEST_F(ExecuteSimpleQuery, TTestFuxture) {
        TString queryId;
        CheckSuccess(FqSetup->StreamRequest({
            .Query = "SELECT 42 AS result_value"
        }, queryId));
        UNIT_ASSERT(queryId);

        const auto meta = WaitQueryExecution(queryId);
        UNIT_ASSERT_VALUES_EQUAL(meta.ResultSetSizes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(meta.ResultSetSizes[0], 1);

        Ydb::ResultSet resultSet;
        CheckSuccess(FqSetup->FetchQueryResults(queryId, 0, resultSet));

        NYdb::TResultSetParser parser(resultSet);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("result_value").GetInt32(), 42);
    }
}

} // NFq
