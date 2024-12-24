#include "controller_impl.h"

#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(AssignTxId) {
    using namespace NTestHelpers;

    void CheckTxIdResult(const NKikimrReplication::TEvTxIdResult& result, const TMap<TRowVersion, ui64>& expected) {
        UNIT_ASSERT_VALUES_EQUAL(result.VersionTxIdsSize(), expected.size());

        int i = 0;
        for (const auto& [version, txId] : expected) {
            const auto& actual = result.GetVersionTxIds(i++);
            UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(actual.GetVersion()), version);
            if (txId) {
                UNIT_ASSERT_VALUES_EQUAL(actual.GetTxId(), txId);
            }
        }
    }

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().RegisterService(
            MakeReplicationServiceId(env.GetRuntime().GetNodeId(0)),
            env.GetRuntime().Register(CreateReplicationMockService(env.GetSender()))
        );

        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase())
        );

        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto result = session
            .ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://%s/?database=/Root",
                    CONSISTENCY_LEVEL = "GLOBAL",
                    COMMIT_INTERVAL = Interval("PT10S")
                );
            )", env.GetEndpoint().c_str()))
            .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto desc = env.GetDescription("/Root/replication");
        const auto& repl = desc.GetPathDescription().GetReplicationDescription();
        const auto tabletId = repl.GetControllerId();

        const auto& cfg = repl.GetConfig().GetConsistencySettings();
        UNIT_ASSERT(cfg.HasGlobal());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetGlobal().GetCommitIntervalMilliSeconds(), 10000);

        TVector<ui64> txIds;

        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(1, 0),
            }));

            const auto& record = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetController().GetTabletId(), tabletId);

            CheckTxIdResult(record, {
                {TRowVersion(10000, 0), 0},
            });

            UNIT_ASSERT_VALUES_EQUAL(Count(txIds, record.GetVersionTxIds(0).GetTxId()), 0);
            txIds.push_back(record.GetVersionTxIds(0).GetTxId());
        }
        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(9999, 0),
            }));

            CheckTxIdResult(ev->Get()->Record, {
                {TRowVersion(10000, 0), txIds.back()},
            });
        }
        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(9999, Max<ui64>()),
            }));

            CheckTxIdResult(ev->Get()->Record, {
                {TRowVersion(10000, 0), txIds.back()},
            });
        }
        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(10000, 0),
            }));

            CheckTxIdResult(ev->Get()->Record, {
                {TRowVersion(20000, 0), 0},
            });

            UNIT_ASSERT_VALUES_EQUAL(Count(txIds, ev->Get()->Record.GetVersionTxIds(0).GetTxId()), 0);
            txIds.push_back(ev->Get()->Record.GetVersionTxIds(0).GetTxId());
        }
        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(5000, 0),
            }));

            CheckTxIdResult(ev->Get()->Record, {
                {TRowVersion(10000, 0), txIds[0]},
            });
        }
        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(20000, 0),
                TRowVersion(30000, 0),
                TRowVersion(40000, 0),
            }));

            CheckTxIdResult(ev->Get()->Record, {
                {TRowVersion(30000, 0), 0},
                {TRowVersion(40000, 0), 0},
                {TRowVersion(50000, 0), 0},
            });

            for (const auto& v : ev->Get()->Record.GetVersionTxIds()) {
                UNIT_ASSERT_VALUES_EQUAL(Count(txIds, v.GetTxId()), 0);
                txIds.push_back(v.GetTxId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(txIds.size(), 5);
        {
            auto ev = env.Send<TEvService::TEvTxIdResult>(tabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
                TRowVersion(50000, 0),
            }));

            CheckTxIdResult(ev->Get()->Record, {
                {TRowVersion(60000, 0), txIds.back()},
            });
        }
    }
}

}
