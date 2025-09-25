#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/shutdown/state.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/shutdown/controller.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/base/counters.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpService) {

    Y_UNIT_TEST(CloseSessionsWithLoad) {
        auto kikimr = std::make_shared<TKikimrRunner>();
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NLog::PRI_DEBUG);
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NLog::PRI_DEBUG);

        auto db = kikimr->GetTableClient();

        const ui32 SessionsCount = 50;
        const TDuration WaitDuration = TDuration::Seconds(1);

        TVector<TSession> sessions;
        for (ui32 i = 0; i < SessionsCount; ++i) {
            auto sessionResult = db.CreateSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());

            sessions.push_back(sessionResult.GetSession());
        }

        NPar::LocalExecutor().RunAdditionalThreads(SessionsCount + 1);
        NPar::LocalExecutor().ExecRange([&kikimr, sessions, WaitDuration](int id) mutable {
            if (id == (i32)sessions.size()) {
                Sleep(WaitDuration);
                Cerr << "start sessions close....." << Endl;
                for (ui32 i = 0; i < sessions.size(); ++i) {
                    sessions[i].Close();
                }

                Cerr << "finished sessions close....." << Endl;
                auto counters = GetServiceCounters(kikimr->GetTestServer().GetRuntime()->GetAppData(0).Counters,  "ydb");

                ui64 pendingCompilations = 0;
                do {
                    Sleep(WaitDuration);
                    pendingCompilations = counters->GetNamedCounter("name", "table.query.compilation.active_count", false)->Val();
                    Cerr << "still compiling... " << pendingCompilations << Endl;
                } while (pendingCompilations != 0);

                ui64 pendingSessions = 0;
                do {
                    Sleep(WaitDuration);
                    pendingSessions = counters->GetNamedCounter("name", "table.session.active_count", false)->Val();
                    Cerr << "still active sessions ... " << pendingSessions << Endl;
                } while (pendingSessions != 0);

                Sleep(TDuration::Seconds(5));

                return;
            }

            auto session = sessions[id];
            std::optional<TTransaction> tx;

            while (true) {
                if (tx) {
                    auto result = tx->Commit().GetValueSync();
                    if (!result.IsSuccess()) {
                        return;
                    }

                    tx = {};
                    continue;
                }

                auto query = Sprintf(R"(
                    SELECT Key, Text, Data FROM `/Root/EightShard` WHERE Key=%1$d + 0;
                    SELECT Key, Data, Text FROM `/Root/EightShard` WHERE Key=%1$d + 1;
                    SELECT Text, Key, Data FROM `/Root/EightShard` WHERE Key=%1$d + 2;
                    SELECT Text, Data, Key FROM `/Root/EightShard` WHERE Key=%1$d + 3;
                    SELECT Data, Key, Text FROM `/Root/EightShard` WHERE Key=%1$d + 4;
                    SELECT Data, Text, Key FROM `/Root/EightShard` WHERE Key=%1$d + 5;

                    UPSERT INTO `/Root/EightShard` (Key, Text) VALUES
                        (%2$dul, "New");
                )", RandomNumber<ui32>(), RandomNumber<ui32>());

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx()).GetValueSync();
                if (!result.IsSuccess()) {
                    Sleep(TDuration::Seconds(5));
                    Cerr << "received non-success status for session " << id << Endl;
                    return;
                }

                tx = result.GetTransaction();
            }
        }, 0, SessionsCount + 1, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
        WaitForZeroReadIterators(kikimr->GetTestServer(), "/Root/EightShard");
    }
}
}
}