#include "ut_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

struct TLogStopwatch {
    TLogStopwatch(TString message)
        : Message(std::move(message))
        , Started(TAppData::TimeProvider->Now())
    {}
    
    ~TLogStopwatch() {
        Cerr << "[STOPWATCH] " << Message << " in " << (TAppData::TimeProvider->Now() - Started).MilliSeconds() << "ms" << Endl;
    }

private:
    TString Message;
    TInstant Started;
};

Y_UNIT_TEST_SUITE(SystemViewLarge) {

    Y_UNIT_TEST(AuthOwners) {
        TTestEnv env;
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetDispatchedEventsLimit(100'000'000'000);
        
        TTableClient client(env.GetDriver());

        const size_t pathsToCreate = 10'000 - 100;

        {
            TLogStopwatch stopwatch(TStringBuilder() << "Created " << pathsToCreate << " paths");

            THashSet<TString> paths;
            paths.emplace("Root");

            // creating a random directories tree:
            while (paths.size() < pathsToCreate) {
                TString path = "/Root";
                ui32 index = RandomNumber<ui32>();
                for (ui32 depth : xrange(15)) {
                    Y_UNUSED(depth);
                    TString dir = "Dir" + std::to_string(index % 3);
                    index /= 3;
                    if (paths.size() < pathsToCreate && paths.emplace(path + "/" + dir).second) {
                        env.GetClient().MkDir(path, dir);
                    }
                    path += "/" + dir;
                }
            }
        }

        Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root").DebugString() << Endl;

        {
            const size_t expectedCount = pathsToCreate + 4;

            TLogStopwatch stopwatch(TStringBuilder() << "Selected " << expectedCount << " rows from .sys/auth_owners");

            auto it = client.StreamExecuteScanQuery(R"(
                SELECT COUNT (*)
                FROM `Root/.sys/auth_owners`
            )").GetValueSync();

            auto expected = Sprintf(R"([
                [%du];
            ])", expectedCount);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }
}

} // NSysView
} // NKikimr
