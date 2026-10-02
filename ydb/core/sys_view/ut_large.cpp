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

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetDiscoveryMode(EDiscoveryMode::Off)
                .SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TLogStopwatch stopwatch(TStringBuilder() << "Selected " << pathsToCreate << " rows from .sys/auth_owners");
            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT COUNT (*)
                FROM `/Root/.sys/auth_owners`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = Sprintf(R"([
                [%du];
            ])", pathsToCreate);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthOwners_DropSubtreeDuringScan) {
        NKqp::TKikimrSettings settings;
        settings.SetUseRealThreads(true).SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableAlterDatabase(true);
        // Return rows while the tree is still being scanned.
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetChannelChunkSizeLimit(1024);
        NKqp::TKikimrRunner kikimr(settings);
        auto& testClient = kikimr.GetTestClient();
        auto schemeClient = kikimr.GetSchemeClient();

        UNIT_ASSERT_VALUES_EQUAL(testClient.AlterSubdomain("/", R"(
            Name: "Root"
            SchemeLimits {
                MaxDepth: 128
                MaxPaths: 20000
            }
        )"), NMsgBusProxy::MSTATUS_OK);

        const TString parent = "/Root/AuthScan";
        const TString deletedPath = parent + "/A";
        const TString survivorPath = parent + "/ZSurvivor";
        testClient.TestMkDir("/Root", "AuthScan/A");

        constexpr ui32 branches = 100;
        constexpr ui32 depth = 100;
        // 100 branches of depth 100: scanning requires 10'000 separate scheme cache navigation requests.
        for (ui32 branch = 0; branch < branches; ++branch) {
            TString path = TStringBuilder() << "A/Dir" << branch;
            for (ui32 level = 1; level < depth; ++level) {
                path += "/Nested";
            }
            testClient.TestMkDir(parent, path);
        }
        testClient.TestMkDir(parent, "ZSurvivor");

        auto client = kikimr.GetTableClient();
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT Path FROM `/Root/.sys/auth_owners`
            WHERE Path >= '/Root/AuthScan' AND Path < '/Root/AuthScan0'
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        TVector<TResultSet> batches;
        while(true) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }
            if (!part.HasResultSet()) {
                continue;
            }

            if (batches.empty()) {
                // Remove the parent as soon as the first result batch arrives.
                UNIT_ASSERT_VALUES_EQUAL(testClient.ForceDeleteUnsafe(parent, "A"), NMsgBusProxy::MSTATUS_OK);
            }
            batches.emplace_back(part.ExtractResultSet());
        }

        UNIT_ASSERT(!batches.empty());
        ui32 scannedSubtreePaths = 0;
        bool seenSurvivor = false;
        for (const auto& batch : batches) {
            TResultSetParser parser(batch);
            while (parser.TryNextRow()) {
                const auto path = parser.ColumnParser("Path").GetOptionalUtf8().value();
                if (path == deletedPath || TStringBuf(path).StartsWith(deletedPath + "/")) {
                    ++scannedSubtreePaths;
                }
                seenSurvivor |= path == survivorPath;
            }
        }

        UNIT_ASSERT(seenSurvivor);
        const ui32 totalSubtreePaths = 1 + branches * depth;
        UNIT_ASSERT_C(scannedSubtreePaths < totalSubtreePaths,
            "Deletion must take effect before the whole subtree is scanned");
    }

}

} // NSysView
} // NKikimr
