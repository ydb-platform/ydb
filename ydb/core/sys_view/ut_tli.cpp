// Transaction Lock Invalidation (TLI) tests

#include "ut_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>

#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

// Helper class for TLI (Transaction Lock Invalidation) tests
class TTliTestHelper {
public:
    TTliTestHelper()
        : Settings_([]() {
            TTestEnvSettings s;
            s.EnableSVP = true;
            s.TableServiceConfig.SetEnableOltpSink(true);
            return s;
        }())
        , Env_(1, 2, Settings_)
        , DriverConfig_(TDriverConfig()
            .SetEndpoint(Env_.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1"))
        , Driver_(DriverConfig_)
        , Client_(Driver_)
    {
        CreateTenant(Env_, "Tenant1", true);
        Session_.emplace(Client_.CreateSession().GetValueSync().GetSession());
        VictimSession_.emplace(Client_.CreateSession().GetValueSync().GetSession());
    }

    void CreateTable(const TString& tableName) {
        NKqp::AssertSuccessResult(Session_->ExecuteSchemeQuery(Sprintf(R"(
            CREATE TABLE `/Root/Tenant1/%s` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )", tableName.c_str())).GetValueSync());
    }

    void CreateTables(const TVector<TString>& tableNames) {
        TStringBuilder query;
        for (const auto& name : tableNames) {
            query << Sprintf(R"(
                CREATE TABLE `/Root/Tenant1/%s` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )", name.c_str());
        }
        NKqp::AssertSuccessResult(Session_->ExecuteSchemeQuery(query).GetValueSync());
    }

    void InsertData(const TString& query) {
        NKqp::AssertSuccessResult(Session_->ExecuteDataQuery(
            query, TTxControl::BeginTx().CommitTx()
        ).GetValueSync());
    }

    // Start victim transaction with a read (establishes snapshot and locks)
    TTransaction VictimBeginRead(const TString& selectQuery) {
        std::optional<TTransaction> victimTx;
        while (!victimTx) {
            auto result = VictimSession_->ExecuteDataQuery(
                selectQuery, TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

            TString yson = FormatResultSetYson(result.GetResultSet(0));
            if (yson == "[]") {
                continue;
            }

            victimTx = result.GetTransaction();
            UNIT_ASSERT(victimTx);
        }
        return *victimTx;
    }

    // Execute breaker transaction (commits immediately)
    void BreakerWrite(const TString& query) {
        NKqp::AssertSuccessResult(Session_->ExecuteDataQuery(
            query, TTxControl::BeginTx().CommitTx()
        ).GetValueSync());
    }

    // Victim commits with a write
    EStatus VictimCommitWrite(TTransaction& tx, const TString& query) {
        auto result = VictimSession_->ExecuteDataQuery(
            query, TTxControl::Tx(tx).CommitTx()
        ).ExtractValueSync();
        return result.GetStatus();
    }

    // Victim reads within transaction (no commit)
    EStatus VictimRead(TTransaction& tx, const TString& query) {
        auto result = VictimSession_->ExecuteDataQuery(
            query, TTxControl::Tx(tx)
        ).ExtractValueSync();
        return result.GetStatus();
    }

    // Victim writes within transaction (no commit)
    TTransaction VictimWrite(TTransaction& tx, const TString& query) {
        auto result = VictimSession_->ExecuteDataQuery(
            query, TTxControl::Tx(tx)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        return *result.GetTransaction();
    }

    struct TLockStats {
        ui64 BreakerCount = 0;
        ui64 VictimCount = 0;
        bool FoundBreaker = false;
        bool FoundVictim = false;
    };

    TLockStats WaitForLockStats(
        const TString& breakerMarker,
        const TString& victimMarker,
        const TString& testName = "")
    {
        TLockStats stats;

        for (size_t iter = 0; iter < 10 && (!stats.FoundBreaker || !stats.FoundVictim); ++iter) {
            auto it = Client_.StreamExecuteScanQuery(Sprintf(R"(
                SELECT QueryText, LocksBrokenAsBreaker, LocksBrokenAsVictim
                FROM `/Root/Tenant1/.sys/query_metrics_one_minute`
                WHERE QueryText LIKE '%%%s%%' OR QueryText LIKE '%%%s%%';
            )", breakerMarker.c_str(), victimMarker.c_str())).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString ysonString = NKqp::StreamResultToYson(it);
            Cerr << "Query metrics result" << (!testName.empty() ? " (" + testName + ")" : "") << ": " << ysonString << Endl;

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());

            for (const auto& row : node.AsList()) {
                if (!row.IsList() || row.AsList().size() < 3) continue;

                TString queryText = GetStringValue(row.AsList()[0]);
                ui64 breaker = GetUint64Value(row.AsList()[1]);
                ui64 victim = GetUint64Value(row.AsList()[2]);

                if (queryText.Contains(breakerMarker) && !queryText.Contains("query_metrics")) {
                    if (breaker > 0 && breaker > stats.BreakerCount) {
                        stats.BreakerCount = breaker;
                        stats.FoundBreaker = true;
                    }
                }
                // For victim, look for any query containing the marker with victim > 0
                // The victim might be detected in different queries depending on when lock break is detected
                if (queryText.Contains(victimMarker) && !queryText.Contains("query_metrics")) {
                    if (victim > 0 && victim > stats.VictimCount) {
                        stats.VictimCount = victim;
                        stats.FoundVictim = true;
                    }
                }
            }

            if (!stats.FoundBreaker || !stats.FoundVictim) {
                Sleep(TDuration::Seconds(5));
            }
        }

        return stats;
    }

private:
    static TString GetStringValue(const NYT::TNode& n) {
        if (n.IsList() && !n.AsList().empty()) {
            const auto& val = n.AsList()[0];
            if (val.IsString()) {
                return val.AsString();
            }
        }
        if (n.IsString()) {
            return n.AsString();
        }
        return "";
    }

    static ui64 GetUint64Value(const NYT::TNode& n) {
        if (n.IsList() && !n.AsList().empty()) {
            const auto& val = n.AsList()[0];
            if (val.IsUint64()) {
                return val.AsUint64();
            } else if (val.IsInt64()) {
                return static_cast<ui64>(val.AsInt64());
            }
            return 0;
        }
        if (n.IsUint64()) {
            return n.AsUint64();
        } else if (n.IsInt64()) {
            return static_cast<ui64>(n.AsInt64());
        }
        return 0;
    }

    TTestEnvSettings Settings_;
    TTestEnv Env_;
    TDriverConfig DriverConfig_;
    TDriver Driver_;
    TTableClient Client_;
    std::optional<TSession> Session_;
    std::optional<TSession> VictimSession_;
};

} // namespace

Y_UNIT_TEST_SUITE(TransactionLockInvalidation) {

    // Basic lock breakage: victim reads key, breaker writes same key, victim tries to write
    Y_UNIT_TEST(LocksBrokenSameKey) {
        TTliTestHelper h;

        h.CreateTable("Table");
        h.InsertData("UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"Initial\")");

        // Victim reads key 1
        auto victimTx = h.VictimBeginRead(
            "SELECT * FROM `/Root/Tenant1/Table` WHERE Key = 1u /* victim-read */");

        // Breaker writes to key 1 (breaks victim's lock)
        h.BreakerWrite(
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"Breaker\") /* breaker-write */");

        // Victim tries to commit with a write -> should be aborted
        auto status = h.VictimCommitWrite(victimTx,
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"Victim\") /* victim-commit */");
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        auto stats = h.WaitForLockStats("breaker-write", "victim-read", "same-key");
        UNIT_ASSERT_C(stats.FoundBreaker, "Breaker not found in metrics");
        UNIT_ASSERT_C(stats.FoundVictim, "Victim not found in metrics");
        UNIT_ASSERT_VALUES_EQUAL(stats.BreakerCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(stats.VictimCount, 1u);
    }

    // Victim reads key1, breaker writes key1, victim writes key2 -> victim aborted
    Y_UNIT_TEST(LocksBrokenDifferentKeys) {
        TTliTestHelper h;

        h.CreateTable("Table");
        h.InsertData("UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"V1\"), (2u, \"V2\")");

        // Victim reads key1
        auto victimTx = h.VictimBeginRead(
            "SELECT * FROM `/Root/Tenant1/Table` WHERE Key = 1u /* victim-r1 */");

        // Breaker writes key1 (breaks victim's lock)
        h.BreakerWrite(
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"Breaker\") /* breaker-w1 */");

        // Victim tries to commit with a write key2 -> should be aborted
        auto status = h.VictimCommitWrite(victimTx,
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (2u, \"VictimWrite\") /* victim-w2 */");
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        auto stats = h.WaitForLockStats("breaker-w1", "victim-r1", "diff-keys");
        UNIT_ASSERT_C(stats.FoundBreaker, "Breaker not found");
        UNIT_ASSERT_C(stats.FoundVictim, "Victim not found");
        UNIT_ASSERT_VALUES_EQUAL(stats.BreakerCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(stats.VictimCount, 1u);
    }

    // Victim reads multiple keys, breaker writes them all
    Y_UNIT_TEST(LocksBrokenMultipleKeys) {
        TTliTestHelper h;

        h.CreateTable("Table");
        h.InsertData("UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"V1\"), (2u, \"V2\"), (3u, \"V3\")");

        // Victim reads multiple keys
        auto victimTx = h.VictimBeginRead(
            "SELECT * FROM `/Root/Tenant1/Table` WHERE Key IN (1u, 2u, 3u) /* victim-rmulti */");

        // Breaker writes multiple keys (breaks victim's locks)
        h.BreakerWrite(
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"B1\"), (2u, \"B2\"), (3u, \"B3\") /* breaker-wmulti */");

        // Victim tries to commit with a write to key1 -> should be aborted
        auto status = h.VictimCommitWrite(victimTx,
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"Victim\") /* victim-wmulti */");
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        auto stats = h.WaitForLockStats("breaker-wmulti", "victim-rmulti", "multi");
        UNIT_ASSERT_C(stats.FoundBreaker, "Breaker not found");
        UNIT_ASSERT_C(stats.FoundVictim, "Victim not found");
        UNIT_ASSERT_GE(stats.BreakerCount, 1u);
        UNIT_ASSERT_GE(stats.VictimCount, 1u);
    }

    // Cross-table: victim reads TableA, breaker writes TableA, victim writes TableB
    Y_UNIT_TEST(LocksBrokenCrossTables) {
        TTliTestHelper h;

        h.CreateTables({"TableA", "TableB"});
        h.InsertData("UPSERT INTO `/Root/Tenant1/TableA` (Key, Value) VALUES (1u, \"ValA\")");

        // Victim reads key 1 from TableA
        auto victimTx = h.VictimBeginRead(
            "SELECT * FROM `/Root/Tenant1/TableA` WHERE Key = 1u /* victim-r */");

        // Breaker writes key 1 to TableA (breaks victim's lock)
        h.BreakerWrite(
            "UPSERT INTO `/Root/Tenant1/TableA` (Key, Value) VALUES (1u, \"Breaker\") /* breaker-w */");

        // Victim tries to commit with a write to TableB -> should be aborted
        auto status = h.VictimCommitWrite(victimTx,
            "UPSERT INTO `/Root/Tenant1/TableB` (Key, Value) VALUES (1u, \"DstVal\") /* victim-w */");
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        auto stats = h.WaitForLockStats("breaker-w", "victim-r", "cross");
        UNIT_ASSERT_C(stats.FoundBreaker, "Breaker not found");
        UNIT_ASSERT_C(stats.FoundVictim, "Victim not found");
        UNIT_ASSERT_VALUES_EQUAL(stats.BreakerCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(stats.VictimCount, 1u);
    }

    // InvisibleRowSkips scenario:
    // Victim reads at snapshot V1, breaker commits at V2, victim reads again at V1.
    // The second read encounters the V2 row as "invisible" at V1 snapshot.
    // When reading at V1, row versions > V1 are skipped (InvisibleRowSkips).
    // This triggers lock invalidation detection on the read path.
    Y_UNIT_TEST(InvisibleRowSkips) {
        TTliTestHelper h;

        h.CreateTable("Table");
        h.InsertData("UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"Initial\")");

        // Victim reads key 1 at snapshot V1 - establishes lock
        auto victimTx = h.VictimBeginRead(
            "SELECT * FROM `/Root/Tenant1/Table` WHERE Key = 1u /* victim-read1 */");

        // Breaker writes to key 1 at V2 > V1, breaking victim's lock
        h.BreakerWrite(
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"BreakerV2\") /* breaker-w */");

        // Victim reads key 1 AGAIN at snapshot V1
        // The read iterator sees V2 row but skips it as "invisible" at V1
        // This triggers InvisibleRowSkips detection and marks locks as broken
        h.VictimRead(victimTx,
            "SELECT * FROM `/Root/Tenant1/Table` WHERE Key = 1u /* victim-read2 */");

        // Victim tries to commit -> aborted because lock was broken (detected via InvisibleRowSkips)
        auto status = h.VictimCommitWrite(victimTx,
            "UPSERT INTO `/Root/Tenant1/Table` (Key, Value) VALUES (1u, \"VictimVal\") /* victim-w */");
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        auto stats = h.WaitForLockStats("breaker-w", "victim-read1", "invisible-skips");
        UNIT_ASSERT_C(stats.FoundBreaker, "Breaker not found");
        UNIT_ASSERT_C(stats.FoundVictim, "Victim not found");
        UNIT_ASSERT_VALUES_EQUAL(stats.BreakerCount, 1u);
        UNIT_ASSERT_GE(stats.VictimCount, 1u);
    }
}

} // namespace NSysView
} // namespace NKikimr

