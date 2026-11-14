#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <util/string/subst.h>

#include <algorithm>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;

struct TRebuildIndexCase {
    TString Definition;
    TString Read;
    bool Async = false;
    bool Unique = false;
    bool Vector = false;
    TString ReadMoved;
};

TRebuildIndexCase SecondaryIndex(TStringBuf mode) {
    return {
        TStringBuilder() << "GLOBAL " << mode << " ON (Text) COVER (Value)",
        R"(SELECT Key, Value FROM `/Root/TestTable` VIEW idx
            WHERE Text >= "alpha" AND Text < "beta" ORDER BY Key;)",
        mode == "ASYNC", mode == "UNIQUE", false,
        R"(SELECT Key, Value FROM `/Root/TestTable` VIEW idx WHERE Text = "beta four" ORDER BY Key;)"
    };
}

TRebuildIndexCase VectorIndex(bool prefixed) {
    return {
        TStringBuilder() << "GLOBAL USING vector_kmeans_tree ON ("
            << (prefixed ? "Bucket, " : "") << "Embedding) COVER (Value) "
            << "WITH (distance=cosine, vector_type=uint8, vector_dimension=2, clusters=2, levels=1)",
        TStringBuilder() << "SELECT Key, Value FROM `/Root/TestTable` VIEW idx "
            << (prefixed ? "WHERE Bucket = 7u " : "")
            << R"(ORDER BY Knn::CosineDistance(Embedding, "\x01\x01\x02") LIMIT 1;)",
        false, false, true, {}
    };
}

TRebuildIndexCase FulltextIndex(bool relevance) {
    return {
        TStringBuilder() << "GLOBAL USING fulltext_" << (relevance ? "relevance" : "plain")
            << " ON (Text) COVER (Value) WITH (tokenizer=standard, use_filter_lowercase=true)",
        TStringBuilder() << "SELECT Key, Value FROM `/Root/TestTable` VIEW idx WHERE "
            << (relevance ? "FulltextScore(Text, \"alpha\") > 0" : "FulltextMatch(Text, \"alpha\")")
            << " ORDER BY Key;",
        false, false, false,
        TStringBuilder() << "SELECT Key, Value FROM `/Root/TestTable` VIEW idx WHERE "
            << (relevance ? "FulltextScore(Text, \"beta\") > 0" : "FulltextMatch(Text, \"beta\")")
            << " ORDER BY Key;"
    };
}

TRebuildIndexCase JsonIndex() {
    return {
        "GLOBAL USING json ON (Payload)",
        R"(SELECT Key, Value FROM `/Root/TestTable` VIEW idx
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "alpha"u ORDER BY Key;)",
        false, false, false,
        R"(SELECT Key, Value FROM `/Root/TestTable` VIEW idx
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "beta"u ORDER BY Key;)"
    };
}

TKikimrSettings RebuildSettings(bool compact, bool onlineUnique, bool streamWrite) {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
    auto& flags = *settings.AppConfig.MutableFeatureFlags();
    flags.SetEnableAddUniqueIndex(true);
    flags.SetEnableOnlineAddUniqueIndex(onlineUnique);
    flags.SetEnableFulltextIndex(true);
    flags.SetEnableCompactFulltextIndex(compact);
    flags.SetEnableJsonIndex(true);
    flags.SetEnableJsonIndexAutoSelect(true);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(streamWrite);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return settings;
}

void TestRebuildConcurrency(const TRebuildIndexCase& index, bool compact = false,
    bool onlineUnique = true, bool streamWrite = true)
{
    TKikimrRunner kikimr(RebuildSettings(compact, onlineUnique, streamWrite));
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
    auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
    auto ddl = [&](const TString& sql, EStatus expected = EStatus::SUCCESS) {
        Cerr << "DDL: " << sql << Endl;
        auto result = kikimr.RunCall([&] {
            return db.ExecuteQuery(sql, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expected, sql << ": " << result.GetIssues().ToString());
        return result;
    };
    auto execute = [&](const TString& sql, EStatus expected = EStatus::SUCCESS) {
        Cerr << "SQL: " << sql << Endl;
        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expected, sql << ": " << result.GetIssues().ToString());
        return result;
    };
    auto checkTable = [&](const TString& expected) {
        auto result = execute("SELECT Key, Value FROM `/Root/TestTable` ORDER BY Key;");
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    };
    auto checkIndexQuery = [&](const TString& query, const TString& expectedRows) {
        Cerr << "Index query: " << query << Endl;
        const auto expected = ReformatYson(expectedRows);
        TString actual;
        const auto deadline = runtime.GetCurrentTime() + TDuration::Seconds(30);
        bool retriedSchemaChange = false;
        do {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteQuery(query,
                    TTxControl::BeginTx(index.Async ? TTxSettings::StaleRO() : TTxSettings::SerializableRW())
                        .CommitTx()).ExtractValueSync();
            });
            if (result.GetStatus() == EStatus::ABORTED && !retriedSchemaChange) {
                // Initiating/finalizing the replacement invalidates cached query plans.
                retriedSchemaChange = true;
                continue;
            }
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            actual = FormatResultSetYson(result.GetResultSet(0));
            if (ReformatYson(actual) == expected || !index.Async) {
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        } while (runtime.GetCurrentTime() < deadline);
        CompareYson(expected, actual, query);
    };
    auto checkIndex = [&](const TString& expectedRows, const TString& expectedNearest,
        const TString& expectedMoved = "[]") {
        checkIndexQuery(index.Read, index.Vector ? expectedNearest : expectedRows);
        if (!index.ReadMoved.empty()) {
            checkIndexQuery(index.ReadMoved, expectedMoved);
        }
    };
    auto checkVectorPostings = [&](const TString& expectedRows, const TString& name = "idx") {
        if (index.Vector) {
            auto result = execute(TStringBuilder() << "SELECT Key, Value FROM `/Root/TestTable/"
                << name << "/indexImplPostingTable` ORDER BY Key;");
            CompareYson(expectedRows, FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    ddl(R"(
        CREATE TABLE `/Root/TestTable` (
            Key Uint64 NOT NULL,
            Bucket Uint64 NOT NULL,
            Text String,
            Value String,
            Embedding String,
            Payload JsonDocument,
            Spare String,
            PRIMARY KEY (Key)
        );
    )");
    execute(R"(
        UPSERT INTO `/Root/TestTable` (Key, Bucket, Text, Value, Embedding, Payload) VALUES
            (1u, 7u, "alpha one", "one", "\x01\x01\x02", JsonDocument('{"tag":"alpha"}')),
            (2u, 7u, "alpha two", "two", "\x01\x04\x02", JsonDocument('{"tag":"alpha"}')),
            (3u, 7u, "alpha three", "three", "\x04\x01\x02", JsonDocument('{"tag":"alpha"}')),
            (4u, 7u, "alpha four", "four", "\x01\x08\x02", JsonDocument('{"tag":"alpha"}'));
    )");
    ddl(TStringBuilder() << "ALTER TABLE `/Root/TestTable` ADD INDEX idx " << index.Definition << ";");
    // Partial writes must preserve unique keys both before and during rebuilding.
    execute("UPDATE `/Root/TestTable` SET Value = 'one' WHERE Key = 1u;");
    execute("UPSERT INTO `/Root/TestTable` (Key, Bucket, Value) VALUES (3u, 7u, 'three');");
    const TString initial = R"([[1u;["one"]];[2u;["two"]];[3u;["three"]];[4u;["four"]]])";
    checkIndex(initial, R"([[1u;["one"]]])");
    checkVectorPostings(initial);

    auto openTransaction = [&] {
        Cerr << "Opening interactive transaction session" << Endl;
        auto txSession = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        Cerr << "Reading in interactive transaction" << Endl;
        auto result = kikimr.RunCall([&] {
            return txSession.ExecuteQuery("SELECT Key, Value FROM `/Root/TestTable` WHERE Key = 1u;",
                TTxControl::BeginTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetTransaction());
        return std::pair{txSession, *result.GetTransaction()};
    };
    auto checkSchemaInvalidation = [&](auto& transaction) {
        auto& [txSession, tx] = transaction;
        auto result = kikimr.RunCall([&] {
            return txSession.ExecuteQuery("SELECT Key, Value FROM `/Root/TestTable` WHERE Key = 1u;",
                TTxControl::Tx(tx)).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Scheme changed for");
        auto commit = kikimr.RunCall([&] { return tx.Commit().ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::NOT_FOUND, commit.GetIssues().ToString());
    };
    auto beforeInitiation = openTransaction();

    TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> secondaryScan(runtime);
    TBlockEvents<TEvDataShard::TEvSampleKRequest> vectorScan(runtime);
    TBlockEvents<TEvDataShard::TEvLocalKMeansRequest> localVectorScan(runtime);
    TBlockEvents<TEvDataShard::TEvPrefixKMeansRequest> prefixScan(runtime);
    TBlockEvents<TEvDataShard::TEvBuildFulltextIndexRequest> fulltextScan(runtime);
    TBlockEvents<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction> replacement(runtime, [](const auto& ev) {
        const auto& record = ev->Get()->Record;
        if (!record.TransactionSize()) {
            return false;
        }
        const auto& tx = record.GetTransaction(0);
        return tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex && tx.GetInternal();
    });
    Y_DEFER {
        secondaryScan.Stop().Unblock();
        vectorScan.Stop().Unblock();
        localVectorScan.Stop().Unblock();
        prefixScan.Stop().Unblock();
        fulltextScan.Stop().Unblock();
        replacement.Stop().Unblock();
    };
    auto rebuild = kikimr.RunInThreadPool([&] {
        return db.ExecuteQuery("ALTER TABLE `/Root/TestTable` REBUILD INDEX idx;", TTxControl::NoTx(),
            NoRetryExecuteQuerySettings())
            .ExtractValueSync();
    });
    runtime.WaitFor("replacement index scan", [&] {
        return !secondaryScan.empty() || !vectorScan.empty() || !localVectorScan.empty()
            || !prefixScan.empty() || !fulltextScan.empty();
    });
    UNIT_ASSERT(!rebuild.HasValue());
    checkSchemaInvalidation(beforeInitiation);
    checkTable(initial);
    checkIndex(initial, R"([[1u;["one"]]])");

    auto checkConflictingAlter = [&] {
        for (const TString& sql : {
                TString("ALTER TABLE `/Root/TestTable` ADD COLUMN Extra Uint64;"),
                TString("ALTER TABLE `/Root/TestTable` DROP COLUMN Spare;"),
                TString("ALTER TABLE `/Root/TestTable` DROP INDEX idx;"),
                TString("ALTER TABLE `/Root/TestTable` RENAME INDEX idx TO renamed;")}) {
            ddl(sql, EStatus::OVERLOADED);
        }
        // A conflicting change must not take away the live index or cancel its rebuild.
        UNIT_ASSERT(!rebuild.HasValue());
    };
    checkConflictingAlter();

    const bool writesBlocked = index.Unique && !onlineUnique;
    auto modification = execute(R"(
        INSERT INTO `/Root/TestTable` (Key, Bucket, Text, Value, Embedding, Payload)
            VALUES (5u, 7u, "alpha five", "final-five", "\x08\x01\x02", JsonDocument('{"tag":"alpha"}'));
        UPDATE `/Root/TestTable` SET Text = "alpha changed", Value = "changed" WHERE Key = 1u;
        UPSERT INTO `/Root/TestTable` (Key, Bucket, Value) VALUES (3u, 7u, "final-three");
        DELETE FROM `/Root/TestTable` WHERE Key = 2u;
        UPDATE `/Root/TestTable` SET Text = "beta four", Payload = JsonDocument('{"tag":"beta"}')
            WHERE Key = 4u;
    )", writesBlocked ? EStatus::BAD_REQUEST : EStatus::SUCCESS);
    if (writesBlocked) {
        UNIT_ASSERT_STRING_CONTAINS(modification.GetIssues().ToString(), "under construction");
        checkTable(initial);
    } else {
        const TString duringBuild = R"([[1u;["changed"]];[3u;["final-three"]];[4u;["four"]];[5u;["final-five"]]])";
        checkTable(duringBuild);
        checkIndex(R"([[1u;["changed"]];[3u;["final-three"]];[5u;["final-five"]]])",
            R"([[1u;["changed"]]])", R"([[4u;["four"]]])");

        // Interactive changes stay invisible outside the transaction until commit.
        auto [txSession, tx] = openTransaction();
        auto write = kikimr.RunCall([&] {
            return txSession.ExecuteQuery("UPDATE `/Root/TestTable` SET Value = 'final-one' WHERE Key = 1u;",
                TTxControl::Tx(tx)).ExtractValueSync();
        });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        checkTable(duringBuild);
        checkIndex(R"([[1u;["changed"]];[3u;["final-three"]];[5u;["final-five"]]])",
            R"([[1u;["changed"]]])", R"([[4u;["four"]]])");
        auto ownRead = kikimr.RunCall([&] {
            return txSession.ExecuteQuery("SELECT Value FROM `/Root/TestTable` WHERE Key = 1u;",
                TTxControl::Tx(tx)).ExtractValueSync();
        });
        UNIT_ASSERT_C(ownRead.IsSuccess(), ownRead.GetIssues().ToString());
        CompareYson(R"([[["final-one"]]])", FormatResultSetYson(ownRead.GetResultSet(0)));
        auto commit = kikimr.RunCall([&] { return tx.Commit().ExtractValueSync(); });
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        checkIndex(R"([[1u;["final-one"]];[3u;["final-three"]];[5u;["final-five"]]])",
            R"([[1u;["final-one"]]])", R"([[4u;["four"]]])");
    }
    if (index.Unique && onlineUnique) {
        // Omitting every unique key must enrich an existing row and insert absent rows
        // with NULL keys. This also exercises the non-streaming zero-constraint path.
        execute(R"(
            UPSERT INTO `/Root/TestTable` (Key, Bucket, Value) VALUES
                (3u, 7u, "final-three"), (6u, 7u, "six"), (7u, 7u, "seven");
        )");
        auto nullKeys = execute(R"(SELECT Key, Value FROM `/Root/TestTable` VIEW idx
            WHERE Text IS NULL ORDER BY Key;)");
        CompareYson(R"([[6u;["six"]];[7u;["seven"]]])", FormatResultSetYson(nullKeys.GetResultSet(0)));
        execute("DELETE FROM `/Root/TestTable` WHERE Key >= 6u;");
        execute(R"(INSERT INTO `/Root/TestTable` (Key, Bucket, Text, Value)
            VALUES (6u, 7u, "alpha four", "reused");)");
        auto reused = execute(R"(SELECT Key, Value FROM `/Root/TestTable` VIEW idx
            WHERE Text = "alpha four";)");
        CompareYson(R"([[6u;["reused"]]])", FormatResultSetYson(reused.GetResultSet(0)));
        execute("DELETE FROM `/Root/TestTable` WHERE Key = 6u;");
        execute(R"(INSERT INTO `/Root/TestTable` (Key, Bucket, Text)
            VALUES (6u, 7u, "beta four");)", EStatus::PRECONDITION_FAILED);
        execute(R"(
            INSERT INTO `/Root/TestTable` (Key, Bucket, Text) VALUES (6u, 7u, "alpha three");
        )", EStatus::PRECONDITION_FAILED);
    }
    if (index.Vector) {
        execute("UPDATE `/Root/TestTable` SET Value = 'final-four' WHERE Key = 4u;");
    }

    secondaryScan.Stop().Unblock();
    vectorScan.Stop().Unblock();
    localVectorScan.Stop().Unblock();
    prefixScan.Stop().Unblock();
    fulltextScan.Stop().Unblock();
    runtime.WaitFor("atomic index replacement", [&] { return !replacement.empty(); });
    UNIT_ASSERT(!rebuild.HasValue());
    checkConflictingAlter();

    // Non-Ready vector indexes deliberately skip DML. Check the replacement's original
    // snapshot explicitly: filling-time inserts/deletes/covered updates are not replayed.
    if (index.Vector) {
        const auto& move = replacement.front()->Get()->Record.GetTransaction(0).GetMoveIndex();
        checkVectorPostings(initial, move.GetSrcPath());
    }
    // Offline unique builds reject writes while filling. Other non-vector types must retain
    // the earlier writes through replacement without rewriting those rows.
    auto replaceFinalRows = [&] {
        execute(R"(
            REPLACE INTO `/Root/TestTable` (Key, Bucket, Text, Value, Embedding, Payload) VALUES
                (1u, 7u, "alpha one", "final-one", "\x01\x01\x02", JsonDocument('{"tag":"alpha"}')),
                (3u, 7u, "alpha three", "final-three", "\x04\x01\x02", JsonDocument('{"tag":"alpha"}')),
                (4u, 7u, "beta four", "final-four", "\x01\x08\x02", JsonDocument('{"tag":"beta"}')),
                (5u, 7u, "alpha five", "final-five", "\x08\x01\x02", JsonDocument('{"tag":"alpha"}'));
        )");
    };
    if (writesBlocked) {
        execute("DELETE FROM `/Root/TestTable` WHERE Key = 2u;");
        replaceFinalRows();
    } else if (!index.Vector) {
        execute("UPDATE `/Root/TestTable` SET Value = 'final-four' WHERE Key = 4u;");
    }
    const TString finalRows = R"([[1u;["final-one"]];[3u;["final-three"]];[4u;["final-four"]];[5u;["final-five"]]])";
    const TString finalMatches = R"([[1u;["final-one"]];[3u;["final-three"]];[5u;["final-five"]]])";
    const TString moved = R"([[4u;["final-four"]]])";
    checkTable(finalRows);
    checkIndex(finalMatches, R"([[1u;["final-one"]]])", moved);

    auto spanningReplacement = openTransaction();
    auto pendingWrite = kikimr.RunCall([&] {
        return spanningReplacement.first.ExecuteQuery(
            "UPDATE `/Root/TestTable` SET Value = 'uncommitted' WHERE Key = 1u;",
            TTxControl::Tx(spanningReplacement.second)).ExtractValueSync();
    });
    UNIT_ASSERT_C(pendingWrite.IsSuccess(), pendingWrite.GetIssues().ToString());
    auto commitAcrossReplacement = openTransaction();
    auto commitOnlyWrite = kikimr.RunCall([&] {
        return commitAcrossReplacement.first.ExecuteQuery(
            "UPDATE `/Root/TestTable` SET Value = 'commit-only' WHERE Key = 3u;",
            TTxControl::Tx(commitAcrossReplacement.second)).ExtractValueSync();
    });
    UNIT_ASSERT_C(commitOnlyWrite.IsSuccess(), commitOnlyWrite.GetIssues().ToString());
    checkTable(finalRows);
    checkIndex(finalMatches, R"([[1u;["final-one"]]])", moved);
    replacement.Stop().Unblock();
    auto rebuilt = runtime.WaitFuture(rebuild);
    UNIT_ASSERT_C(rebuilt.IsSuccess(), rebuilt.GetIssues().ToString());
    // Commit must fail safely even when no further statement is executed after replacement.
    auto commitOnly = kikimr.RunCall([&] { return commitAcrossReplacement.second.Commit().ExtractValueSync(); });
    if (index.Vector && commitOnly.GetStatus() == EStatus::UNAVAILABLE) {
        // A vector transaction may reach a retired implementation tablet during prepare.
        UNIT_ASSERT_STRING_CONTAINS(commitOnly.GetIssues().ToString(), "deliver message to tablet");
        UNIT_ASSERT_STRING_CONTAINS(commitOnly.GetIssues().ToString(), "/Root/TestTable");
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(commitOnly.GetStatus(), EStatus::ABORTED, commitOnly.GetIssues().ToString());
    }
    checkSchemaInvalidation(spanningReplacement);
    checkTable(finalRows);
    checkIndex(finalMatches, R"([[1u;["final-one"]]])", moved);
    checkVectorPostings(initial);
    if (index.Vector) {
        // The published snapshot still contains deleted row 2 and old covered values,
        // and lacks inserted row 5. Refill only after checking these known limitations,
        // so later assertions separately verify maintenance of the new Ready index.
        execute(R"(INSERT INTO `/Root/TestTable` (Key, Bucket, Text, Value, Embedding, Payload)
            VALUES (2u, 7u, "alpha two", "two", "\x01\x04\x02", JsonDocument('{"tag":"alpha"}'));)");
        execute("DELETE FROM `/Root/TestTable`;");
        replaceFinalRows();
        checkTable(finalRows);
        checkVectorPostings(finalRows);
        for (const auto& [target, expected] : {
                std::pair{TString(R"(\x04\x01\x02)"), TString(R"([[3u;["final-three"]]])")},
                std::pair{TString(R"(\x01\x08\x02)"), TString(R"([[4u;["final-four"]]])")},
                std::pair{TString(R"(\x08\x01\x02)"), TString(R"([[5u;["final-five"]]])")}}) {
            auto query = index.Read;
            SubstGlobal(query, R"(\x01\x01\x02)", target);
            checkIndexQuery(query, expected);
        }
    }

    // Rollback after replacement must leave both the table and the new index unchanged.
    auto [rollbackSession, rollbackTx] = openTransaction();
    auto uncommitted = kikimr.RunCall([&] {
        return rollbackSession.ExecuteQuery("UPDATE `/Root/TestTable` SET Value = 'rolled-back' WHERE Key = 1u;",
            TTxControl::Tx(rollbackTx)).ExtractValueSync();
    });
    UNIT_ASSERT_C(uncommitted.IsSuccess(), uncommitted.GetIssues().ToString());
    auto rollback = kikimr.RunCall([&] { return rollbackTx.Rollback().ExtractValueSync(); });
    UNIT_ASSERT_C(rollback.IsSuccess(), rollback.GetIssues().ToString());
    checkIndex(finalMatches, R"([[1u;["final-one"]]])", moved);

    execute("UPDATE `/Root/TestTable` SET Value = 'after' WHERE Key = 1u;");
    checkIndex(R"([[1u;["after"]];[3u;["final-three"]];[5u;["final-five"]]])",
        R"([[1u;["after"]]])", moved);
    checkVectorPostings(R"([[1u;["after"]];[3u;["final-three"]];[4u;["final-four"]];[5u;["final-five"]]])");
    if (index.Unique) {
        execute("INSERT INTO `/Root/TestTable` (Key, Bucket, Text) VALUES (6u, 7u, 'alpha three');",
            EStatus::PRECONDITION_FAILED);
    }

    // The rebuild lock must be released: previously rejected ALTERs now succeed.
    ddl("ALTER TABLE `/Root/TestTable` ADD COLUMN Extra Uint64;");
    ddl("ALTER TABLE `/Root/TestTable` DROP COLUMN Spare;");
    ddl("ALTER TABLE `/Root/TestTable` RENAME INDEX idx TO renamed;");
    ddl("ALTER TABLE `/Root/TestTable` REBUILD INDEX renamed;");
    ddl("ALTER TABLE `/Root/TestTable` DROP INDEX renamed;");
}

void TestRebuildEmpty(const TRebuildIndexCase& index, bool compact = false) {
    TKikimrRunner kikimr(RebuildSettings(compact, true, true));
    auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
    auto ddl = [&](const TString& sql) {
        auto result = kikimr.RunCall([&] {
            return db.ExecuteQuery(sql, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
    };
    ddl(R"(CREATE TABLE `/Root/TestTable` (
        Key Uint64 NOT NULL, Bucket Uint64 NOT NULL, Text String, Value String, Payload JsonDocument,
        PRIMARY KEY (Key));)");
    ddl(TStringBuilder() << "ALTER TABLE `/Root/TestTable` ADD INDEX idx " << index.Definition << ";");
    ddl("ALTER TABLE `/Root/TestTable` REBUILD INDEX idx;");
    ddl("ALTER TABLE `/Root/TestTable` REBUILD INDEX idx;");
    auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
    auto execute = [&](const TString& sql) {
        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
        return result;
    };
    execute(R"(INSERT INTO `/Root/TestTable` (Key, Bucket, Text, Value, Payload) VALUES
        (1u, 7u, "alpha one", "first", JsonDocument('{"tag":"alpha"}')),
        (2u, 7u, "beta four", "second", JsonDocument('{"tag":"beta"}'));)");
    // Async index reads may initially lag behind the first committed insert.
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    for (const auto& [query, expected] : {
            std::pair{index.Read, TString(R"([[1u;["first"]]])")},
            std::pair{index.ReadMoved, TString(R"([[2u;["second"]]])")}}) {
        TString actual;
        const auto deadline = runtime.GetCurrentTime() + TDuration::Seconds(30);
        do {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteQuery(query,
                    TTxControl::BeginTx(index.Async ? TTxSettings::StaleRO() : TTxSettings::SerializableRW())
                        .CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            actual = FormatResultSetYson(result.GetResultSet(0));
            if (ReformatYson(actual) == ReformatYson(expected) || !index.Async) {
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        } while (runtime.GetCurrentTime() < deadline);
        CompareYson(expected, actual, query);
    }
    if (index.Unique) {
        auto duplicate = kikimr.RunCall([&] {
            return session.ExecuteQuery(R"(INSERT INTO `/Root/TestTable` (Key, Bucket, Text)
                VALUES (3u, 7u, "alpha one");)", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(duplicate.GetStatus(), EStatus::PRECONDITION_FAILED,
            duplicate.GetIssues().ToString());
    }
}

class TFulltextRebuildE2E {
public:
    using TSettings = NYdb::NTable::TFulltextIndexSettings;
    using TTokenizer = TSettings::ETokenizer;

    TFulltextRebuildE2E(bool relevance, bool compact, bool streamWrite = true)
        : Kikimr(RebuildSettings(compact, true, streamWrite))
        , Db(Kikimr.RunCall([&] { return Kikimr.GetQueryClient(); }))
        , Session(Kikimr.RunCall([&] { return Db.GetSession().GetValueSync().GetSession(); }))
        , Relevance(relevance)
    {}

    auto Ddl(const TString& sql) {
        auto result = Kikimr.RunCall([&] {
            return Db.ExecuteQuery(sql, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
        return result;
    }

    auto Execute(const TString& sql) {
        auto result = Kikimr.RunCall([&] {
            return Session.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
        return result;
    }

    void Create(bool inlineIndex) {
        const auto definition = TStringBuilder() << "idx GLOBAL USING fulltext_"
            << (Relevance ? "relevance" : "plain") << " ON (Text) COVER (Value) "
            << "WITH (tokenizer=whitespace, use_filter_lowercase=false)";
        Ddl(TStringBuilder() << "CREATE TABLE `/Root/TestTable` ("
            << "Key Uint64 NOT NULL, Text String, Value String, PRIMARY KEY (Key)"
            << (inlineIndex ? TStringBuilder() << ", INDEX " << definition : TString()) << ");");
        Execute(R"(UPSERT INTO `/Root/TestTable` (Key, Text, Value) VALUES
            (1u, "RED-GREEN blue", "one"), (2u, "red green", "two"),
            (3u, "a BLUE violet violet violet", "three");)");
        Execute(R"(UPSERT INTO `/Root/TestTable` (Key, Value) VALUES (4u, "no-text");)");
        if (!inlineIndex) {
            Ddl(TStringBuilder() << "ALTER TABLE `/Root/TestTable` ADD INDEX " << definition << ";");
        }
    }

    void CheckSettings(TTokenizer tokenizer, bool lowercase,
        std::optional<std::pair<i32, i32>> length = {}, size_t indexCount = 1)
    {
        auto result = Kikimr.RunCall([&] {
            auto session = Kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            return session.DescribeTable("/Root/TestTable").ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
        UNIT_ASSERT_VALUES_EQUAL(indexes.size(), indexCount);
        const auto it = std::find_if(indexes.begin(), indexes.end(), [](const auto& index) {
            return index.GetIndexName() == "idx";
        });
        UNIT_ASSERT(it != indexes.end());
        const auto& index = *it;
        UNIT_ASSERT_VALUES_EQUAL(index.GetIndexName(), "idx");
        UNIT_ASSERT_VALUES_EQUAL(index.GetIndexType(), Relevance
            ? NYdb::NTable::EIndexType::GlobalFulltextRelevance : NYdb::NTable::EIndexType::GlobalFulltextPlain);
        UNIT_ASSERT_VALUES_EQUAL(index.GetIndexColumns(), std::vector<std::string>{"Text"});
        UNIT_ASSERT_VALUES_EQUAL(index.GetDataColumns(), std::vector<std::string>{"Value"});
        const auto& settings = std::get<TSettings>(index.GetIndexSettings());
        UNIT_ASSERT_VALUES_EQUAL(settings.Columns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.Columns[0].Column.value_or(""), "Text");
        UNIT_ASSERT(settings.Columns[0].Analyzers);
        const auto& analyzers = *settings.Columns[0].Analyzers;
        UNIT_ASSERT_VALUES_EQUAL(analyzers.Tokenizer.value_or(TTokenizer::Unspecified), tokenizer);
        UNIT_ASSERT(analyzers.UseFilterLowercase.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*analyzers.UseFilterLowercase, lowercase);
        UNIT_ASSERT_VALUES_EQUAL(analyzers.UseFilterLength.value_or(false), length.has_value());
        if (length) {
            UNIT_ASSERT_VALUES_EQUAL(analyzers.FilterLengthMin.value_or(-1), length->first);
            UNIT_ASSERT_VALUES_EQUAL(analyzers.FilterLengthMax.value_or(-1), length->second);
        } else {
            UNIT_ASSERT(!analyzers.FilterLengthMin);
            UNIT_ASSERT(!analyzers.FilterLengthMax);
        }
    }

    void CheckTokens(const TString& expected) {
        auto result = Execute("SELECT DISTINCT __ydb_token FROM `/Root/TestTable/idx/indexImplTable` "
            "ORDER BY __ydb_token;");
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    }

    TString SearchQuery(TStringBuf text) const {
        return TStringBuilder() << "SELECT Key, Value FROM `/Root/TestTable` VIEW idx WHERE "
            << (Relevance ? "FulltextScore" : "FulltextMatch") << "(Text, \"" << text << "\")"
            << (Relevance ? " > 0" : "") << " ORDER BY Key;";
    }

    void CheckSearch(TStringBuf text, const TString& expected) {
        const auto query = SearchQuery(text);
        auto result = Execute(query);
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)), query);
    }

    void CheckFilteredSearch(TStringBuf text) {
        const auto result = Kikimr.RunCall([&] {
            return Session.ExecuteQuery(SearchQuery(text), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "No search terms were extracted from the query");
    }

    void CheckRanking(TStringBuf text, const TVector<ui64>& expected) {
        if (!Relevance) {
            return;
        }
        auto query = TStringBuilder() << "SELECT Key, FulltextScore(Text, \"" << text << "\") AS Score "
            << "FROM `/Root/TestTable` VIEW idx "
            << "WHERE FulltextScore(Text, \"" << text << "\") > 0 "
            << "ORDER BY Score DESC, Key;";
        auto result = Execute(query);
        TVector<ui64> keys;
        auto parser = result.GetResultSetParser(0);
        while (parser.TryNextRow()) {
            keys.push_back(parser.ColumnParser("Key").GetUint64());
        }
        UNIT_ASSERT_VALUES_EQUAL_C(keys, expected, query);
    }

    void Rebuild(TStringBuf settings = {}) {
        Ddl(TStringBuilder() << "ALTER TABLE `/Root/TestTable` REBUILD INDEX idx"
            << (settings.empty() ? TString() : TStringBuilder() << " WITH (" << settings << ")") << ";");
    }

    void RejectSettings(TStringBuf settings, TStringBuf issue) {
        auto result = Kikimr.RunCall([&] {
            return Db.ExecuteQuery(TStringBuilder() << "ALTER TABLE `/Root/TestTable` REBUILD INDEX idx WITH ("
                << settings << ");", TTxControl::NoTx(), NoRetryExecuteQuerySettings()).ExtractValueSync();
        });
        UNIT_ASSERT_C(!result.IsSuccess(), settings);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), issue);
    }

    TKikimrRunner Kikimr;
    TQueryClient Db;
    TSession Session;
    const bool Relevance;
};

void TestFulltextRebuildParameters(bool relevance, bool compact, bool inlineIndex) {
    TFulltextRebuildE2E test(relevance, compact);
    using TTokenizer = TFulltextRebuildE2E::TTokenizer;
    test.Create(inlineIndex);
    test.CheckSettings(TTokenizer::Whitespace, false);
    test.CheckTokens(R"([["BLUE"];["RED-GREEN"];["a"];["blue"];["green"];["red"];["violet"]])");
    test.CheckSearch("green", R"([[2u;["two"]]])");
    test.CheckSearch("BLUE", R"([[3u;["three"]]])");

    // Each partial override must inherit the other analyzer options and the column name.
    test.Rebuild("tokenizer=alphanumeric, parallel=2");
    test.CheckSettings(TTokenizer::Alphanumeric, false);
    test.CheckTokens(R"([["BLUE"];["GREEN"];["RED"];["a"];["blue"];["green"];["red"];["violet"]])");
    test.CheckSearch("GREEN", R"([[1u;["one"]]])");
    test.CheckSearch("green", R"([[2u;["two"]]])");

    test.Execute(R"(UPDATE `/Root/TestTable` SET Value = "changed" WHERE Key = 1u;)");
    test.Rebuild("use_filter_lowercase=true");
    test.CheckSettings(TTokenizer::Alphanumeric, true);
    test.CheckTokens(R"([["a"];["blue"];["green"];["red"];["violet"]])");
    test.CheckSearch("RED", R"([[1u;["changed"]];[2u;["two"]]])");
    test.CheckSearch("blue", R"([[1u;["changed"]];[3u;["three"]]])");
    test.CheckSearch("violet", R"([[3u;["three"]]])");
    test.CheckRanking("blue", {1, 3});

    test.Rebuild("use_filter_length=true, filter_length_min=4, filter_length_max=5");
    test.CheckSettings(TTokenizer::Alphanumeric, true, {{4, 5}});
    test.CheckTokens(R"([["blue"];["green"]])");
    test.CheckFilteredSearch("red");
    test.CheckFilteredSearch("violet");
    test.CheckSearch("green", R"([[1u;["changed"]];[2u;["two"]]])");
    // Removing repeated long terms changes document lengths and reverses the ranking.
    test.CheckRanking("blue", {3, 1});

    test.Rebuild("tokenizer=keyword, filter_length_min=1, filter_length_max=1000");
    test.CheckSettings(TTokenizer::Keyword, true, {{1, 1000}});
    const TString keywordTokens = R"([["a blue violet violet violet"];["red green"];["red-green blue"]])";
    test.CheckTokens(keywordTokens);
    test.CheckSearch("blue", "[]");
    test.CheckSearch("RED-GREEN blue", R"([[1u;["changed"]]])");
    test.CheckSearch("a BLUE violet violet violet", R"([[3u;["three"]]])");

    test.RejectSettings("tokenizer=missing", "Invalid tokenizer: missing");
    test.RejectSettings("use_filter_lowercase=missing", "Invalid use_filter_lowercase: missing");
    test.RejectSettings("clusters=3", "Unknown index setting: clusters");
    test.RejectSettings("filter_length_min=6, filter_length_max=4",
        "filter_length_min: should be less than or equal to filter_length_max");
    test.CheckSettings(TTokenizer::Keyword, true, {{1, 1000}});
    test.CheckTokens(keywordTokens);
    test.CheckSearch("RED-GREEN blue", R"([[1u;["changed"]]])");
    // Repeated rebuilds without WITH preserve the last successful analyzer and release locks.
    test.Rebuild();
    test.CheckSettings(TTokenizer::Keyword, true, {{1, 1000}});
    test.CheckTokens(keywordTokens);
    test.Execute(R"(UPSERT INTO `/Root/TestTable` (Key, Text, Value) VALUES (4u, "Fresh WORD", "after");)");
    test.CheckSearch("fresh word", R"([[4u;["after"]]])");
    test.Rebuild("use_filter_lowercase=false");
    test.CheckSettings(TTokenizer::Keyword, false, {{1, 1000}});
    const TString caseSensitiveTokens = R"([["Fresh WORD"];["RED-GREEN blue"];["a BLUE violet violet violet"];["red green"]])";
    test.CheckTokens(caseSensitiveTokens);
    test.CheckSearch("fresh word", "[]");
    test.CheckSearch("Fresh WORD", R"([[4u;["after"]]])");
    test.Rebuild("parallel=2");
    test.CheckSettings(TTokenizer::Keyword, false, {{1, 1000}});
    test.CheckTokens(caseSensitiveTokens);
    test.CheckSearch("RED-GREEN blue", R"([[1u;["changed"]]])");
}

void TestFulltextRebuildParametersWithWrites(bool relevance, bool compact, bool streamWrite = true) {
    TFulltextRebuildE2E test(relevance, compact, streamWrite);
    using TTokenizer = TFulltextRebuildE2E::TTokenizer;
    test.Create(true);
    test.Execute("DELETE FROM `/Root/TestTable`;");
    test.Execute(R"(UPSERT INTO `/Root/TestTable` (Key, Text, Value) VALUES
        (1u, "OLD-token", "one"), (2u, "KEEP TOKEN", "two"), (3u, "DROP-token", "three");)");
    auto& runtime = *test.Kikimr.GetTestServer().GetRuntime();
    TBlockEvents<TEvDataShard::TEvBuildFulltextIndexRequest> scan(runtime);
    TBlockEvents<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction> replacement(runtime, [](const auto& ev) {
        const auto& record = ev->Get()->Record;
        return record.TransactionSize() && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex
            && record.GetTransaction(0).GetInternal();
    });
    Y_DEFER {
        scan.Stop().Unblock();
        replacement.Stop().Unblock();
    };
    auto rebuild = test.Kikimr.RunInThreadPool([&] {
        return test.Db.ExecuteQuery(R"(ALTER TABLE `/Root/TestTable` REBUILD INDEX idx WITH
            (tokenizer=alphanumeric, use_filter_lowercase=true,
             use_filter_length=true, filter_length_min=4, filter_length_max=5);)",
            TTxControl::NoTx(), NoRetryExecuteQuerySettings()).ExtractValueSync();
    });
    runtime.WaitFor("fulltext rebuild filling", [&] { return !scan.empty() || rebuild.HasValue(); });
    UNIT_ASSERT_C(!rebuild.HasValue(), rebuild.GetValueSync().GetIssues().ToString());
    test.CheckSettings(TTokenizer::Whitespace, false, {}, 2);

    test.Execute(R"(UPDATE `/Root/TestTable` SET Text = "NEW-item", Value = "changed" WHERE Key = 1u;)");
    test.Execute("DELETE FROM `/Root/TestTable` WHERE Key = 3u;");
    test.Execute(R"(UPSERT INTO `/Root/TestTable` (Key, Text, Value) VALUES (4u, "FRESH-token", "inserted");)");
    test.Execute(R"(UPDATE `/Root/TestTable` SET Value = "covered" WHERE Key = 2u;)");
    test.CheckSearch("NEW-item", R"([[1u;["changed"]]])");
    test.CheckSearch("TOKEN", R"([[2u;["covered"]]])");
    test.CheckSearch("FRESH-token", R"([[4u;["inserted"]]])");
    test.CheckSearch("DROP-token", "[]");
    test.CheckSearch("item", "[]");
    scan.Stop().Unblock();
    runtime.WaitFor("fulltext replacement", [&] { return !replacement.empty() || rebuild.HasValue(); });
    UNIT_ASSERT_C(!rebuild.HasValue(), rebuild.GetValueSync().GetIssues().ToString());

    // Writes after scanning must also use the shadow index's new analyzer.
    test.Execute(R"(UPDATE `/Root/TestTable` SET Value = "before-swap" WHERE Key = 1u;)");
    test.Execute(R"(UPSERT INTO `/Root/TestTable` (Key, Text, Value) VALUES (5u, "AFTER-scan", "late");)");
    test.CheckSearch("NEW-item", R"([[1u;["before-swap"]]])");
    test.CheckSearch("AFTER-scan", R"([[5u;["late"]]])");
    replacement.Stop().Unblock();
    auto result = test.Kikimr.RunCall([&] { return rebuild.GetValueSync(); });
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    test.CheckSettings(TTokenizer::Alphanumeric, true, {{4, 5}});
    const TString tokens = R"([["after"];["fresh"];["item"];["keep"];["scan"];["token"]])";
    if (!compact) {
        // Compact postings can retain removal segments until a fresh scan rebuilds them.
        test.CheckTokens(tokens);
    }
    test.CheckSearch("TOKEN", R"([[2u;["covered"]];[4u;["inserted"]]])");
    test.CheckSearch("item", R"([[1u;["before-swap"]]])");
    test.CheckSearch("scan", R"([[5u;["late"]]])");
    test.CheckSearch("drop", "[]");

    test.Rebuild();
    test.CheckSettings(TTokenizer::Alphanumeric, true, {{4, 5}});
    test.CheckTokens(tokens);
    test.CheckSearch("item", R"([[1u;["before-swap"]]])");
    test.Execute(R"(UPDATE `/Root/TestTable` SET Text = "POST-write" WHERE Key = 4u;)");
    test.CheckSearch("fresh", "[]");
    test.CheckSearch("token", R"([[2u;["covered"]]])");
    test.CheckSearch("write", R"([[4u;["inserted"]]])");

    test.Rebuild("tokenizer=keyword, filter_length_min=1, filter_length_max=1000");
    test.CheckSettings(TTokenizer::Keyword, true, {{1, 1000}});
    test.CheckTokens(R"([["after-scan"];["keep token"];["new-item"];["post-write"]])");
    test.CheckSearch("POST-write", R"([[4u;["inserted"]]])");
    test.CheckSearch("token", "[]");
    auto rows = test.Execute("SELECT Key, Value FROM `/Root/TestTable` ORDER BY Key;");
    CompareYson(R"([[1u;["before-swap"]];[2u;["covered"]];[4u;["inserted"]];[5u;["late"]]])",
        FormatResultSetYson(rows.GetResultSet(0)));
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRebuildIndex) {
    Y_UNIT_TEST_QUAD(FulltextPlainParametersEndToEnd, InlineIndex, Compact) {
        TestFulltextRebuildParameters(false, Compact, InlineIndex);
    }

    Y_UNIT_TEST_QUAD(FulltextRelevanceParametersEndToEnd, InlineIndex, Compact) {
        TestFulltextRebuildParameters(true, Compact, InlineIndex);
    }

    Y_UNIT_TEST_QUAD(FulltextParametersWithConcurrentWrites, Relevance, Compact) {
        TestFulltextRebuildParametersWithWrites(Relevance, Compact);
    }

    Y_UNIT_TEST_QUAD(FulltextParametersWithConcurrentWritesWithoutStreaming, Relevance, Compact) {
        TestFulltextRebuildParametersWithWrites(Relevance, Compact, false);
    }

    Y_UNIT_TEST_TWIN(SyncConcurrentOperations, StreamWrite) {
        TestRebuildConcurrency(SecondaryIndex("SYNC"), false, true, StreamWrite);
    }

    Y_UNIT_TEST_TWIN(AsyncConcurrentOperations, StreamWrite) {
        TestRebuildConcurrency(SecondaryIndex("ASYNC"), false, true, StreamWrite);
    }

    Y_UNIT_TEST_QUAD(UniqueConcurrentOperations, OnlineUnique, StreamWrite) {
        TestRebuildConcurrency(SecondaryIndex("UNIQUE"), false, OnlineUnique, StreamWrite);
    }

    Y_UNIT_TEST_TWIN(VectorSnapshotAndReadyWrites, Prefixed) {
        TestRebuildConcurrency(VectorIndex(Prefixed));
    }

    Y_UNIT_TEST_QUAD(FulltextConcurrentOperations, Relevance, Compact) {
        TestRebuildConcurrency(FulltextIndex(Relevance), Compact);
    }

    Y_UNIT_TEST_TWIN(JsonConcurrentOperations, Compact) {
        TestRebuildConcurrency(JsonIndex(), Compact);
    }

    Y_UNIT_TEST(EmptySyncFirstWrite) {
        TestRebuildEmpty(SecondaryIndex("SYNC"));
    }

    Y_UNIT_TEST(EmptyAsyncFirstWrite) {
        TestRebuildEmpty(SecondaryIndex("ASYNC"));
    }

    Y_UNIT_TEST(EmptyUniqueFirstWrite) {
        TestRebuildEmpty(SecondaryIndex("UNIQUE"));
    }

    Y_UNIT_TEST_QUAD(EmptyFulltextFirstWrite, Relevance, Compact) {
        TestRebuildEmpty(FulltextIndex(Relevance), Compact);
    }

    Y_UNIT_TEST_TWIN(EmptyJsonFirstWrite, Compact) {
        TestRebuildEmpty(JsonIndex(), Compact);
    }
}

} // namespace NKikimr::NKqp
