#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

inline NKikimrConfig::TAppConfig GetAppConfig(bool enableIndexStreamWrite) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    return app;
}

static TExecuteQuerySettings ConcurrentStreamSettings() {
    return TExecuteQuerySettings()
        .ConcurrentResultSets(true);
}

static TMap<ui64, TString> CollectConcurrentResults(TExecuteQueryIterator& it) {
    TMap<ui64, TString> result;
    TMap<ui64, TStringStream> streams;

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            break;
        }

        if (streamPart.HasResultSet()) {
            auto idx = streamPart.GetResultSetIndex();
            auto resultSet = streamPart.ExtractResultSet();
            NYson::TYsonWriter writer(&streams[idx], NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
            NYdb::FormatResultSetYson(resultSet, writer);
        }
    }

    for (auto& [idx, stream] : streams) {
        result[idx] = stream.Str();
    }

    return result;
}

static void AssertConcurrentResult(const TMap<ui64, TString>& results, ui64 index, const TString& expectedYson) {
    auto it = results.find(index);
    UNIT_ASSERT_C(it != results.end(), TStringBuilder() << "result set index " << index << " not found");
    CompareYson(expectedYson, it->second);
}

static void AssertConcurrentResultUnordered(const TMap<ui64, TString>& results, ui64 index, const TString& expectedYson) {
    auto it = results.find(index);
    UNIT_ASSERT_C(it != results.end(), TStringBuilder() << "result set index " << index << " not found");
    CompareYsonUnordered(expectedYson, it->second);
}

static TString ReadTableViaQuery(TSession& session, const TString& table, const TString& columns, const TString& orderByColumn) {
    auto query = TStringBuilder() << "SELECT " << columns << " FROM " << table << " ORDER BY " << orderByColumn << ";";
    auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

static void ExecuteSchemeQuery(TSession& session, const TString& query) {
    auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

static void ExecuteDataQuery(TSession& session, const TString& query) {
    auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

Y_UNIT_TEST_SUITE(KqpConcurrentResults) {

Y_UNIT_TEST_TWIN(ConcurrentStreamSelectBetweenReturnings, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
        CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
        CREATE TABLE t3 (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t1 (key, val) VALUES (1, "a");
        INSERT INTO t2 (key, val) VALUES (10, "x");
        INSERT INTO t3 (key, val) VALUES (20, "y");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t2 (key) VALUES (10) RETURNING key, val;
            SELECT key, val FROM t1 ORDER BY key;
            UPSERT INTO t3 (key) VALUES (20) RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
        AssertConcurrentResult(results, 0, R"([[[10];["x"]]])");
        AssertConcurrentResult(results, 1, R"([[[1];["a"]]])");
        AssertConcurrentResult(results, 2, R"([[[20];["y"]]])");
    }
}

Y_UNIT_TEST_TWIN(ConcurrentStreamMultipleSelectsAndReturnings, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
        CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
        CREATE TABLE t3 (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t1 (key, val) VALUES (10, "x");
        INSERT INTO t2 (key, val) VALUES (20, "y");
        INSERT INTO t3 (key, val) VALUES (1, "a");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            SELECT * FROM t3;
            UPSERT INTO t1 (key) VALUES (10) RETURNING key, val;
            SELECT * FROM t3;
            UPSERT INTO t2 (key) VALUES (20) RETURNING key, val;
            SELECT * FROM t3;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 5u);
        AssertConcurrentResult(results, 0, R"([[[1];["a"]]])");
        AssertConcurrentResult(results, 1, R"([[[10];["x"]]])");
        AssertConcurrentResult(results, 2, R"([[[1];["a"]]])");
        AssertConcurrentResult(results, 3, R"([[[20];["y"]]])");
        AssertConcurrentResult(results, 4, R"([[[1];["a"]]])");
    }
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableTwoReturnings, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, version Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, version, val) VALUES (1, 1, "first");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, version) VALUES (1, 2) RETURNING key, version, val;
            UPSERT INTO t (key, version) VALUES (1, 3) RETURNING key, version, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2u);
        AssertConcurrentResult(results, 0, R"([[[1];[2];["first"]]])");
        AssertConcurrentResult(results, 1, R"([[[1];[3];["first"]]])");
    }

    CompareYson(R"([[[1];[3];["first"]]])", ReadTableViaQuery(session, "t", "key, version, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturning, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "overwritten");
            UPSERT INTO t (key) VALUES (1) RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["overwritten"]]])");
    }

    CompareYson(R"([[[1];["overwritten"]]])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamReturningThenReadSameTable, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t1 (key, val) VALUES (1, "original");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t1 (key, val) VALUES (1, "updated") RETURNING key, val;
            SELECT val FROM t1 WHERE key = 1;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2u);
        AssertConcurrentResult(results, 0, R"([[[1];["updated"]]])");
        AssertConcurrentResult(results, 1, R"([[["updated"]]])");
    }
}

Y_UNIT_TEST_TWIN(ConcurrentStreamReturningWithIndex, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (c0 Int64, c1 Int64, c2 Int64, PRIMARY KEY(c0),
        INDEX idx GLOBAL SYNC ON (c2));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (c0, c1, c2) VALUES (1, 10, 20);
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (c0) VALUES (1) RETURNING c0, c1, c2;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];[10];[20]]])");
    }

    CompareYson(R"([[[1];[10];[20]]])", ReadTableViaQuery(session, "t", "c0, c1, c2", "c0"));

    ExecuteDataQuery(session, R"(
        UPSERT INTO t (c0, c1, c2) VALUES (1, 100, 200);
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            $data = SELECT c0, c1, c2 FROM t WHERE c2 = 200 ORDER BY c0;
            UPSERT INTO t SELECT c0, (c1 + 1) AS c1, c2 FROM $data RETURNING c0, c1, c2;
            SELECT c0, c1, c2 FROM $data;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2u);
        AssertConcurrentResult(results, 0, R"([[[1];[101];[200]]])");
        AssertConcurrentResult(results, 1, R"([[[1];[100];[200]]])");
    }
}

Y_UNIT_TEST_TWIN(ConcurrentStreamNamedExprSharedByWriteAndSelect, EnableIndexStreamWrite) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(EnableIndexStreamWrite);
    auto settings = TKikimrSettings(app).SetWithSampleTables(false);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE Source (Key String, Value String, PRIMARY KEY(Key));
        CREATE TABLE Dest (Key String, Value String, PRIMARY KEY(Key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO Source (Key, Value) VALUES ("1", "a"), ("2", "b");
        INSERT INTO Dest (Key, Value) VALUES ("1", "c"), ("2", "d");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            $rows = SELECT Key, Value FROM Source ORDER BY Key;
            UPSERT INTO Dest (Key) SELECT Key FROM $rows RETURNING Key, Value;
            SELECT Key, Value FROM $rows;
            SELECT Key, Value FROM Dest;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
        AssertConcurrentResultUnordered(results, 0, R"([[["1"];["c"]];[["2"];["d"]]])");
        AssertConcurrentResult(results, 1, R"([[["1"];["a"]];[["2"];["b"]]])");
        AssertConcurrentResult(results, 2, R"([[["1"];["c"]];[["2"];["d"]]])");
    }
}

Y_UNIT_TEST_TWIN(ConcurrentStreamNamedExprRandomConsistency, EnableIndexStreamWrite) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(EnableIndexStreamWrite);
    auto settings = TKikimrSettings(app).SetWithSampleTables(false);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE Source (Key String, Value String, PRIMARY KEY(Key));
        CREATE TABLE Dest1 (Key String, Value String, PRIMARY KEY(Key));
        CREATE TABLE Dest2 (Key String, Value String, PRIMARY KEY(Key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO Source (Key, Value) VALUES ("1", "");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            $rows = SELECT Key, CAST(RandomUuid(Key) AS String) AS Value FROM Source;
            UPSERT INTO Dest1 SELECT * FROM $rows RETURNING Value;
            UPSERT INTO Dest2 SELECT * FROM $rows RETURNING Value;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(results.at(0), results.at(1));
    }
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningUpsert, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "overwritten");
            UPSERT INTO t (key) VALUES (1) RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["overwritten"]]])");
    }

    CompareYson(R"([[[1];["overwritten"]]])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningReplace, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, val2 String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val, val2) VALUES (1, "first", "second");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            REPLACE INTO t (key, val) VALUES (1, "updated");
            REPLACE INTO t (key, val) VALUES (1, "final") RETURNING key, val, val2;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["final"];#]])");
    }

    CompareYson(R"([[[1];["final"];#]])", ReadTableViaQuery(session, "t", "key, val, val2", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningUpdate, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "overwritten");
            UPDATE t SET val = "updated" WHERE key = 1 RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["updated"]]])");
    }

    CompareYson(R"([[[1];["updated"]]])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningDelete, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "overwritten");
            DELETE FROM t WHERE key = 1 RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["overwritten"]]])");
    }

    CompareYson(R"([])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningUpdateOn, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "overwritten");
            UPDATE t ON (key, val) VALUES (1, "updated") RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["updated"]]])");
    }

    CompareYson(R"([[[1];["updated"]]])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningAllColumnsInInput, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "first");
            UPSERT INTO t (key, val) VALUES (1, "overwritten") RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["overwritten"]]])");
    }

    CompareYson(R"([[[1];["overwritten"]]])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningDeleteOn, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val) VALUES (1, "existing");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val) VALUES (1, "overwritten");
            DELETE FROM t ON (key) VALUES (1) RETURNING key, val;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[1];["overwritten"]]])");
    }

    CompareYson(R"([])", ReadTableViaQuery(session, "t", "key, val", "key"));
}

Y_UNIT_TEST_TWIN(ConcurrentStreamSameTableBlindWriteThenReturningInsert, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    ExecuteSchemeQuery(session, R"(
        CREATE TABLE t (key Int32, val String, val2 String, PRIMARY KEY(key));
    )");

    ExecuteDataQuery(session, R"(
        INSERT INTO t (key, val, val2) VALUES (1, "existing", "other");
    )");

    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO t (key, val, val2) VALUES (1, "overwritten", "other2");
            INSERT INTO t (key, val) VALUES (2, "new") RETURNING key, val, val2;
        )", TTxControl::BeginTx().CommitTx(), ConcurrentStreamSettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto results = CollectConcurrentResults(it);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        AssertConcurrentResult(results, 0, R"([[[2];["new"];#]])");
    }

    CompareYsonUnordered(R"([[[1];["overwritten"];["other2"]];[[2];["new"];#]])", ReadTableViaQuery(session, "t", "key, val, val2", "key"));
}

} // Y_UNIT_TEST_SUITE(KqpConcurrentResults)

} // namespace NKikimr::NKqp
