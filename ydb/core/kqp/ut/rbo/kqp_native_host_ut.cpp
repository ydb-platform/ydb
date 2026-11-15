#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/file.h>
#include <util/string/subst.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <limits>
#include <numeric>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NTable;
using TMode = decltype(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED);

TKikimrSettings NativeSettings(TMode mode) {
    NKikimrConfig::TAppConfig config;
    config.MutableTableServiceConfig()->SetEnableNewRBO(mode != NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
    config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    config.MutableTableServiceConfig()->SetNativeFrontendMode(mode);
    return TKikimrSettings(config).SetWithSampleTables(false);
}

struct TResultSnapshot {
    TString Values;
    TVector<std::pair<TString, TString>> Columns;
    TString NamedValues;
};

TResultSnapshot SnapshotRows(const TResultSet& rows) {
    TResultSnapshot snapshot{FormatResultSetYson(rows), {}};
    for (const auto& column : rows.GetColumnsMeta()) {
        snapshot.Columns.emplace_back(column.Name, column.Type.ToString());
    }
    // The native and legacy frontends differ in SELECT * projection order.
    // Keep the original positional snapshot and also compare every value by
    // column name, without changing row order or normalizing types.
    const auto& source = TProtoAccessor::GetProto(rows);
    auto named = source;
    TVector<size_t> order(rows.ColumnsCount());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return source.columns(a).name() < source.columns(b).name();
    });
    named.clear_columns();
    for (const auto column : order) *named.add_columns() = source.columns(column);
    for (int row = 0; row < source.rows_size(); ++row) {
        auto* values = named.mutable_rows(row);
        values->clear_items();
        for (const auto column : order) *values->add_items() = source.rows(row).items(column);
    }
    snapshot.NamedValues = FormatResultSetYson(TResultSet(std::move(named)));
    return snapshot;
}

TResultSnapshot Snapshot(const TDataQueryResult& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
    return SnapshotRows(result.GetResultSet(0));
}

void AssertSame(const TResultSnapshot& native, const TResultSnapshot& legacy) {
    UNIT_ASSERT_VALUES_EQUAL(native.Values, legacy.Values);
    UNIT_ASSERT_VALUES_EQUAL(native.Columns.size(), legacy.Columns.size());
    for (size_t i = 0; i < native.Columns.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(native.Columns[i].first, legacy.Columns[i].first);
        UNIT_ASSERT_VALUES_EQUAL(native.Columns[i].second, legacy.Columns[i].second);
    }
}

void AssertSameByName(const TResultSnapshot& native, const TResultSnapshot& legacy) {
    auto actual = native.Columns, expected = legacy.Columns;
    std::sort(actual.begin(), actual.end());
    std::sort(expected.begin(), expected.end());
    UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    UNIT_ASSERT_VALUES_EQUAL(native.NamedValues, legacy.NamedValues);
}

TResultSnapshot ConstantQuery(TMode mode) {
    TKikimrRunner kikimr(NativeSettings(mode));
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    return Snapshot(session.ExecuteDataQuery(
        "SELECT 1 AS answer, 'hello' AS label, NULL IS NULL AS is_null",
        TTxControl::BeginTx().CommitTx()).GetValueSync());
}

TVector<TResultSnapshot> ParameterQueries(TMode mode) {
    TKikimrRunner kikimr(NativeSettings(mode));
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto prepared = session.PrepareDataQuery(
        "DECLARE $x AS Int32; SELECT $x + 3 AS answer, $x > 0 AS positive").GetValueSync();
    UNIT_ASSERT_C(prepared.IsSuccess(), prepared.GetIssues().ToString());
    auto query = prepared.GetQuery();
    const auto types = query.GetParameterTypes();
    UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(types.at("$x").ToString(), "Int32");
    TVector<TResultSnapshot> results;
    for (const i32 value : {7, -5}) {
        auto params = query.GetParamsBuilder().AddParam("$x").Int32(value).Build().Build();
        results.push_back(Snapshot(query.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
    }
    return results;
}

TResultSnapshot TableQuery(TMode mode) {
    TKikimrRunner kikimr(NativeSettings(mode));
    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();
    const auto schema = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/NativeRows` (
            id Uint64 NOT NULL,
            value Int32,
            PRIMARY KEY (id)
        )
    )").GetValueSync();
    UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
    TValueBuilder rows;
    rows.BeginList()
        .AddListItem().BeginStruct()
            .AddMember("id").Uint64(1)
            .AddMember("value").OptionalInt32(10)
        .EndStruct()
        .AddListItem().BeginStruct()
            .AddMember("id").Uint64(2)
            .AddMember("value").OptionalInt32(std::nullopt)
        .EndStruct()
    .EndList();
    const auto inserted = client.BulkUpsert("/Root/NativeRows", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
    return Snapshot(session.ExecuteDataQuery(
        "SELECT id, value, value + 1 AS incremented FROM `/Root/NativeRows` WHERE id >= 1ul ORDER BY id",
        TTxControl::BeginTx().CommitTx()).GetValueSync());
}

TVector<TResultSnapshot> TpchReadQueries(TMode mode, bool newRbo) {
    NKikimrConfig::TAppConfig config;
    auto* service = config.MutableTableServiceConfig();
    service->SetEnableNewRBO(newRbo);
    service->SetNativeFrontendMode(mode);
    service->SetEnableFallbackToYqlOptimizer(false);
    service->SetEnableInlineJoinFiltersAfterCBO(false);
    service->SetEnableFallbackOnMultipleStatements(false);
    service->SetAllowOlapDataQuery(true);
    service->SetDefaultLangVer(NYql::GetMaxLangVersion());
    service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();
    auto schema = TFileInput(SRC_("data/schema/tpch.sql")).ReadAll();
    // Same schema and storage settings as RunPerf_YqlTest.
    SubstGlobal(schema, "\n);", "\n) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    const auto created = session.ExecuteSchemeQuery(schema).GetValueSync();
    UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
    struct TRow {
        const char* Flag;
        const char* Status;
        const char* Quantity;
        const char* Price;
        const char* Discount;
        const char* Tax;
        const char* ShipDate;
        i64 Part = 2;
        const char* ShipMode = "AIR";
        const char* Instruction = "DELIVER IN PERSON";
    };
    const TRow data[]{
        {"A", "F", "17", "100", "0.05", "0.02", "1998-09-01"},
        {"A", "F", "20", "200", "0.10", "0.05", "1998-09-02"},
        {"A", "F", "99", "999", "0.90", "0.90", "1998-09-03"}, // outside inclusive date boundary
        {"R", "F", "20", "200", "0.10", "0.05", "1997-01-01"},
        {"R", "F", nullptr, "50", nullptr, "0.03", "1997-01-01"},
        {nullptr, "F", "1", "10", "0", "0", "1997-01-01"},
        {"N", "O", nullptr, nullptr, nullptr, nullptr, "1997-01-01"}, // all-null measures
        {"A", "F", "99", "999", "0.90", "0.90", nullptr},
        {"A", "F", "17", "100", "0.05", "0.02", "1998-09-01"}, // second line in the same order
        {"A", "F", "1", "10", "0", "0", "1999-01-01", 1}, // first Q19 branch
        {"A", "F", "25", "100", "0.10", "0", "1999-01-01", 3, "AIR REG"}, // third branch
        {"A", "F", "25", "100", "0.10", "0", "1999-01-01", 3, "TRUCK"},
        {"A", "F", "25", "100", "0.10", "0", "1999-01-01", 3, "AIR", "COLLECT COD"},
        {"A", "F", "25", "100", "0.10", "0", "1999-01-01", 999}, // unmatched part
        {"A", "F", "11", "100", "0", "0", "1999-01-01", 1}, // inclusive quantity boundary
        {"A", "F", "12", "100", "0", "0", "1999-01-01", 1},
    };
    // Date-dependent queries share actual joined rows, inclusive/exclusive
    // boundaries, failed predicates and NULLs. Each row is a separate order;
    // excluded Q13 comments and Brand#45 keep the earlier cases independent.
    struct TCalendarRow {
        const char* OrderDate;
        const char* ShipDate;
        const char* CommitDate;
        const char* ReceiptDate;
        const char* Priority;
        const char* ShipMode;
        const char* Quantity;
        const char* Discount;
        i64 Customer;
        i64 Supplier;
        i64 Part = 50;
    };
    const TCalendarRow calendarRows[]{
        {"1993-07-01", "1993-07-02", "1993-07-03", "1993-07-04", "1-URGENT", "MAIL", "23", ".06", 103, 3},
        {"1993-09-30", "1993-10-01", "1993-10-02", "1993-10-03", "2-HIGH", "MAIL", "23", ".06", 103, 3},
        {"1993-10-01", "1993-10-02", "1993-10-03", "1993-10-04", "1-URGENT", "MAIL", "23", ".05", 102, 2},
        {"1993-07-02", "1993-07-03", "1993-07-04", "1993-07-04", "2-HIGH", "MAIL", "23", ".06", 103, 3},
        {"1993-06-30", "1993-12-30", "1993-12-31", "1994-01-01", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1993-08-01", "1993-08-02", "1993-08-03", nullptr, "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1993-12-31", "1994-01-01", "1994-01-02", "1994-01-03", "3-MEDIUM", "MAIL", "23", ".07", 102, 2},
        {"1993-10-02", "1993-10-03", "1993-10-04", "1993-10-05", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1994-01-01", "1994-01-01", "1994-01-02", "1994-01-03", "1-URGENT", "MAIL", "23", ".05", 103, 3},
        {"1994-12-31", "1994-12-31", "1995-01-01", "1995-01-02", "3-MEDIUM", "MAIL", "23", ".07", 103, 3},
        {"1995-01-01", "1995-01-01", "1995-01-02", "1995-01-03", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1994-01-02", "1994-01-02", "1994-01-03", "1994-01-04", "2-HIGH", "MAIL", "24", ".06", 103, 3},
        {"1994-01-03", "1994-01-03", "1994-01-04", "1994-01-05", "3-MEDIUM", "MAIL", "23", ".04", 103, 3},
        {"1994-01-04", "1994-01-04", "1994-01-05", "1994-01-06", nullptr, "MAIL", "23", ".08", 103, 3},
        {"1994-01-05", "1994-01-05", "1994-01-06", "1994-01-07", "1-URGENT", "SHIP", nullptr, ".06", 103, 3},
        {"1994-01-06", "1994-01-06", "1994-01-07", "1994-01-08", "3-MEDIUM", "SHIP", "23", nullptr, 103, 3},
        {"1994-01-07", "1994-01-07", nullptr, "1994-01-09", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1994-01-08", "1994-01-08", "1994-01-08", "1994-01-09", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1994-01-09", "1994-01-09", "1994-01-10", "1994-01-10", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1994-12-31", "1995-01-01", "1995-01-02", "1995-01-03", "3-MEDIUM", "MAIL", "23", ".06", 102, 2},
        {"1995-12-31", "1996-01-01", "1996-01-02", "1996-01-03", "3-MEDIUM", "MAIL", "23", ".06", 102, 2},
        {"1996-03-30", "1996-03-31", "1996-04-01", "1996-04-02", "3-MEDIUM", "MAIL", "23", ".06", 104, 1},
        {"1996-12-30", "1996-12-31", "1997-01-01", "1997-01-02", "3-MEDIUM", "MAIL", "23", ".06", 104, 1},
        {"1996-12-31", "1997-01-01", "1997-01-02", "1997-01-03", "3-MEDIUM", "MAIL", "23", ".06", 102, 2},
        {"1996-03-31", "1996-04-01", "1996-04-02", "1996-04-03", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {"1995-12-30", "1995-12-31", "1996-01-01", "1996-01-02", "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        {nullptr, nullptr, nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 103, 3},
        // Q8: Brazilian/non-Brazilian volume in both years, at both order-date boundaries.
        {"1995-01-01", "1995-08-31", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 8},
        {"1995-08-01", "1995-08-31", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 9},
        {"1996-01-01", "1996-07-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 8},
        {"1996-06-01", "1996-07-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 9},
        {"1996-12-31", "1997-01-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 9},
        // Q14: promotional rows at the lower, last included and excluded upper ship dates.
        {"1995-08-31", "1995-09-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 9, 51},
        {"1996-08-30", "1996-08-31", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 9, 51},
        {"1996-08-31", "1996-09-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "23", ".06", 108, 9, 51},
        // Q20: two qualifying parts for one supplier, equality excluded for another,
        // and supplier 11 with no matching lines (NULL aggregate, not zero).
        {"1994-01-01", "1994-01-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "100", ".06", 108, 9},
        {"1994-12-30", "1994-12-31", nullptr, nullptr, "3-MEDIUM", "MAIL", "100", ".06", 108, 9, 51},
        {"1994-07-01", "1994-07-01", nullptr, nullptr, "3-MEDIUM", "MAIL", "200", ".06", 108, 10},
    };
    auto optionalDate = [](const char* text) -> std::optional<TInstant> {
        return text ? std::optional<TInstant>{TInstant::ParseIso8601(TString(text) + "T00:00:00Z")} : std::nullopt;
    };
    auto optionalText = [](const char* text) -> std::optional<std::string> {
        return text ? std::optional<std::string>{text} : std::nullopt;
    };
    const auto decimalType = TTypeBuilder().Decimal({12, 2}).Build();
    TValueBuilder rows;
    rows.BeginList();
    i64 key = 0;
    for (const auto& row : data) {
        ++key;
        rows.AddListItem().BeginStruct()
            .AddMember("l_orderkey").Int64(key == 9 ? 1 : key)
            .AddMember("l_linenumber").Int32(key == 9 ? 2 : 1)
            .AddMember("l_comment").OptionalUtf8(std::nullopt)
            .AddMember("l_partkey").OptionalInt64(row.Part)
            .AddMember("l_suppkey").OptionalInt64(std::nullopt)
            .AddMember("l_shipinstruct").OptionalUtf8(row.Instruction)
            .AddMember("l_shipmode").OptionalUtf8(row.ShipMode)
            .AddMember("l_commitdate").OptionalDate(std::nullopt)
            .AddMember("l_receiptdate").OptionalDate(std::nullopt)
            .AddMember("l_returnflag").OptionalUtf8(row.Flag ? std::optional<std::string>{row.Flag} : std::nullopt)
            .AddMember("l_linestatus").OptionalUtf8(row.Status)
            .AddMember("l_shipdate").OptionalDate(row.ShipDate ?
                std::optional<TInstant>{TInstant::ParseIso8601(TString(row.ShipDate) + "T00:00:00Z")} : std::nullopt);
        for (const auto [name, value] : {std::pair{"l_quantity", row.Quantity}, {"l_extendedprice", row.Price},
                {"l_discount", row.Discount}, {"l_tax", row.Tax}}) {
            rows.AddMember(name);
            if (value) rows.BeginOptional().Decimal(TDecimalValue(value, 12, 2)).EndOptional();
            else rows.EmptyOptional(decimalType);
        }
        rows.EndStruct();
    }
    // Q21: duplicate qualifying late lines; two late suppliers; one supplier
    // only; an unfinished order; a NULL competitor date; and a missing nation.
    // Q18: sums above, equal to, and below 300, including an all-NULL group.
    for (const auto& [order, line, supplier, receipt, quantity] : std::initializer_list<std::tuple<i64, i32, i64, const char*, const char*>>{
            {100, 1, 6, "1999-01-03", "150"}, {100, 2, 6, "1999-01-03", "151"}, {100, 3, 1, "1999-01-02", nullptr},
            {101, 1, 6, "1999-01-03", "150"}, {101, 2, 7, "1999-01-03", "150"},
            {102, 1, 6, "1999-01-03", "151"}, {102, 2, 6, "1999-01-03", "151"},
            {103, 1, 7, "1999-01-03", "149"}, {103, 2, 1, "1999-01-02", "150"},
            {104, 1, 6, "1999-01-03", "301"}, {104, 2, 1, "1999-01-02", nullptr},
            {105, 1, 6, "1999-01-03", nullptr}, {105, 2, 1, nullptr, nullptr},
            {106, 1, 5, "1999-01-03", nullptr}, {106, 2, 6, "1999-01-02", nullptr}}) {
        rows.AddListItem().BeginStruct()
            .AddMember("l_orderkey").Int64(order).AddMember("l_linenumber").Int32(line)
            .AddMember("l_suppkey").OptionalInt64(supplier).AddMember("l_partkey").OptionalInt64(std::nullopt)
            .AddMember("l_commitdate").OptionalDate(TInstant::ParseIso8601("1999-01-02T00:00:00Z"))
            .AddMember("l_receiptdate").OptionalDate(receipt ? std::optional<TInstant>{TInstant::ParseIso8601(TString(receipt) + "T00:00:00Z")} : std::nullopt)
            .AddMember("l_shipdate").OptionalDate(TInstant::ParseIso8601("1999-01-01T00:00:00Z"));
        rows.AddMember("l_quantity");
        if (quantity) rows.BeginOptional().Decimal(TDecimalValue(quantity, 12, 2)).EndOptional();
        else rows.EmptyOptional(decimalType);
        for (const auto* column : {"l_extendedprice", "l_discount", "l_tax"}) rows.AddMember(column).EmptyOptional(decimalType);
        for (const auto* column : {"l_comment", "l_shipinstruct", "l_shipmode", "l_returnflag", "l_linestatus"}) rows.AddMember(column).OptionalUtf8(std::nullopt);
        rows.EndStruct();
    }
    for (std::size_t i = 0; i < std::size(calendarRows); ++i) {
        const auto& row = calendarRows[i];
        rows.AddListItem().BeginStruct()
            .AddMember("l_orderkey").Int64(200 + i).AddMember("l_linenumber").Int32(1)
            .AddMember("l_suppkey").OptionalInt64(row.Supplier).AddMember("l_partkey").OptionalInt64(row.Part)
            .AddMember("l_commitdate").OptionalDate(optionalDate(row.CommitDate))
            .AddMember("l_receiptdate").OptionalDate(optionalDate(row.ReceiptDate))
            .AddMember("l_shipdate").OptionalDate(optionalDate(row.ShipDate))
            .AddMember("l_comment").OptionalUtf8(std::nullopt)
            .AddMember("l_shipinstruct").OptionalUtf8("DELIVER IN PERSON")
            .AddMember("l_shipmode").OptionalUtf8(row.ShipMode)
            .AddMember("l_returnflag").OptionalUtf8("R")
            .AddMember("l_linestatus").OptionalUtf8("F");
        for (const auto [name, value] : {std::pair{"l_quantity", row.Quantity}, {"l_extendedprice", "100"},
                {"l_discount", row.Discount}, {"l_tax", "0"}}) {
            rows.AddMember(name);
            if (value) rows.BeginOptional().Decimal(TDecimalValue(value, 12, 2)).EndOptional();
            else rows.EmptyOptional(decimalType);
        }
        rows.EndStruct();
    }
    rows.EndList();
    const auto inserted = client.BulkUpsert("/Root/lineitem", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
    TValueBuilder customers, orders, parts;
    customers.BeginList();
    // Q22 positive in-country balances average 180. Customers 105 and 113
    // exceed it without orders; 106 exceeds it but has orders. NULLs, zero,
    // negative balances and excluded phone prefixes must not skew that mean.
    for (const auto& [id, phone, balance] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {101, nullptr, nullptr}, {102, nullptr, nullptr}, {103, nullptr, nullptr},
            {104, "131234", "100"}, {105, "139876", "200"}, {106, "311234", "300"},
            {107, "231234", "50"}, {108, "991234", "1000"}, {109, nullptr, "100"},
            {110, "311234", nullptr}, {111, "311234", "-10"}, {112, "131234", "0"}, {113, "319876", "250"}}) {
        customers.AddListItem().BeginStruct()
            .AddMember("c_custkey").Int64(id)
            .AddMember("c_mktsegment").OptionalUtf8(id == 103 ? std::nullopt :
                std::optional<std::string>{id == 101 ? "BUILDING" : "AUTOMOBILE"})
            .AddMember("c_acctbal");
        if (balance) customers.BeginOptional().Decimal(TDecimalValue(balance, 12, 2)).EndOptional();
        else customers.EmptyOptional(decimalType);
        customers.AddMember("c_address").OptionalUtf8(std::nullopt)
            .AddMember("c_comment").OptionalUtf8(std::nullopt)
            .AddMember("c_name").OptionalUtf8(id == 106 ? std::optional<std::string>{"Customer106"} : std::nullopt)
            .AddMember("c_nationkey").OptionalInt32(id == 102 ? std::optional<i32>{1} : id == 103 ?
                std::optional<i32>{3} : id == 104 ? std::optional<i32>{2} : id == 108 ? std::optional<i32>{5} : std::nullopt)
            .AddMember("c_phone").OptionalUtf8(phone ? std::optional<std::string>{phone} : std::nullopt)
            .EndStruct();
    }
    customers.EndList();
    orders.BeginList();
    for (const i64 id : {1, 2, 3, 4, 5, 6, 8, 99, 100, 101, 102, 103, 104, 105, 106}) {
        orders.AddListItem().BeginStruct()
            .AddMember("o_orderkey").Int64(id)
            .AddMember("o_custkey").OptionalInt64(id >= 100 ? 106 : id == 4 ? 102 : id == 5 ? 999 : id == 6 ? 103 : 101)
            .AddMember("o_orderdate").OptionalDate(id == 8 ? std::nullopt :
                std::optional<TInstant>{TInstant::ParseIso8601(id == 102 ? "1999-01-02T00:00:00Z" : id >= 100 ? "1999-01-01T00:00:00Z" : id == 3 ? "1995-03-15T00:00:00Z" : "1995-03-14T00:00:00Z")})
            .AddMember("o_shippriority").OptionalInt32(0)
            .AddMember("o_clerk").OptionalUtf8(std::nullopt)
            .AddMember("o_comment").OptionalUtf8(id == 6 ? std::nullopt : std::optional<std::string>{
                id == 1 ? "special big requests" : id == 4 ? "specialrequests" : id == 8 ? "special requests special" :
                id == 99 ? "special and requests" : id == 3 ? "requests before special" : "ordinary"})
            .AddMember("o_orderpriority").OptionalUtf8(std::nullopt)
            .AddMember("o_orderstatus").OptionalUtf8(id < 100 ? std::nullopt : std::optional<std::string>{id == 104 ? "O" : "F"})
            .AddMember("o_totalprice");
        if (id == 100 || id == 102 || id == 104) orders.BeginOptional().Decimal(TDecimalValue(id == 104 ? "500" : "900", 12, 2)).EndOptional();
        else orders.EmptyOptional(decimalType);
        orders.EndStruct();
    }
    for (std::size_t i = 0; i < std::size(calendarRows); ++i) {
        const auto& row = calendarRows[i];
        orders.AddListItem().BeginStruct()
            .AddMember("o_orderkey").Int64(200 + i).AddMember("o_custkey").OptionalInt64(row.Customer)
            .AddMember("o_orderdate").OptionalDate(optionalDate(row.OrderDate))
            .AddMember("o_shippriority").OptionalInt32(0)
            .AddMember("o_clerk").OptionalUtf8(std::nullopt)
            .AddMember("o_comment").OptionalUtf8("special requests")
            .AddMember("o_orderpriority").OptionalUtf8(optionalText(row.Priority))
            .AddMember("o_orderstatus").OptionalUtf8(std::nullopt)
            .AddMember("o_totalprice").EmptyOptional(decimalType).EndStruct();
    }
    orders.EndList();
    parts.BeginList();
    for (const i64 id : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 50, 51}) {
        parts.AddListItem().BeginStruct()
            .AddMember("p_partkey").Int64(id)
            .AddMember("p_brand").OptionalUtf8(id == 1 ? "Brand#12" : id == 2 ? "Brand#23" : (id == 11 || id >= 50) ? "Brand#45" : "Brand#34")
            .AddMember("p_container").OptionalUtf8(id == 1 ? "SM BOX" : id == 2 ? "MED BOX" : "LG BOX")
            .AddMember("p_size").OptionalInt32(id < 5 || id >= 50 ? 3 : id == 7 || id >= 9 ? 14 : 15)
            .AddMember("p_comment").OptionalUtf8(std::nullopt)
            .AddMember("p_mfgr").OptionalUtf8("Maker")
            .AddMember("p_name").OptionalUtf8(id == 50 ? std::optional<std::string>{"forest green"} : id == 51 ? std::optional<std::string>{"forest brown"} : std::nullopt)
            .AddMember("p_retailprice").EmptyOptional(decimalType)
            .AddMember("p_type").OptionalUtf8(id < 5 ? std::nullopt : std::optional<std::string>{id == 50 ? "ECONOMY ANODIZED STEEL" : id == 51 ? "PROMO NICKEL" : id == 10 ? "MEDIUM POLISHED BRASS" : id == 8 || id == 12 ? "LARGE TIN" : "LARGE BRASS"})
            .EndStruct();
    }
    parts.EndList();
    TValueBuilder regions, nations, suppliers, partsupp;
    regions.BeginList();
    for (const i32 id : {1, 2, 3}) regions.AddListItem().BeginStruct()
        .AddMember("r_regionkey").Int32(id)
        .AddMember("r_name").OptionalUtf8(id == 1 ? "EUROPE" : id == 2 ? "ASIA" : "AMERICA")
        .AddMember("r_comment").OptionalUtf8(std::nullopt).EndStruct();
    regions.EndList();
    nations.BeginList();
    for (const i32 id : {1, 2, 3, 4, 5, 6}) nations.AddListItem().BeginStruct()
        .AddMember("n_nationkey").Int32(id)
        .AddMember("n_name").OptionalUtf8(id == 1 ? "GERMANY" : id == 2 ? "FRANCE" : id == 3 ? "JAPAN" : id == 4 ? "SAUDI ARABIA" : id == 5 ? "BRAZIL" : "CANADA")
        .AddMember("n_regionkey").OptionalInt32(id >= 5 ? 3 : id >= 3 ? 2 : 1)
        .AddMember("n_comment").OptionalUtf8(std::nullopt).EndStruct();
    nations.EndList();
    suppliers.BeginList();
    for (const i64 id : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}) suppliers.AddListItem().BeginStruct()
        .AddMember("s_suppkey").Int64(id)
        .AddMember("s_name").OptionalUtf8("Supplier" + std::to_string(id))
        .AddMember("s_nationkey").OptionalInt32(id == 5 ? std::nullopt : std::optional<i32>{id >= 9 ? 6 : id == 8 ? 5 : id >= 6 ? 4 : id == 4 ? 1 : i32(id)})
        .AddMember("s_acctbal").BeginOptional().Decimal(TDecimalValue(id == 2 ? "200" : id == 3 ? "999" : "100", 12, 2)).EndOptional()
        .AddMember("s_address").OptionalUtf8("Address")
        .AddMember("s_phone").OptionalUtf8("Phone")
        .AddMember("s_comment").OptionalUtf8(id == 1 || id == 5 ? std::nullopt : std::optional<std::string>{
            id == 3 ? "Customer repeated Complaints" : id == 6 ? "CustomerComplaints" : id == 4 ? "Complaints Customer" : "ordinary"}).EndStruct();
    suppliers.EndList();
    partsupp.BeginList();
    // Tied European minima for part 5; lower Asian/missing-nation costs must
    // not affect that minimum. Part 6 has a NULL competing cost. Parts 7/8
    // fail size/type filters even though their supply costs qualify.
    for (const auto& [part, supplier, cost] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {5, 1, "10"}, {5, 2, "10"}, {5, 3, "1"}, {5, 4, "11"}, {5, 5, "0"},
            {6, 1, "20"}, {6, 2, nullptr}, {7, 1, "1"}, {8, 1, "1"},
            // Q16: distinct suppliers across parts, excluded complaint suppliers,
            // a reversed comment pattern, and filtered brand/type/size values.
            {7, 2, "9"}, {7, 3, "8"}, {9, 1, "9"}, {9, 4, "10"}, {9, 6, "10"},
            {10, 1, "10"}, {11, 1, "10"}, {12, 2, "10"}, {50, 1, "2"}, {50, 2, "2"}, {50, 3, "2"},
            {50, 8, "2"}, {50, 9, "2"}, {50, 10, "2"}, {50, 11, "2"}, {51, 9, "2"},
            // Q11 must exclude NULL, zero and negative grouped values.
            {1, 1, nullptr}, {3, 1, "-1"}, {4, 1, "0"}}) {
        partsupp.AddListItem().BeginStruct()
            .AddMember("ps_partkey").Int64(part).AddMember("ps_suppkey").Int64(supplier)
            // Distinct positive totals make Q11's ORDER BY value deterministic.
            .AddMember("ps_availqty").OptionalInt32(part == 8 || part == 11 ? 99 : 100).AddMember("ps_comment").OptionalUtf8(std::nullopt)
            .AddMember("ps_supplycost");
        if (cost) partsupp.BeginOptional().Decimal(TDecimalValue(cost, 12, 2)).EndOptional();
        else partsupp.EmptyOptional(decimalType);
        partsupp.EndStruct();
    }
    partsupp.EndList();
    for (auto& [table, values] : TVector<std::pair<TString, TValue>>{
            {"/Root/customer", customers.Build()}, {"/Root/orders", orders.Build()}, {"/Root/part", parts.Build()},
            {"/Root/region", regions.Build()}, {"/Root/nation", nations.Build()}, {"/Root/supplier", suppliers.Build()}, {"/Root/partsupp", partsupp.Build()}}) {
        const auto loaded = client.BulkUpsert(table, std::move(values)).GetValueSync();
        UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
    }
    const TString prelude = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> {return $x;};
)";
    TVector<TResultSnapshot> snapshots;
    for (const auto [query, expectedRows] : {std::pair{1, 4}, {2, 3}, {3, 2}, {4, 2}, {5, 1}, {6, 1}, {7, 3}, {8, 2}, {9, 15}, {10, 2},
            {11, 8}, {12, 2}, {13, 4}, {14, 1}, {15, 2}, {16, 2}, {17, 1}, {18, 3}, {19, 1}, {20, 1}, {21, 2}, {22, 2}}) {
        Cerr << "TPC-H Q" << query << ", native mode " << int(mode) << ", new RBO " << newRbo << Endl;
        const auto sql = prelude + TFileInput(SRC_(TStringBuilder() << "data/yql-tpch/q" << query << ".yql")).ReadAll();
        const auto result = kikimr.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSet(0).RowsCount(), expectedRows, SnapshotRows(result.GetResultSet(0)).Values);
        if (query == 2) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto [part, supplier] : {std::pair{5, "Supplier2"}, {5, "Supplier1"}, {6, "Supplier1"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("p_partkey").GetInt64(), part);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_name").GetOptionalUtf8().value(), supplier);
            }
        }
        if (query == 4) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto* priority : {"1-URGENT", "2-HIGH"}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("o_orderpriority").GetOptionalUtf8().value(), priority);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("order_count").GetUint64(), 1);
            }
        }
        if (query == 5 || query == 6) {
            TResultSetParser values(result.GetResultSet(0));
            UNIT_ASSERT(values.TryNextRow());
            if (query == 5) UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("n_name").GetOptionalUtf8().value(), "JAPAN");
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("revenue").GetOptionalDecimal()->ToString(), query == 5 ? "846" : "37");
        }
        if (query == 7) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [supplier, customer, year, revenue] : std::initializer_list<std::tuple<const char*, const char*, ui16, const char*>>{
                    {"FRANCE", "GERMANY", 1995, "94"}, {"FRANCE", "GERMANY", 1996, "94"}, {"GERMANY", "FRANCE", 1996, "188"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("supp_nation").GetOptionalUtf8().value(), supplier);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("cust_nation").GetOptionalUtf8().value(), customer);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("l_year").GetOptionalUint16().value(), year);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("revenue").GetOptionalDecimal()->ToString(), revenue);
            }
        }
        if (query == 8) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [year, share] : {std::pair{1995, "0.5"}, {1996, "0.33"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("o_year").GetOptionalUint16().value(), year);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("mkt_share").GetOptionalDecimal()->ToString(), share);
            }
        }
        if (query == 9) {
            TResultSetParser values(result.GetResultSet(0));
            THashMap<TString, TString> expected{{"BRAZIL/1995", "48"}, {"BRAZIL/1996", "48"},
                {"CANADA/1994", "-412"}, {"CANADA/1995", "48"}, {"CANADA/1996", "96"}, {"FRANCE/1996", "48"}, {"FRANCE/1995", "48"}, {"FRANCE/1994", "48"},
                {"FRANCE/1993", "96"}, {"GERMANY/1996", "96"}, {"JAPAN/1996", "48"}, {"JAPAN/1995", "96"},
                {"JAPAN/1994", "382"}, {"JAPAN/1993", "288"}, {"JAPAN/null", "48"}};
            while (values.TryNextRow()) {
                const auto year = values.ColumnParser("o_year").GetOptionalUint16();
                const TString key = TString(values.ColumnParser("nation").GetOptionalUtf8().value()) + "/" + (year ? ToString(*year) : "null");
                UNIT_ASSERT_C(expected.contains(key), key);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("sum_profit").GetOptionalDecimal()->ToString(), expected.at(key));
                expected.erase(key);
            }
            UNIT_ASSERT(expected.empty());
        }
        if (query == 10) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [customer, revenue] : {std::pair{102, "188"}, {103, "94"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("c_custkey").GetInt64(), customer);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("revenue").GetOptionalDecimal()->ToString(), revenue);
            }
        }
        if (query == 11) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [part, value] : {std::pair{5, "2100"}, {6, "2000"}, {9, "1900"}, {10, "1000"},
                    {11, "990"}, {50, "200"}, {7, "100"}, {8, "99"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("ps_partkey").GetInt64(), part);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("value").GetOptionalDecimal()->ToString(), value);
            }
        }
        if (query == 12) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [mode, high, low] : {std::tuple{"MAIL", 2, 3}, {"SHIP", 1, 1}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("l_shipmode").GetOptionalUtf8().value(), mode);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("high_line_count").GetInt64(), high);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("low_line_count").GetInt64(), low);
            }
        }
        if (query == 14) {
            TResultSetParser values(result.GetResultSet(0));
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("promo_revenue").GetOptionalDecimal()->ToString(), "25");
        }
        if (query == 15) {
            TResultSetParser values(result.GetResultSet(0));
            for (const i64 supplier : {1, 2}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_suppkey").GetInt64(), supplier);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("total_revenue").GetOptionalDecimal()->ToString(), "94");
            }
        }
        if (query == 13) {
            TResultSetParser values(result.GetResultSet(0));
            // YQL's complex LIKE returns false for NULL, so customer 103's
            // NULL-comment order satisfies NOT LIKE and contributes one.
            for (const auto& [orders, customers] : {std::pair{0, 10}, {7, 1}, {2, 1}, {1, 1}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("c_count").GetUint64(), orders);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("custdist").GetUint64(), customers);
            }
        }
        if (query == 16) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [type, suppliers] : {std::pair{"LARGE BRASS", 3}, {"LARGE TIN", 1}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("p_brand").GetOptionalUtf8().value(), "Brand#34");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("p_type").GetOptionalUtf8().value(), type);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("p_size").GetOptionalInt32().value(), 14);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("supplier_cnt").GetUint64(), suppliers);
            }
        }
        if (query == 17) {
            TResultSetParser values(result.GetResultSet(0));
            UNIT_ASSERT(values.TryNextRow());
            const auto average = values.ColumnParser("avg_yearly").GetOptionalDecimal();
            UNIT_ASSERT(average);
            UNIT_ASSERT_VALUES_EQUAL(average->ToString(), "1.43");
        }
        if (query == 18) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [order, quantity] : {std::pair{100, "301"}, {102, "302"}, {104, "301"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("c_name").GetOptionalUtf8().value(), "Customer106");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("c_custkey").GetInt64(), 106);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("o_orderkey").GetInt64(), order);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("o_totalprice").GetOptionalDecimal()->ToString(), order == 104 ? "500" : "900");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(5).GetOptionalDecimal()->ToString(), quantity);
            }
        }
        if (query == 20) {
            TResultSetParser values(result.GetResultSet(0));
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_name").GetOptionalUtf8().value(), "Supplier9");
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_address").GetOptionalUtf8().value(), "Address");
        }
        if (query == 21) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [supplier, count] : {std::pair{"Supplier6", 3}, {"Supplier7", 1}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_name").GetOptionalUtf8().value(), supplier);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("numwait").GetUint64(), count);
            }
        }
        if (query == 22) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto& [country, total] : {std::pair{"13", "200"}, {"31", "250"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("cntrycode").GetOptionalString().value(), country);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("numcust").GetUint64(), 1);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("totacctbal").GetOptionalDecimal()->ToString(), total);
            }
        }
        if (query == 3 || query == 19) {
            TResultSetParser values(result.GetResultSet(0));
            for (const auto* expected : query == 3 ? TVector<const char*>{"190", "180"} : TVector<const char*>{"750"}) {
                UNIT_ASSERT(values.TryNextRow());
                const auto revenue = values.ColumnParser(query == 3 ? 1 : 0).GetOptionalDecimal();
                UNIT_ASSERT(revenue);
                UNIT_ASSERT_VALUES_EQUAL(revenue->ToString(), expected);
            }
        }
        if (newRbo) {
            const auto explain = kikimr.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
            UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetPlan());
            const TString plan{*explain.GetStats()->GetPlan()};
            NJson::TJsonValue root;
            NJson::ReadJsonTree(plan, &root, true);
            const auto& stats = root.GetMapSafe().at("SimplifiedPlan").GetMapSafe().at("OptimizerStats").GetMapSafe();
            const auto total = stats.at("CBOTreesTotal").GetUIntegerSafe();
            UNIT_ASSERT_VALUES_EQUAL_C(stats.at("CBOTreesOptimized").GetUIntegerSafe(), total, plan);
            if (query != 1 && query != 6) UNIT_ASSERT_C(total > 0, plan);
        }
        snapshots.push_back(SnapshotRows(result.GetResultSet(0)));
    }
    return snapshots;
}

TVector<TResultSnapshot> TpcdsReadQueries(TMode mode, bool newRbo, bool remaining = false) {
    NKikimrConfig::TAppConfig config;
    auto* service = config.MutableTableServiceConfig();
    service->SetEnableNewRBO(newRbo);
    service->SetNativeFrontendMode(mode);
    service->SetEnableFallbackToYqlOptimizer(false);
    service->SetEnableInlineJoinFiltersAfterCBO(false);
    service->SetEnableFallbackOnMultipleStatements(false);
    service->SetAllowOlapDataQuery(true);
    service->SetDefaultLangVer(NYql::GetMaxLangVersion());
    service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();
    auto schema = TFileInput(SRC_("data/schema/tpcds.sql")).ReadAll();
    SubstGlobal(schema, "\n);", "\n) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    const auto created = session.ExecuteSchemeQuery(schema).GetValueSync();
    UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
    auto load = [&](const char* table, TValueBuilder& rows) {
        rows.EndList();
        const TString path = TString("/Root/test/ds/") + table;
        const auto described = session.DescribeTable(path).GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        auto values = rows.Build();
        auto& rowType = *values.GetType().GetProto().mutable_list_type()->mutable_item()->mutable_struct_type();
        // Column-table bulk upserts require every column. Keep the original
        // benchmark schema and explicitly fill unused nullable fields.
        for (const auto& column : described.GetTableDescription().GetColumns()) {
            if (std::any_of(rowType.members().begin(), rowType.members().end(),
                    [&](const auto& member) { return member.name() == column.Name; })) continue;
            UNIT_ASSERT_C(column.Type.GetProto().has_optional_type(), column.Name);
            auto* member = rowType.add_members();
            member->set_name(column.Name);
            *member->mutable_type() = column.Type.GetProto();
            for (auto& row : *values.GetProto().mutable_items()) {
                row.add_items()->set_null_flag_value(google::protobuf::NULL_VALUE);
            }
        }
        const auto result = client.BulkUpsert(path, std::move(values)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    };
    auto itemName = [](i64 id) { return "Item" + std::to_string(1000 + id).substr(1); };
    const auto decimalType = TTypeBuilder().Decimal({7, 2}).Build();
    auto decimal = [&](TValueBuilder& builder, const char* text) {
        if (text) builder.BeginOptional().Decimal(TDecimalValue(text, 7, 2)).EndOptional();
        else builder.EmptyOptional(decimalType);
    };
    TValueBuilder dates;
    dates.BeginList();
    for (const auto& [id, text] : std::initializer_list<std::pair<i64, const char*>>{
            {1, "2001-06-02"}, {2, "2001-08-01"}, {3, "2001-08-02"}, {4, nullptr}, {5, "2001-2-30"}, {6, "2001-06-01"}, {7, "2001-7-1"},
            {10, "2002-05-30"}, {11, "2002-07-29"}, {12, "2002-07-30"}, {13, "2002-05-29"}, {14, "2002-6-1"},
            {20, "1998-03-19"}, {21, "1998-04-07"}, {22, "1998-04-08"}, {23, "1998-04-28"}, {24, "1998-03-18"}, {25, "1998-04-29"},
            {30, "1998-06-16"}, {31, "1998-06-17"}, {32, "1998-03-17"}, {33, "1998-5-1"},
            {40, "1998-03-09"}, {41, "1998-05-08"}, {42, "1998-03-08"}, {43, "1998-05-09"},
            {50, "2001-01-12"}, {51, "2001-02-11"}, {52, "2001-01-11"}, {53, "2001-02-12"}, {54, "2001-1-20"},
            {60, "2001-01-01"}, {61, "2001-02-28"}, {62, "2001-04-01"}, {63, "2001-07-01"}, {64, "2001-12-31"},
            {65, "2000-12-31"}, {66, "2002-01-01"}, {70, "2000-01-01"}, {71, "2000-02-01"}, {72, "2000-03-01"}, {73, "1999-01-01"},
            {74, "2002-04-01"}, {75, "2001-02-30"},
            {80, "1999-02-01"}, {81, "1999-04-02"}, {82, "1999-01-31"}, {83, "1999-04-03"},
            {84, "1999-05-01"}, {85, "1999-06-30"}, {86, "1999-04-30"}, {87, "1999-07-01"},
            {90, "1998-01-01"}, {91, "1998-12-31"}, {92, "1997-12-31"}, {93, "1999-01-01"}, {94, "1999-12-01"},
            {95, "1999-03-01"}, {96, "1999-03-31"}, {97, "1998-03-01"},
            {100, "2002-01-01"}, {101, "2002-02-01"}, {102, "2002-03-01"}, {103, "2002-04-01"},
            {104, "2002-05-01"}, {105, "2002-06-01"}, {106, "2002-07-01"}, {107, "2002-08-01"},
            {108, "2002-09-01"}, {109, "2002-10-01"}, {110, "2002-11-01"}, {111, "2002-12-01"},
            {170, "1998-04-01"}, {171, "1998-04-02"}, {172, "1998-04-03"}, {173, "1998-04-04"},
            {174, "1998-05-01"}, {175, "1998-05-02"}, {176, "1998-05-03"}, {177, "1998-05-04"},
            {180, "1998-01-01"}, {181, "1998-04-01"}, {182, "1998-07-01"}, {183, "1998-10-01"}, {184, "1999-01-01"},
            {190, "2000-03-01"}, {191, "2000-04-01"}, {192, "2000-05-01"}, {193, "2000-07-01"},
            {194, "2000-10-01"}, {195, "2000-11-01"}, {200, "1999-03-01"}, {201, "1999-04-01"},
            {202, "1999-07-01"}, {203, "1999-08-01"},
            {210, "1998-12-01"}, {211, "1998-11-30"}, {212, "1999-12-01"},
            {213, "2001-12-01"}, {214, "2001-11-30"}, {215, "2000-12-01"},
            {216, "1999-11-01"}, {217, "1999-11-30"}, {218, "1999-10-31"}, {219, "1999-12-01"},
            {220, "1998-11-01"}, {221, "2000-06-30"},
            {300, "1998-01-04"}, {301, "1998-01-05"}, {302, "1998-01-06"}, {303, "1998-01-07"},
            {304, "1998-01-08"}, {305, "1998-01-09"}, {306, "1998-01-10"}, {307, "1998-01-11"},
            {308, "1998-01-12"}, {309, "1999-01-03"}}) {
        std::optional<i64> year, month;
        if (id >= 60) {
            year = FromString<i64>(TStringBuf(text, 4));
            month = FromString<i64>(TStringBuf(text + 5, 2));
        }
        const char* days[]{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
        const char* day = id >= 300 && id <= 306 ? days[id - 300] : id == 307 ? "Unknown" : id == 309 ? "Sunday" : nullptr;
        dates.AddListItem().BeginStruct().AddMember("d_day_name").OptionalUtf8(day ? std::optional<std::string>{day} : std::nullopt)
            .AddMember("d_date_sk").Int64(id)
            .AddMember("d_date_id").Utf8("Date" + std::to_string(id))
            .AddMember("d_date").OptionalUtf8(text ? std::optional<std::string>{text} : std::nullopt)
            .AddMember("d_year").OptionalInt64(year).AddMember("d_moy").OptionalInt64(month)
            .AddMember("d_month_seq").OptionalInt64(year ? std::optional<i64>{(*year - 1900) * 12 + *month - 1} : std::nullopt)
            .AddMember("d_qoy").OptionalInt64(month ? std::optional<i64>{(*month - 1) / 3 + 1} : std::nullopt)
            .AddMember("d_quarter_name").OptionalUtf8(id >= 180 ? std::optional<std::string>{
                std::to_string(*year) + "Q" + std::to_string((*month - 1) / 3 + 1)} : std::nullopt).EndStruct();
    }
    load("date_dim", dates);
    const TVector<std::pair<const char*, const char*>> revenueItems{
        {"Books", "Shared"}, {"Sports", "Shared"}, {"Jewelry", "Shared"},
        {"Books", nullptr}, {"Jewelry", nullptr}, {"Sports", "AllNull"},
        {"Books", "Shared"}, {"Jewelry", "Single"}, {"Music", "Shared"},
        {nullptr, "Shared"}, {"Books", "Shared"}, {"Books", "Shared"}, {"Books", "Shared"}};
    auto optionalText = [](const char* text) { return text ? std::optional<std::string>{text} : std::nullopt; };
    TValueBuilder items;
    items.BeginList();
    struct TItemDetails {
        const char* Product; const char* Manufacturer; const char* Color; const char* Units; const char* Size;
        std::optional<i64> CategoryId = std::nullopt, BrandId = std::nullopt;
    };
    auto appendItem = [&](i64 id, const char* price, std::optional<i64> manufacturer,
            const char* category, const char* itemClass, const char* brand = nullptr, std::optional<i64> manager = std::nullopt,
            TItemDetails details = {}) {
        items.AddListItem().BeginStruct().AddMember("i_item_sk").Int64(id).AddMember("i_item_id").Utf8(itemName(id))
            .AddMember("i_item_desc").OptionalUtf8("Description" + std::to_string(id))
            .AddMember("i_category").OptionalUtf8(optionalText(category)).AddMember("i_class").OptionalUtf8(optionalText(itemClass))
            .AddMember("i_brand").OptionalUtf8(optionalText(brand)).AddMember("i_manager_id").OptionalInt64(manager)
            .AddMember("i_product_name").OptionalUtf8(optionalText(details.Product)).AddMember("i_manufact").OptionalUtf8(optionalText(details.Manufacturer))
            .AddMember("i_color").OptionalUtf8(optionalText(details.Color)).AddMember("i_units").OptionalUtf8(optionalText(details.Units))
            .AddMember("i_size").OptionalUtf8(optionalText(details.Size))
            .AddMember("i_category_id").OptionalInt64(details.CategoryId).AddMember("i_brand_id").OptionalInt64(details.BrandId)
            .AddMember("i_manufact_id").OptionalInt64(manufacturer).AddMember("i_current_price");
        decimal(items, price);
        items.EndStruct();
    };
    for (const auto& [id, price, manufacturer] : std::initializer_list<std::tuple<i64, const char*, i64>>{
            {1, "22", 678}, {2, "52", 964}, {3, "35", 918}, {4, "21.99", 678}, {5, "52.01", 849}, {6, "35", 999},
            {7, "35", 849}, {8, "35", 849}, {9, "35", 849}, {10, "35", 849}, {11, "35", 849}, {12, "35", 849},
            {13, "35", 849}, {14, nullptr, 849}, {15, "30", 849},
            {21, "30", 437}, {22, "60", 129}, {23, "43", 727}, {24, "29.99", 437}, {25, "60.01", 663}, {26, "43", 999},
            {27, "43", 663}, {28, "43", 663}, {29, "43", 663}, {30, "43", 663}, {31, "43", 663}, {32, "43", 663},
            {33, "43", 663}, {34, nullptr, 663}, {35, "38", 663},
            {101, "1", 0}, {102, "1.49", 0}, {103, "1", 0}, {104, "1", 0}, {105, "1", 0}, {106, "0.98", 0}, {107, "1.50", 0}, {108, "1", 0},
            {300, "10", 269}, {301, "20", 269}, {302, "25", 269}, {303, "25", 999}, {304, "25", 269}, {305, "25", 269},
            {400, "10", 0}, {401, "20", 0}, {402, nullptr, 0}, {403, "30", 0}, {404, "40", 0}, {405, "50", 0},
            {406, "60", 0}, {407, "70", 0}, {408, "80", 0}, {409, "90", 0}, {410, "100", 0}, {411, "110", 0}, {412, "120", 0}}) {
        const auto [category, itemClass] = id >= 400 ? revenueItems.at(id - 400) : std::pair<const char*, const char*>{nullptr, nullptr};
        appendItem(id, price, manufacturer, category, itemClass);
    }
    appendItem(500, "10", 10, "Books", "personal", "scholaramalgamalg #14", 20);
    appendItem(501, "10", 11, "Women", "pants", "amalgimporto #1", 21);
    appendItem(502, "10", 10, "Books", "personal", "excluded brand", 20);
    appendItem(503, "10", 10, "Music", "excluded class", "amalgimporto #1", 20);
    appendItem(504, "10", 12, "Books", "personal", "scholaramalgamalg #14", 22);
    appendItem(505, "10", 13, "Books", "personal", "scholaramalgamalg #14", 23);
    appendItem(506, "10", 14, "Books", "personal", "scholaramalgamalg #14", 24);
    appendItem(507, "10", std::nullopt, "Books", "personal", "scholaramalgamalg #14");
    appendItem(700, "10", 0, "Books", "parenting", "Family");
    appendItem(701, "10", 0, "Books", "wallpaper", "Family");
    appendItem(702, "10", 0, "Books", "parenting");
    appendItem(703, "10", 0, "Books", "parenting", "Zero");
    appendItem(704, "10", 0, "Books", "parenting", "Boundary");
    appendItem(705, "10", 0, "Books", "parenting", "Null");
    appendItem(706, "10", 0, "Music", "parenting", "Family");
    appendItem(707, "10", 0, "Books", "excluded class", "Family");
    // Q41: every disjunct has its own correlated manufacturer. Inner matches
    // deliberately lie outside the outer manufacturer's numeric range.
    i64 branch = 0;
    for (const auto& [category, color, units, size] : std::initializer_list<std::tuple<const char*, const char*, const char*, const char*>>{
            {"Women", "orchid", "Pound", "petite"}, {"Women", "navy", "Each", "extra large"},
            {"Men", "bisque", "N/A", "small"}, {"Men", "cornflower", "Gross", "medium"},
            {"Women", "salmon", "Oz", "petite"}, {"Women", "steel", "Tbl", "extra large"},
            {"Men", "purple", "Dram", "small"}, {"Men", "forest", "Ounce", "medium"}}) {
        const auto product = "Product" + std::to_string(branch), manufacturer = "Match" + std::to_string(branch);
        appendItem(800 + branch, nullptr, branch == 0 ? 742 : branch == 7 ? 782 : 760, "Other", nullptr, nullptr, std::nullopt,
            {product.c_str(), manufacturer.c_str(), nullptr, nullptr, nullptr});
        appendItem(820 + branch, nullptr, 999, category, nullptr, nullptr, std::nullopt,
            {nullptr, manufacturer.c_str(), color, units, size});
        ++branch;
    }
    appendItem(808, nullptr, 760, "Other", nullptr, nullptr, std::nullopt, {nullptr, "Match0", nullptr, nullptr, nullptr});
    appendItem(809, nullptr, 760, "Other", nullptr, nullptr, std::nullopt, {"Product0", "Match0", nullptr, nullptr, nullptr});
    appendItem(810, nullptr, 741, "Other", nullptr, nullptr, std::nullopt, {"OutsideLow", "Match0", nullptr, nullptr, nullptr});
    appendItem(811, nullptr, 783, "Other", nullptr, nullptr, std::nullopt, {"OutsideHigh", "Match7", nullptr, nullptr, nullptr});
    appendItem(812, nullptr, 760, "Women", nullptr, nullptr, std::nullopt, {"NullManufacturer", nullptr, "orchid", "Pound", "petite"});
    appendItem(813, nullptr, 760, "Other", nullptr, nullptr, std::nullopt, {"MissingMatch", "Missing", nullptr, nullptr, nullptr});
    i64 rejected = 814;
    for (const auto& [category, color, units, size] : std::initializer_list<std::tuple<const char*, const char*, const char*, const char*>>{
            {"Women", "orchid", "Pound", "large"}, {"Women", "orchid", "Bucket", "petite"}, {"Women", "red", "Pound", "petite"},
            {"Music", "orchid", "Pound", "petite"}, {"Women", "orchid", "Pound", nullptr}, {"Women", nullptr, "Pound", "petite"}}) {
        const auto name = "Excluded" + std::to_string(rejected);
        appendItem(rejected, nullptr, 760, "Other", nullptr, nullptr, std::nullopt, {name.c_str(), name.c_str(), nullptr, nullptr, nullptr});
        appendItem(rejected + 20, nullptr, 999, category, nullptr, nullptr, std::nullopt, {nullptr, name.c_str(), color, units, size});
        ++rejected;
    }
    // Grouping fixtures use their own categories, outside existing query filters.
    for (const auto& [id, product, brand, itemClass, category] :
            std::initializer_list<std::tuple<i64, const char*, const char*, const char*, const char*>>{
            {970, "P", "B", "C1", "Roll A"}, {971, "P", "B", "C2", "Roll A"},
            {972, "P", "Other", "C1", "Roll B"}, {973, "Q", nullptr, nullptr, "Roll B"},
            {974, nullptr, nullptr, "C3", nullptr}, {975, "R", "B", "C4", "Roll C"}, {976, "S", "B", "C5", "Roll D"}})
        appendItem(id, nullptr, std::nullopt, category, itemClass, brand, std::nullopt, {product, nullptr, nullptr, nullptr, nullptr});
    for (const i64 id : {1100, 1101, 1102, 1103, 1104})
        appendItem(id, nullptr, std::nullopt, "Frequent", nullptr);
    for (const auto& [id, brand] : std::initializer_list<std::pair<i64, const char*>>{
            {1200, "Main"}, {1201, "Threshold"}, {1202, "Zero"}, {1203, "Null"}, {1204, nullptr}, {1206, "Missing"}})
        appendItem(id, nullptr, std::nullopt, "Adjacent", nullptr, brand);
    appendItem(1205, nullptr, std::nullopt, nullptr, nullptr, "Main");
    for (const auto& [id, category, itemClass] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {1300, "Jewelry", "consignment"}, {1301, "Other", "consignment"}, {1302, "Jewelry", "Other"},
            {1303, nullptr, "consignment"}, {1304, "Jewelry", nullptr}})
        appendItem(id, nullptr, std::nullopt, category, itemClass);
    appendItem(1400, "10", std::nullopt, nullptr, nullptr, nullptr, 7, {nullptr, nullptr, "orchid", "Each", "Small"});
    appendItem(1401, nullptr, std::nullopt, nullptr, nullptr, nullptr, std::nullopt, {nullptr, nullptr, "orchid", nullptr, nullptr});
    appendItem(1402, "1", std::nullopt, nullptr, nullptr, nullptr, 7, {nullptr, nullptr, "red", "Each", "Small"});
    appendItem(1403, nullptr, std::nullopt, nullptr, nullptr);
    for (i64 id = 1800; id <= 1808; ++id) appendItem(id, nullptr, std::nullopt, nullptr, nullptr);
    for (const i64 id : {1900, 1901, 1902, 2100, 2101, 2102, 2200, 2201, 2202}) appendItem(id, nullptr, std::nullopt, nullptr, nullptr);
    // Q42/Q52/Q55: distinct items share group keys, IDs can share a name,
    // and one ID can have different names. Both group-key components matter.
    for (const i64 first : {2300, 2400}) {
        const i64 manager = first == 2300 ? 1 : 36;
        for (const auto& [offset, id, category, brand] :
                std::initializer_list<std::tuple<i64, std::optional<i64>, const char*, const char*>>{
                {0, 10, "Revenue A", "Same"}, {1, 10, "Revenue A", "Same"},
                {2, 20, "Revenue A", "Same"}, {3, 10, "Revenue B", "Other"},
                {4, 30, "Revenue Negative", "Negative"}, {5, 40, "Revenue Zero", "Zero"},
                {6, 50, "Revenue Null", "AllNull"}, {7, std::nullopt, nullptr, nullptr},
                {8, 60, "Revenue Excluded", "Excluded"}, {9, 61, "Revenue Excluded", "ExcludedNull"}}) {
            appendItem(first + offset, nullptr, std::nullopt, category, nullptr, brand,
                offset == 9 ? std::nullopt : std::optional<i64>{manager + (offset == 8)},
                {nullptr, nullptr, nullptr, nullptr, nullptr, id, id});
        }
    }
    // Q3 groups by both brand fields and year, independently of the manager
    // predicates used by the other revenue queries.
    for (const auto& [offset, id, brand] : std::initializer_list<std::tuple<i64, std::optional<i64>, const char*>>{
            {0, 10, "Same"}, {1, 10, "Same"}, {2, 20, "Same"}, {3, 10, "Other"},
            {4, 30, "Negative"}, {5, 40, "Zero"}, {6, 50, "AllNull"}, {7, std::nullopt, nullptr},
            {8, 60, "Excluded"}, {9, 61, "ExcludedNull"}})
        appendItem(2600 + offset, nullptr, offset == 9 ? std::nullopt : std::optional<i64>{436 + (offset == 8)},
            nullptr, nullptr, brand, std::nullopt, {nullptr, nullptr, nullptr, nullptr, nullptr, std::nullopt, id});
    for (const i64 id : {2700, 2701, 2702, 2703, 2704, 3700}) appendItem(id, nullptr, std::nullopt, nullptr, nullptr);
    for (const auto& [offset, brandId, brand, manufacturer, name] :
            std::initializer_list<std::tuple<i64, std::optional<i64>, const char*, std::optional<i64>, const char*>>{
            {0, 10, "Zip", 100, "Maker"}, {1, 10, "Zip", 100, "Maker"}, {2, 10, "Zip", 200, "Maker"},
            {3, 10, "Zip", 100, "Other"}, {4, std::nullopt, nullptr, std::nullopt, nullptr},
            {5, 30, "Neg", 300, "Neg"}, {6, 50, "Null", 500, "Null"},
            {7, 10, "Zip", 100, "Maker"}, {8, 10, "Zip", 100, "Maker"}})
        appendItem(2800 + offset, nullptr, manufacturer, nullptr, nullptr, brand,
            offset == 8 ? std::nullopt : std::optional<i64>{offset == 7 ? 8 : 7},
            {nullptr, name, nullptr, nullptr, nullptr, std::nullopt, brandId});
    load("item", items);
    TValueBuilder stores;
    stores.BeginList();
    for (const auto& [id, name, company, zip] : std::initializer_list<std::tuple<i64, const char*, const char*, const char*>>{
            {1, "Store A", "Company A", nullptr}, {2, "Store A", "Company B", nullptr}, {3, "Store B", "Company A", nullptr}, {4, nullptr, nullptr, nullptr},
            {80, "Intersect A", nullptr, "x94"}, {81, "Intersect A", nullptr, "x94"}, {82, "Intersect B", nullptr, "x08"},
            {83, "Excluded count", nullptr, "x50"}, {84, "Excluded flag", nullptr, "x29"}, {85, "Excluded list", nullptr, "x00"},
            {86, "Excluded null", nullptr, nullptr}, {87, "Intersect null", nullptr, "x39"},
            {90, "Grouping TN", nullptr, nullptr}, {91, "Grouping TX", nullptr, nullptr}, {92, "Grouping null", nullptr, nullptr},
            {110, "Adjacent", "Adjacent Company", nullptr},
            {180, "Returns A", nullptr, nullptr}, {181, "Returns B", nullptr, nullptr}, {182, "Returns null", nullptr, nullptr},
            {190, "Zip A", nullptr, "a12345A"}, {191, "Zip equal", nullptr, "b12345B"},
            {192, "Zip different", nullptr, "a12346A"}, {193, "Zip null", nullptr, nullptr}, {194, "Zip short", nullptr, "ab"}}) {
        stores.AddListItem().BeginStruct().AddMember("s_store_sk").Int64(id).AddMember("s_store_id").Utf8("Store" + std::to_string(id))
            .AddMember("s_store_name").OptionalUtf8(optionalText(name)).AddMember("s_company_name").OptionalUtf8(optionalText(company))
            .AddMember("s_zip").OptionalUtf8(optionalText(zip))
            .AddMember("s_state").OptionalUtf8(id == 90 || (id >= 180 && id <= 182) ? optionalText("TN") : id == 91 ? optionalText("TX") : std::nullopt)
            .AddMember("s_county").OptionalUtf8(std::nullopt).AddMember("s_market_id").OptionalInt64(std::nullopt).AddMember("s_gmt_offset").EmptyOptional(TTypeBuilder().Decimal({5, 2}).Build()).EndStruct();
    }
    for (const auto& [id, state, county] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {100, "CA", "Alpha"}, {101, "CA", "Beta"}, {102, "WA", "Alpha"}, {103, "WA", nullptr}, {104, "OR", nullptr},
            {130, "KS", "Segment"}, {131, "KS", nullptr}, {132, nullptr, "Segment"}})
        stores.AddListItem().BeginStruct().AddMember("s_store_sk").Int64(id).AddMember("s_store_id").Utf8("Store" + std::to_string(id))
            .AddMember("s_store_name").OptionalUtf8(std::nullopt).AddMember("s_company_name").OptionalUtf8(std::nullopt)
            .AddMember("s_zip").OptionalUtf8(std::nullopt).AddMember("s_state").OptionalUtf8(optionalText(state))
            .AddMember("s_county").OptionalUtf8(optionalText(county)).AddMember("s_market_id").OptionalInt64(std::nullopt).AddMember("s_gmt_offset").EmptyOptional(TTypeBuilder().Decimal({5, 2}).Build()).EndStruct();
    for (i64 id = 140; id <= 144; ++id)
        stores.AddListItem().BeginStruct().AddMember("s_store_sk").Int64(id).AddMember("s_store_id").Utf8("Store" + std::to_string(id))
            .AddMember("s_store_name").OptionalUtf8("Case Shop").AddMember("s_company_name").OptionalUtf8(std::nullopt)
            .AddMember("s_zip").OptionalUtf8(optionalText(id == 143 ? "wrong" : id == 144 ? nullptr : "casezip"))
            .AddMember("s_state").OptionalUtf8(std::nullopt).AddMember("s_county").OptionalUtf8(std::nullopt)
            .AddMember("s_market_id").OptionalInt64(id == 142 ? std::nullopt : std::optional<i64>{id == 141 ? 8 : 7}).AddMember("s_gmt_offset").EmptyOptional(TTypeBuilder().Decimal({5, 2}).Build()).EndStruct();
    stores.AddListItem().BeginStruct().AddMember("s_store_sk").Int64(160).AddMember("s_store_id").Utf8("Store160")
        .AddMember("s_store_name").OptionalUtf8("ese").AddMember("s_company_name").OptionalUtf8(std::nullopt)
        .AddMember("s_zip").OptionalUtf8(std::nullopt).AddMember("s_state").OptionalUtf8(std::nullopt)
        .AddMember("s_county").OptionalUtf8(std::nullopt).AddMember("s_market_id").OptionalInt64(std::nullopt).AddMember("s_gmt_offset").EmptyOptional(TTypeBuilder().Decimal({5, 2}).Build()).EndStruct();
    // Q43 groups by both store fields. Existing stores keep their NULL offset.
    for (const auto& [id, name, offset] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {3000, "Week A", "-5"}, {3001, "Week A", "-5"}, {3002, nullptr, "-5"},
            {3003, "AllNull", "-5"}, {3004, "Zero", "-5"}, {3005, "Excluded offset", "-4"}, {3006, "Unknown day", "-5"}})
        stores.AddListItem().BeginStruct().AddMember("s_store_sk").Int64(id).AddMember("s_store_id").Utf8("Store" + std::to_string(id))
            .AddMember("s_store_name").OptionalUtf8(optionalText(name)).AddMember("s_company_name").OptionalUtf8(std::nullopt)
            .AddMember("s_zip").OptionalUtf8(std::nullopt).AddMember("s_state").OptionalUtf8(std::nullopt)
            .AddMember("s_county").OptionalUtf8(std::nullopt).AddMember("s_market_id").OptionalInt64(std::nullopt)
            .AddMember("s_gmt_offset").BeginOptional().Decimal(TDecimalValue(offset, 5, 2)).EndOptional().EndStruct();
    load("store", stores);
    TValueBuilder demographics; demographics.BeginList();
    for (const auto& [id, gender, marital, education] : std::initializer_list<std::tuple<i64, const char*, const char*, const char*>>{
            {90, "F", "W", "Primary"}, {91, "M", "W", "Primary"}, {92, "F", "S", "Primary"},
            {93, "F", "W", "College"}, {94, nullptr, "W", "Primary"},
            {3000, nullptr, "M", "4 yr Degree"}, {3001, nullptr, "D", "Primary"}, {3002, nullptr, "U", "Advanced Degree"},
            {3003, nullptr, "M", "4 yr Degree"}, {3004, nullptr, "M", "Primary"},
            {3005, nullptr, nullptr, "4 yr Degree"}, {3006, nullptr, "M", nullptr},
            {3200, nullptr, "D", "2 yr Degree"}, {3201, nullptr, "S", "Secondary"}, {3202, nullptr, "W", "Advanced Degree"},
            {3203, nullptr, "D", "2 yr Degree"}, {3204, nullptr, "D", "Secondary"},
            {3205, nullptr, nullptr, "2 yr Degree"}, {3206, nullptr, "D", nullptr}})
        demographics.AddListItem().BeginStruct().AddMember("cd_demo_sk").Int64(id)
            .AddMember("cd_gender").OptionalUtf8(optionalText(gender)).AddMember("cd_marital_status").OptionalUtf8(optionalText(marital))
            .AddMember("cd_education_status").OptionalUtf8(optionalText(education)).EndStruct();
    load("customer_demographics", demographics);
    TValueBuilder promotions; promotions.BeginList();
    for (const auto& [id, email, event] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {2700, "N", "Y"}, {2701, "Y", "N"}, {2702, "N", "N"}, {2703, nullptr, "N"}, {2704, "N", nullptr},
            {2705, "Y", "Y"}, {2706, nullptr, "Y"}, {2707, "Y", nullptr}, {2708, nullptr, nullptr}})
        promotions.AddListItem().BeginStruct().AddMember("p_promo_sk").Int64(id).AddMember("p_promo_id").Utf8("Promo" + std::to_string(id))
            .AddMember("p_channel_email").OptionalUtf8(optionalText(email)).AddMember("p_channel_event").OptionalUtf8(optionalText(event)).EndStruct();
    load("promotion", promotions);
    TValueBuilder households; households.BeginList();
    for (const auto& [id, dependents] : std::initializer_list<std::pair<i64, std::optional<i64>>>{{1, 8}, {2, 7}, {3, 9}, {4, std::nullopt}})
        households.AddListItem().BeginStruct().AddMember("hd_demo_sk").Int64(id).AddMember("hd_dep_count").OptionalInt64(dependents)
            .AddMember("hd_vehicle_count").OptionalInt64(std::nullopt).EndStruct();
    for (const auto& [id, dependents, vehicles] : std::initializer_list<std::tuple<i64, std::optional<i64>, std::optional<i64>>>{
            {160, 3, 5}, {161, 0, 2}, {162, 1, 3}, {163, 3, 6}, {164, 0, 3}, {165, 1, 4},
            {166, 2, 0}, {167, std::nullopt, 1}, {168, 3, std::nullopt}, {169, 0, std::nullopt}, {170, 1, std::nullopt}})
        households.AddListItem().BeginStruct().AddMember("hd_demo_sk").Int64(id).AddMember("hd_dep_count").OptionalInt64(dependents)
            .AddMember("hd_vehicle_count").OptionalInt64(vehicles).EndStruct();
    for (const auto& [id, dependents] : std::initializer_list<std::pair<i64, std::optional<i64>>>{
            {3200, 3}, {3201, 1}, {3202, 1}, {3203, 3}, {3204, 2}, {3205, std::nullopt},
            {3300, 5}, {3301, 5}, {3302, 4}, {3303, std::nullopt}})
        households.AddListItem().BeginStruct().AddMember("hd_demo_sk").Int64(id).AddMember("hd_dep_count").OptionalInt64(dependents)
            .AddMember("hd_vehicle_count").OptionalInt64(std::nullopt).EndStruct();
    load("household_demographics", households);
    TValueBuilder times; times.BeginList();
    for (const auto& [id, hour] : std::initializer_list<std::pair<i64, std::optional<i64>>>{
            {1, 6}, {2, 7}, {3, 14}, {4, 15}, {5, 5}, {6, 8}, {7, 13}, {8, 16}, {9, std::nullopt}})
        times.AddListItem().BeginStruct().AddMember("t_time_sk").Int64(id).AddMember("t_time_id").Utf8("Time" + std::to_string(id))
            .AddMember("t_hour").OptionalInt64(hour).AddMember("t_time").OptionalInt64(std::nullopt)
            .AddMember("t_minute").OptionalInt64(std::nullopt).EndStruct();
    for (const auto& [id, seconds] : std::initializer_list<std::pair<i64, std::optional<i64>>>{
            {150, 49530}, {151, 78330}, {152, 49529}, {153, 78331}, {154, std::nullopt}})
        times.AddListItem().BeginStruct().AddMember("t_time_sk").Int64(id).AddMember("t_time_id").Utf8("Time" + std::to_string(id))
            .AddMember("t_hour").OptionalInt64(std::nullopt).AddMember("t_time").OptionalInt64(seconds)
            .AddMember("t_minute").OptionalInt64(std::nullopt).EndStruct();
    for (const auto& [id, hour, minute] : std::initializer_list<std::tuple<i64, std::optional<i64>, std::optional<i64>>>{
            {160, 8, 30}, {161, 9, 29}, {162, 9, 30}, {163, 10, 29}, {164, 10, 30}, {165, 11, 29}, {166, 11, 30}, {167, 12, 29},
            {168, 8, 29}, {169, 12, 30}, {170, 7, 59}, {171, 13, 0}, {172, std::nullopt, 30}, {173, 9, std::nullopt}, {174, 9, 0}})
        times.AddListItem().BeginStruct().AddMember("t_time_sk").Int64(id).AddMember("t_time_id").Utf8("Time" + std::to_string(id))
            .AddMember("t_hour").OptionalInt64(hour).AddMember("t_time").OptionalInt64(std::nullopt)
            .AddMember("t_minute").OptionalInt64(minute).EndStruct();
    for (const auto& [id, hour, minute] : std::initializer_list<std::tuple<i64, std::optional<i64>, std::optional<i64>>>{
            {3300, 8, 30}, {3301, 8, 59}, {3302, 8, 29}, {3303, 9, 30}, {3304, std::nullopt, 30}, {3305, 8, std::nullopt}})
        times.AddListItem().BeginStruct().AddMember("t_time_sk").Int64(id).AddMember("t_time_id").Utf8("Time" + std::to_string(id))
            .AddMember("t_hour").OptionalInt64(hour).AddMember("t_time").OptionalInt64(std::nullopt)
            .AddMember("t_minute").OptionalInt64(minute).EndStruct();
    load("time_dim", times);
    TValueBuilder pages; pages.BeginList();
    for (const auto& [id, characters] : std::initializer_list<std::pair<i64, std::optional<i64>>>{
            {1, 5000}, {2, 5200}, {3, 4999}, {4, 5201}, {5, std::nullopt}})
        pages.AddListItem().BeginStruct().AddMember("wp_web_page_sk").Int64(id).AddMember("wp_web_page_id").Utf8("Page" + std::to_string(id))
            .AddMember("wp_char_count").OptionalInt64(characters).EndStruct();
    load("web_page", pages);
    TValueBuilder warehouse; warehouse.BeginList();
    auto appendWarehouse = [&](i64 id, const char* name, const char* state, std::optional<i64> area = std::nullopt) {
        warehouse.AddListItem().BeginStruct().AddMember("w_warehouse_sk").Int64(id)
            .AddMember("w_warehouse_id").Utf8(id == 1 ? "WH001" : "Warehouse" + std::to_string(id))
            .AddMember("w_warehouse_name").OptionalUtf8(optionalText(name)).AddMember("w_state").OptionalUtf8(optionalText(state))
            .AddMember("w_warehouse_sq_ft").OptionalInt64(area).AddMember("w_city").OptionalUtf8(std::nullopt)
            .AddMember("w_county").OptionalUtf8(std::nullopt).AddMember("w_country").OptionalUtf8(std::nullopt).EndStruct();
    };
    appendWarehouse(1, "Warehouse A", "CA");
    for (const auto& [id, name] : std::initializer_list<std::pair<i64, const char*>>{
            {90, "xABCDEFGHIJKLMNOPQRST-A"}, {91, "yABCDEFGHIJKLMNOPQRST-B"}, {92, nullptr}, {93, "xOther"}})
        appendWarehouse(id, name, nullptr);
    for (const auto& [id, name, area] : std::initializer_list<std::tuple<i64, const char*, std::optional<i64>>>{
            {150, "Round", 2}, {151, "Round", 2}, {152, "Negative", -3}, {153, "Zero", 0}, {154, "ZeroZero", 0},
            {155, "Null", std::nullopt}, {156, "NullAmount", 2}, {157, "Qty", 100}})
        appendWarehouse(id, name, nullptr, area);
    appendWarehouse(170, "Deviation", nullptr);
    appendWarehouse(171, nullptr, nullptr);
    load("warehouse", warehouse);
    TValueBuilder modes; modes.BeginList();
    for (const auto& [id, name] : std::initializer_list<std::pair<i64, const char*>>{{90, "AIR"}, {91, "SEA"}, {92, nullptr}})
        modes.AddListItem().BeginStruct().AddMember("sm_ship_mode_sk").Int64(id).AddMember("sm_ship_mode_id").Utf8("Mode" + std::to_string(id))
            .AddMember("sm_type").OptionalUtf8(optionalText(name)).AddMember("sm_carrier").OptionalUtf8(std::nullopt).EndStruct();
    for (const auto& [id, carrier] : std::initializer_list<std::pair<i64, const char*>>{
            {150, "DIAMOND"}, {151, "AIRBORNE"}, {152, "Other"}, {153, nullptr}})
        modes.AddListItem().BeginStruct().AddMember("sm_ship_mode_sk").Int64(id).AddMember("sm_ship_mode_id").Utf8("Mode" + std::to_string(id))
            .AddMember("sm_type").OptionalUtf8(std::nullopt).AddMember("sm_carrier").OptionalUtf8(optionalText(carrier)).EndStruct();
    load("ship_mode", modes);
    TValueBuilder inventory;
    inventory.BeginList();
    // Q37/Q82: both date/price/quantity boundaries, invalid and NULL dates,
    // missing sales, duplicate sales, and wrong manufacturers.
    for (const auto& [date, item, quantity] : std::initializer_list<std::tuple<i64, i64, std::optional<i64>>>{
            {1, 1, 100}, {2, 2, 500}, {3, 3, 200}, {1, 4, 200}, {1, 5, 200}, {1, 6, 200},
            {1, 7, 99}, {1, 8, 501}, {1, 9, std::nullopt}, {4, 10, 200}, {5, 11, 200}, {1, 12, 200},
            {6, 13, 200}, {1, 14, 200}, {7, 15, 200},
            {10, 21, 100}, {11, 22, 500}, {12, 23, 200}, {10, 24, 200}, {10, 25, 200}, {10, 26, 200},
            {10, 27, 99}, {10, 28, 501}, {10, 29, std::nullopt}, {4, 30, 200}, {5, 31, 200}, {10, 32, 200},
            {13, 33, 200}, {10, 34, 200}, {14, 35, 200},
            // Q21: inventory on both sides of the pivot and inclusive bounds.
            {20, 101, 10}, {21, 101, 1}, {22, 101, 1}, {23, 101, 10}, {24, 101, 999}, {25, 101, 999}, {4, 101, 999}, {5, 101, 999},
            {20, 102, 10}, {23, 102, 20}, {20, 103, 10}, {23, 103, 5}, {20, 104, 0}, {23, 104, 10},
            {20, 105, std::nullopt}, {23, 105, 10}, {20, 106, 10}, {23, 106, 10}, {20, 107, 10}, {23, 107, 10},
            {20, 108, 10}, {23, 108, 10},
            // Q22: full four-key rollup, all-NULL measures, real NULL keys,
            // shared prefixes, zero/negative inputs and inclusive month bounds.
            {60, 970, 10}, {64, 970, 30}, {60, 971, 60}, {61, 972, 20}, {62, 973, 50},
            {63, 974, 0}, {60, 975, std::nullopt}, {60, 976, -20}, {64, 976, 20},
            {65, 970, 999}, {66, 970, 999}, {4, 970, 999}, {999, 970, 999}, {60, 99999, 999}}) {
        inventory.AddListItem().BeginStruct().AddMember("inv_date_sk").Int64(date).AddMember("inv_item_sk").Int64(item)
            .AddMember("inv_warehouse_sk").Int64(item == 108 ? 999 : 1)
            .AddMember("inv_quantity_on_hand").OptionalInt64(quantity).EndStruct();
    }
    // Q39: independently known sample deviations in adjacent months. Distinct
    // warehouses share an item; NULL measures do not change the finite groups.
    auto inventoryRow = [&](i64 date, i64 item, i64 warehouse, std::optional<i64> quantity) {
        inventory.AddListItem().BeginStruct().AddMember("inv_date_sk").Int64(date).AddMember("inv_item_sk").Int64(item)
            .AddMember("inv_warehouse_sk").Int64(warehouse).AddMember("inv_quantity_on_hand").OptionalInt64(quantity).EndStruct();
    };
    auto monthInventory = [&](i64 item, i64 warehouse, i64 date, std::initializer_list<std::optional<i64>> quantities) {
        for (const auto quantity : quantities) inventoryRow(date++, item, warehouse, quantity);
    };
    monthInventory(1800, 170, 170, {-1, 1, 3, std::nullopt});
    monthInventory(1800, 170, 174, {-2, 2, 6, std::nullopt});
    monthInventory(1801, 170, 170, {-1, 3, 7, std::nullopt});
    monthInventory(1801, 170, 174, {-2, 6, 14, std::nullopt});
    monthInventory(1800, 171, 170, {-3, 3, 9, std::nullopt});
    monthInventory(1800, 171, 174, {-4, 4, 12, std::nullopt});
    monthInventory(1802, 170, 170, {-2, 0, 2}); // zero mean
    monthInventory(1803, 170, 170, {-3, -1, 1}); // negative mean
    monthInventory(1804, 170, 170, {0, 1, 2}); // coefficient exactly one
    monthInventory(1805, 170, 170, {std::nullopt, std::nullopt});
    monthInventory(1806, 170, 170, {7, std::nullopt}); // singleton -> NaN
    for (i64 item = 1802; item <= 1806; ++item) monthInventory(item, 170, 174, {-2, 2, 6});
    monthInventory(1807, 170, 170, {-1, 1, 3}); // no May group
    for (const i64 date : {170, 174}) {
        monthInventory(1808, 99999, date, {-1, 1, 3}); // missing warehouse
        monthInventory(1809, 170, date, {-1, 1, 3}); // missing item
    }
    for (const i64 date : {4, 90, 93, 99999}) inventoryRow(date, 1800, 170, 999); // NULL year, wrong month/year, missing date
    load("inventory", inventory);
    struct TSale {
        i64 Item;
        i64 Order;
        std::optional<i64> Date;
        std::optional<i64> Warehouse;
        const char* Price;
        const char* Discount;
        const char* ExtendedPrice = nullptr;
        std::optional<i64> Store = std::nullopt;
        const char* NetProfit = nullptr;
        std::optional<i64> Customer = std::nullopt;
        std::optional<i64> ShipDate = std::nullopt;
        std::optional<i64> ShipAddress = std::nullopt;
        std::optional<i64> Site = std::nullopt;
        const char* ShipCost = nullptr;
        const char* NetPaid = nullptr;
        std::optional<i64> Quantity = std::nullopt;
        const char* ListPrice = nullptr;
        const char* Coupon = nullptr;
        std::optional<i64> Demographics = std::nullopt;
        std::optional<i64> ShipMode = std::nullopt;
        std::optional<i64> SoldTime = std::nullopt;
        std::optional<i64> ShipHousehold = std::nullopt;
        std::optional<i64> WebPage = std::nullopt;
        const char* NetPaidTax = nullptr;
        std::optional<i64> Household = std::nullopt;
        const char* WholesaleCost = nullptr;
        std::optional<i64> Promotion = std::nullopt;
        std::optional<i64> Address = std::nullopt;
        const char* ExtendedCost = nullptr;
    };
    TVector<TSale> catalog, store, web;
    for (i64 item = 1; item <= 15; ++item) {
        if (item == 12) continue;
        catalog.push_back({item, item * 10, 1, 1, "1", nullptr});
        store.push_back({item + 20, item * 10, 10, 1, "1", nullptr});
    }
    catalog.push_back({2, 21, 1, 1, "1", nullptr});
    store.push_back({22, 21, 10, 1, "1", nullptr});
    i64 order = 3000;
    // Q32/Q92: correlated averages, strict threshold equality, all-NULL groups,
    // excluded manufacturers, both date bounds and dates outside either end.
    for (const auto& [item, date, catalogDiscount, webDiscount] : std::initializer_list<std::tuple<i64, i64, const char*, const char*>>{
            {300, 24, "1", "2"}, {300, 33, "1", "2"}, {300, 30, "10", "20"}, {300, 24, nullptr, nullptr},
            {300, 31, "999", "1998"}, {300, 32, "999", "1998"}, {300, 4, "999", "1998"}, {300, 5, "999", "1998"},
            {301, 24, "2", "4"}, {301, 33, "2", "4"}, {301, 30, "2", "4"}, {302, 24, nullptr, nullptr}, {303, 24, "100", "200"},
            {304, 24, "2", "4"}, {304, 30, "8", "16"}, {305, 24, "3.5", "7"}, {305, 30, "6.5", "13"}}) {
        catalog.push_back({item, order, date, 1, nullptr, catalogDiscount});
        web.push_back({item, order, date, 1, nullptr, webDiscount});
        ++order;
    }
    // Q40: returns, missing returns, NULL refund/price, negative net sales,
    // inclusive dates, excluded dates and a missing warehouse.
    for (const TSale row : {TSale{101, 4001, 40, 1, "100", nullptr}, {101, 4002, 21, 1, "50", nullptr},
            {101, 4003, 22, 1, "200", nullptr}, {101, 4004, 41, 1, "100", nullptr},
            {101, 4005, 42, 1, "999", nullptr}, {101, 4006, 43, 1, "999", nullptr},
            {101, 4007, 4, 1, "999", nullptr}, {101, 4008, 5, 1, "999", nullptr}, {101, 4009, 22, 999, "999", nullptr},
            {102, 4010, 21, 1, "40", nullptr}, {102, 4011, 22, 1, nullptr, nullptr}, {104, 4012, 21, 1, nullptr, nullptr},
            {106, 4013, 21, 1, "100", nullptr}, {107, 4014, 21, 1, "100", nullptr}}) catalog.push_back(row);
    // Q12/Q20/Q98: identical item groups with distinct channel totals, shared
    // classes across categories, NULL partitions/inputs, zero and negative
    // revenue, inclusive dates, invalid dates and missing dimension rows.
    order = 5000;
    for (const auto& [item, date, webPrice, catalogPrice, storePrice] :
            std::initializer_list<std::tuple<i64, i64, const char*, const char*, const char*>>{
            {400, 50, "10", "20", "30"}, {400, 51, "5", "10", "15"}, {400, 54, nullptr, nullptr, nullptr},
            {400, 52, "999", "999", "999"}, {400, 53, "999", "999", "999"},
            {400, 4, "999", "999", "999"}, {400, 5, "999", "999", "999"}, {400, 999, "999", "999", "999"},
            {401, 50, "30", "60", "90"}, {401, 54, "15", "30", "45"}, {402, 50, nullptr, nullptr, nullptr},
            {403, 50, "20", "40", "60"}, {404, 51, "60", "120", "180"},
            {405, 50, nullptr, nullptr, nullptr}, {405, 51, nullptr, nullptr, nullptr},
            {406, 50, "0", "0", "0"}, {407, 50, "-4", "-8", "-12"},
            {408, 50, "999", "999", "999"}, {409, 50, "999", "999", "999"},
            {410, 52, "999", "999", "999"}, {410, 53, "999", "999", "999"},
            {411, 4, "999", "999", "999"}, {411, 5, "999", "999", "999"}, {412, 999, "999", "999", "999"},
            {999, 50, "999", "999", "999"}}) {
        web.push_back({item, order, date, 1, nullptr, nullptr, webPrice});
        catalog.push_back({item, order, date, 1, nullptr, nullptr, catalogPrice});
        store.push_back({item, order, date, 1, nullptr, nullptr, storePrice});
        ++order;
    }
    // Q53/Q63: monthly and quarterly groups differ, both category branches,
    // NULL partition keys, all-NULL/zero means and the exact 10% threshold.
    order = 6000;
    for (const auto& [item, date, shop, price] : std::initializer_list<std::tuple<i64, i64, std::optional<i64>, const char*>>{
            {500, 60, 1, "10"}, {500, 61, 1, "20"}, {500, 62, 1, "90"}, {500, 64, 1, "0"},
            {501, 60, 1, "10"}, {501, 62, 1, "30"}, {502, 60, 1, "999"}, {503, 60, 1, "999"},
            {504, 60, 1, "-10"}, {504, 62, 1, "10"}, {505, 60, 1, nullptr}, {505, 62, 1, nullptr},
            {506, 60, 1, "9"}, {506, 62, 1, "11"}, {507, 60, 1, "4"}, {507, 62, 1, "12"},
            {500, 60, 999, "999"}, {500, 60, std::nullopt, "999"}, {999, 60, 1, "999"},
            {500, 65, 1, "999"}, {500, 66, 1, "999"}, {500, 4, 1, "999"}, {500, 999, 1, "999"},
            // Q89: multiple classes/months share a window; company and NULL
            // keys separate partitions, with zero/NULL means and threshold ties.
            {700, 70, 1, "10"}, {700, 71, 1, "20"}, {701, 70, 1, "30"},
            {700, 70, 2, "4"}, {700, 71, 2, "12"}, {700, 70, 3, "99"},
            {702, 70, 4, "8"}, {702, 71, 4, "24"}, {703, 70, 1, "-1"}, {703, 71, 1, "1"},
            {704, 70, 1, "9"}, {704, 71, 1, "11"}, {705, 70, 1, nullptr}, {705, 71, 1, nullptr},
            {706, 70, 1, "999"}, {707, 70, 1, "999"}, {700, 73, 1, "999"}, {700, 70, 999, "999"}})
        store.push_back({item, order++, date, 1, price, nullptr, nullptr, shop});
    // Q51: overlapping and missing dates on each side of the full join,
    // duplicate sales, NULL/negative increments, and running MAX after SUM.
    for (const auto& [item, date, price] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {600, 60, "10"}, {600, 60, "5"}, {600, 61, nullptr}, {600, 62, "20"}, {600, 64, "-30"},
            {600, 65, "999"}, {600, 66, "999"}, {601, 60, "10"}, {603, 61, "20"},
            {605, 60, "10"}, {606, 60, nullptr}, {607, 60, "5"}})
        web.push_back({item, order++, date, 1, price, nullptr});
    for (const auto& [item, date, price] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {600, 60, "12"}, {600, 62, "10"}, {600, 63, "50"}, {600, 64, "-100"},
            {600, 65, "999"}, {600, 66, "999"}, {602, 60, "10"}, {603, 60, "10"},
            {605, 60, "10"}, {606, 60, "10"}, {607, 60, nullptr}})
        store.push_back({item, order++, date, 1, price, nullptr});
    // Q8: duplicate zip prefixes must not multiply sales after INTERSECT.
    // Exercise the strict >10 preferred-customer threshold, failed membership,
    // NULL zip/profit, both date predicates and missing join keys.
    TValueBuilder addresses, customers;
    addresses.BeginList(); customers.BeginList();
    auto appendCustomer = [&](i64 id, std::optional<i64> address, const char* preferred, const char* first, const char* last, const char* country = nullptr) {
        customers.AddListItem().BeginStruct().AddMember("c_customer_sk").Int64(id)
            .AddMember("c_customer_id").Utf8("Customer" + std::to_string(id)).AddMember("c_current_addr_sk").OptionalInt64(address)
            .AddMember("c_preferred_cust_flag").OptionalUtf8(optionalText(preferred))
            .AddMember("c_first_name").OptionalUtf8(optionalText(first)).AddMember("c_last_name").OptionalUtf8(optionalText(last))
            .AddMember("c_birth_country").OptionalUtf8(optionalText(country)).EndStruct();
    };
    i64 customer = 1;
    for (const auto& [id, zip, count] : std::initializer_list<std::tuple<i64, const char*, int>>{
            {80, "x89436A", 11}, {81, "x89436B", 0}, {82, "x30868A", 11}, {83, "x65085A", 10},
            {84, "x22977A", 12}, {85, "x00000A", 12}, {86, nullptr, 12}, {87, "x83927A", 11}}) {
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id)
            .AddMember("ca_address_id").Utf8("Address" + std::to_string(id)).AddMember("ca_zip").OptionalUtf8(optionalText(zip))
            .AddMember("ca_state").OptionalUtf8(std::nullopt).AddMember("ca_county").OptionalUtf8(std::nullopt)
            .AddMember("ca_country").OptionalUtf8(std::nullopt).EndStruct();
        for (int i = 0; i < count; ++i, ++customer)
            appendCustomer(customer, id, id == 84 && i >= 10 ? "N" : "Y", nullptr, nullptr);
    }
    for (const auto& [id, first, last] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {1000, "Common", "One"}, {1001, "Common", "Two"}, {1002, "Nullable", nullptr}, {1003, "Nullable", ""},
            {1004, nullptr, nullptr}, {1005, "Store", "Only"}, {1006, "Store", "Only"}, {1007, "Different", "Date"},
            {1008, "Removed", "Catalog"}, {1009, "Removed", "Web"}, {1010, "Outside", "Low"}, {1011, "Outside", "High"},
            {1012, "Missing", "Date"}})
        appendCustomer(id, std::nullopt, nullptr, first, last);
    for (const auto& [id, last] : std::initializer_list<std::pair<i64, const char*>>{
            {1100, "A"}, {1101, "B"}, {1102, "Threshold"}, {1103, "Null"}, {1104, "E"}})
        appendCustomer(id, std::nullopt, nullptr, "Buyer", last);
    for (const auto& [id, state] : std::initializer_list<std::pair<i64, const char*>>{
            {90, "IL"}, {91, "TX"}, {92, "CA"}, {93, nullptr}})
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id)
            .AddMember("ca_address_id").Utf8("Address" + std::to_string(id)).AddMember("ca_zip").OptionalUtf8(std::nullopt)
            .AddMember("ca_state").OptionalUtf8(optionalText(state)).AddMember("ca_county").OptionalUtf8(std::nullopt)
            .AddMember("ca_country").OptionalUtf8(std::nullopt).EndStruct();
    for (const auto& [id, state, county] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {1300, "KS", "Segment"}, {1301, "KS", "Other"}, {1302, nullptr, "Segment"}, {1303, "KS", nullptr}})
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id)
            .AddMember("ca_address_id").Utf8("Address" + std::to_string(id)).AddMember("ca_zip").OptionalUtf8(std::nullopt)
            .AddMember("ca_state").OptionalUtf8(optionalText(state)).AddMember("ca_county").OptionalUtf8(optionalText(county))
            .AddMember("ca_country").OptionalUtf8(std::nullopt).EndStruct();
    for (i64 id = 1300; id <= 1322; ++id) {
        const std::optional<i64> address = id == 1312 ? std::nullopt : std::optional<i64>{
            id >= 1309 && id <= 1311 ? 1301 + id - 1309 : id == 1313 ? 999 : 1300};
        appendCustomer(id, address, nullptr, nullptr, nullptr);
    }
    for (i64 id = 1400; id <= 1403; ++id)
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id).AddMember("ca_address_id").Utf8("Address" + std::to_string(id))
            .AddMember("ca_zip").OptionalUtf8(optionalText(id == 1402 ? nullptr : id == 1403 ? "wrong" : "casezip"))
            .AddMember("ca_state").OptionalUtf8(std::nullopt).AddMember("ca_county").OptionalUtf8(std::nullopt)
            .AddMember("ca_country").OptionalUtf8(optionalText(id == 1401 ? nullptr : "école")).EndStruct();
    for (const auto& [id, first, last, country] : std::initializer_list<std::tuple<i64, const char*, const char*, const char*>>{
            {1400, "Alice", "Alpha", "Else"}, {1401, "Alice", "Alpha", "Else"}, {1402, nullptr, "Bravo", "Else"},
            {1403, "Buyer", "Boundary", "Else"}, {1404, "Buyer", "Below", "Else"}, {1405, "Buyer", "Zero", "Else"},
            {1406, "Buyer", "Negative", "Else"}, {1407, "Buyer", "NullMeasure", "Else"}, {1408, "Country", "Lower", "école"},
            {1409, "Red", "Other", "Else"}, {1410, "Rejected", "Equal", "ÉCOLE"}, {1411, "Rejected", "NullCountry", "Else"},
            {1412, "Rejected", "NullBirth", nullptr}, {1413, "Rejected", "NullZip", "Else"}, {1414, "Rejected", "WrongZip", "Else"},
            {1415, "Rejected", "NullAddress", "Else"}, {1416, "Rejected", "MissingAddress", "Else"}, {1417, "NullColor", "Other", "Else"}}) {
        const std::optional<i64> address = id == 1415 ? std::nullopt : std::optional<i64>{
            id == 1411 ? 1401 : id == 1413 ? 1402 : id == 1414 ? 1403 : id == 1416 ? 99999 : 1400};
        appendCustomer(id, address, nullptr, first, last, country);
    }
    for (i64 id = 2500; id <= 2510; ++id) appendCustomer(id, std::nullopt, nullptr, nullptr, nullptr);
    for (const auto& [id, zip, state] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {2800, "z54321A", nullptr}, {2801, "b12345B", nullptr}, {2802, nullptr, nullptr},
            {2803, "a1234", nullptr}, {2804, "z12346A", nullptr},
            {2900, "x85669A", "Other"}, {2901, "y85669A", "Other"}, {2902, "x85669A", "Other"},
            {2903, "outside", "CA"}, {2904, "costly", "Other"}, {2905, nullptr, "WA"}, {2906, "allnull", "GA"},
            {2907, "negative", "CA"}, {2908, "zero", "GA"}, {2909, "rejected", "Other"},
            {2910, "x86197B", nullptr}, {2911, "unknown", nullptr}, {2912, nullptr, "Other"}, {2913, "late", "CA"}}) {
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id).AddMember("ca_address_id").Utf8("Address" + std::to_string(id))
            .AddMember("ca_zip").OptionalUtf8(optionalText(zip)).AddMember("ca_state").OptionalUtf8(optionalText(state))
            .AddMember("ca_county").OptionalUtf8(std::nullopt).AddMember("ca_country").OptionalUtf8(std::nullopt).EndStruct();
        appendCustomer(id, id, nullptr, nullptr, nullptr);
    }
    appendCustomer(2805, 99999, nullptr, nullptr, nullptr); appendCustomer(2806, std::nullopt, nullptr, nullptr, nullptr);
    appendCustomer(2914, 99999, nullptr, nullptr, nullptr); appendCustomer(2915, std::nullopt, nullptr, nullptr, nullptr);
    // Q48 has three independent state/profit bands. Duplicate attributes on
    // different address keys must not multiply a sale's contribution.
    for (const auto& [id, state, country] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {3100, "KY", "United States"}, {3101, "MT", "United States"}, {3102, "WI", "United States"},
            {3103, "KY", "United States"}, {3104, "GA", "United States"}, {3105, "NM", "United States"},
            {3106, "OR", "United States"}, {3107, "IN", "United States"}, {3108, "MO", "United States"},
            {3109, "WV", "United States"}, {3110, "KY", "Canada"}, {3111, "KY", "united states"},
            {3112, "CA", "United States"}, {3113, "KY", nullptr}, {3114, nullptr, "United States"}})
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id).AddMember("ca_address_id").Utf8("Address" + std::to_string(id))
            .AddMember("ca_zip").OptionalUtf8(std::nullopt).AddMember("ca_state").OptionalUtf8(optionalText(state))
            .AddMember("ca_county").OptionalUtf8(std::nullopt).AddMember("ca_country").OptionalUtf8(optionalText(country)).EndStruct();
    for (const auto& [id, state, country] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {3300, "CO", "United States"}, {3301, "OH", "United States"}, {3302, "TX", "United States"},
            {3303, "CO", "United States"}, {3304, "IL", "United States"}, {3305, "MN", "United States"},
            {3306, "MT", "United States"}, {3307, "NM", "United States"}, {3308, "MO", "United States"},
            {3309, "MI", "United States"}, {3310, "CO", "Canada"}, {3311, "CO", "united states"},
            {3312, "CA", "United States"}, {3313, "CO", nullptr}, {3314, nullptr, "United States"}})
        addresses.AddListItem().BeginStruct().AddMember("ca_address_sk").Int64(id).AddMember("ca_address_id").Utf8("Address" + std::to_string(id))
            .AddMember("ca_zip").OptionalUtf8(std::nullopt).AddMember("ca_state").OptionalUtf8(optionalText(state))
            .AddMember("ca_county").OptionalUtf8(std::nullopt).AddMember("ca_country").OptionalUtf8(optionalText(country)).EndStruct();
    load("customer_address", addresses); load("customer", customers);
    TValueBuilder centers, sites;
    centers.BeginList(); sites.BeginList();
    for (i64 id = 10; id <= 12; ++id) {
        centers.AddListItem().BeginStruct().AddMember("cc_call_center_sk").Int64(id)
            .AddMember("cc_call_center_id").Utf8("Center" + std::to_string(id))
            .AddMember("cc_county").OptionalUtf8(optionalText(id == 10 ? "Williamson County" : id == 11 ? "Other" : nullptr))
            .AddMember("cc_name").OptionalUtf8(std::nullopt).EndStruct();
        sites.AddListItem().BeginStruct().AddMember("web_site_sk").Int64(id)
            .AddMember("web_site_id").Utf8("Site" + std::to_string(id))
            .AddMember("web_company_name").OptionalUtf8(optionalText(id == 10 ? "pri" : id == 11 ? "Other" : nullptr))
            .AddMember("web_name").OptionalUtf8(std::nullopt).EndStruct();
    }
    for (const auto& [id, name] : std::initializer_list<std::pair<i64, const char*>>{{90, "A"}, {91, "B"}, {92, nullptr}, {93, "Adjacent"}}) {
        centers.AddListItem().BeginStruct().AddMember("cc_call_center_sk").Int64(id)
            .AddMember("cc_call_center_id").Utf8("Center" + std::to_string(id)).AddMember("cc_county").OptionalUtf8(std::nullopt)
            .AddMember("cc_name").OptionalUtf8(optionalText(name)).EndStruct();
        sites.AddListItem().BeginStruct().AddMember("web_site_sk").Int64(id)
            .AddMember("web_site_id").Utf8("Site" + std::to_string(id)).AddMember("web_company_name").OptionalUtf8(std::nullopt)
            .AddMember("web_name").OptionalUtf8(optionalText(name)).EndStruct();
    }
    load("call_center", centers); load("web_site", sites);
    for (const auto& [shop, date, profit] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {80, 66, "5"}, {80, 66, "-2"}, {81, 66, "7"}, {82, 66, "-4"}, {82, 66, nullptr}, {87, 66, nullptr},
            {83, 66, "999"}, {84, 66, "999"}, {85, 66, "999"}, {86, 66, "999"},
            {80, 74, "999"}, {80, 64, "999"}, {80, 999, "999"}, {999, 66, "999"}})
        store.push_back({950, order++, date, 1, nullptr, nullptr, nullptr, shop, profit});
    // Q38/Q87: four common name/date keys and three store-only keys. NULL and
    // empty names coalesce, invalid dates use the epoch, duplicates disappear,
    // and the date remains part of the key. Include both month bounds and misses.
    for (const auto& [buyer, date, channels] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {1000, 60, "ssscw"}, {1001, 64, "scw"}, {1002, 61, "sw"}, {1003, 61, "sc"},
            {1004, 75, "scw"}, {1005, 61, "ss"}, {1006, 62, "s"}, {1007, 63, "s"}, {1007, 62, "c"},
            {1008, 61, "sc"}, {1009, 63, "sw"}, {1010, 65, "scw"}, {1011, 66, "scw"},
            {1012, 999, "scw"}, {99999, 60, "scw"}}) {
        for (const char channel : TStringBuf(channels)) {
            auto& sales = channel == 's' ? store : channel == 'c' ? catalog : web;
            sales.push_back({960, order++, date, 1, nullptr, nullptr, nullptr, std::nullopt, nullptr, buyer});
        }
    }
    // Q16/Q94/Q95: warehouse multiplicity qualifies orders without multiplying
    // their measures. Three unreturned orders contribute shipping 3/profit 9;
    // two returned orders contribute shipping 5/profit 3. Invalid counterpart
    // dates still establish warehouse diversity, but never contribute amounts.
    i64 shippingItem = 10000;
    for (const bool online : {false, true}) {
        auto& sales = online ? web : catalog;
        const i64 base = online ? 200000 : 100000;
        const i64 first = online ? 84 : 80, last = first + 1, address = online ? 91 : 90;
        auto append = [&](i64 number, std::optional<i64> warehouse, std::optional<i64> date,
                std::optional<i64> addr, std::optional<i64> site, const char* cost, const char* profit) {
            sales.push_back({shippingItem++, base + number, 999, warehouse, nullptr, nullptr, nullptr,
                std::nullopt, profit, std::nullopt, date, addr, site, cost});
        };
        append(1, 1, first, address, 10, "1.25", "10");
        append(1, 2, first, address, 10, "2.75", "-3");
        append(1, 2, first, address, 10, nullptr, "2");
        append(2, 1, last, address, 10, "-1", nullptr);
        append(2, 2, last + 2, address, 10, "999", "999");
        append(3, 1, first, address, 10, "999", "999");
        append(3, 1, first, address, 10, "999", "999");
        append(4, 1, first, address, 10, "999", "999");
        append(4, std::nullopt, first, address, 10, "999", "999");
        append(5, 1, first, address, 10, "7.50", "-2");
        append(5, 2, std::nullopt, address, 10, "999", "999");
        append(6, 1, last, address, 10, nullptr, nullptr);
        append(6, 2, last, address, 10, nullptr, nullptr);
        append(7, 1, last, address, 10, "-2.50", "5");
        append(7, 2, last, address, 10, nullptr, nullptr);
        i64 number = 10;
        for (const auto& [date, addr, site] : std::initializer_list<std::tuple<std::optional<i64>, std::optional<i64>, std::optional<i64>>>{
                {first + 2, address, 10}, {last + 2, address, 10}, {5, address, 10}, {999, address, 10}, {std::nullopt, address, 10},
                {first, 92, 10}, {first, 93, 10}, {first, 999, 10}, {first, std::nullopt, 10},
                {first, address, 11}, {first, address, 12}, {first, address, 999}, {first, address, std::nullopt}}) {
            append(number, 1, date, addr, site, "999", "999");
            append(number++, 2, std::nullopt, address, 10, "999", "999");
        }
    }
    // Q36/Q86: hierarchy ranks, shared class names across categories, real
    // NULL keys and measures, negative/zero values, and month/year boundaries.
    for (const auto& [item, price, profit] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
            {970, "50", "5"}, {970, "50", "5"}, {971, "100", "30"}, {972, "100", "-10"},
            {973, "100", "30"}, {974, "50", "15"}, {975, nullptr, nullptr}, {976, "50", "0"}})
        store.push_back({item, order++, 70, 1, nullptr, nullptr, price, 90, profit});
    for (const auto& [item, date, paid] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {970, 60, "10"}, {970, 64, "5"}, {971, 61, "30"}, {972, 62, "-10"},
            {973, 63, "20"}, {974, 60, "7"}, {975, 60, nullptr}, {976, 60, "0"},
            {970, 65, "999"}, {970, 66, "999"}, {970, 4, "999"}, {970, 999, "999"}, {99999, 60, "999"}}) {
        TSale sale{item, order++, date, 1, nullptr, nullptr}; sale.NetPaid = paid; web.push_back(sale);
    }
    for (const auto& [date, shop, item] : std::initializer_list<std::tuple<i64, std::optional<i64>, i64>>{
            {73, 90, 970}, {60, 90, 970}, {4, 90, 970}, {999, 90, 970},
            {70, 91, 970}, {70, 92, 970}, {70, 999, 970}, {70, std::nullopt, 970}, {70, 90, 99999}})
        store.push_back({item, order++, date, 1, nullptr, nullptr, "999", shop, "999"});
    auto demographicSale = [&](i64 item, i64 date, std::optional<i64> shop, std::optional<i64> demo,
            std::optional<i64> quantity, const char* listPrice, const char* coupon, const char* price) {
        TSale sale{item, order++, date, 1, price, nullptr, nullptr, shop};
        sale.Quantity = quantity; sale.ListPrice = listPrice; sale.Coupon = coupon; sale.Demographics = demo;
        store.push_back(sale);
    };
    demographicSale(970, 90, 90, 90, 2, "10", "1", "5");
    demographicSale(970, 91, 90, 90, 6, "30", "3", "15");
    demographicSale(970, 90, 90, 90, std::nullopt, nullptr, nullptr, nullptr);
    demographicSale(971, 90, 90, 90, 10, "20", "2", "10");
    demographicSale(975, 90, 90, 90, std::nullopt, nullptr, nullptr, nullptr);
    for (const auto demo : std::initializer_list<std::optional<i64>>{91, 92, 93, 94, 999, std::nullopt})
        demographicSale(970, 90, 90, demo, 999, "999", "999", "999");
    for (const auto shop : std::initializer_list<std::optional<i64>>{91, 92, 999, std::nullopt})
        demographicSale(970, 90, shop, 90, 999, "999", "999", "999");
    for (const auto date : {92, 93, 4, 999}) demographicSale(970, date, 90, 90, 999, "999", "999", "999");
    demographicSale(99999, 90, 90, 90, 999, "999", "999", "999");
    // Q70: the IN lookup contains an uncorrelated window. New sales have no
    // matching item/customer and leave the earlier benchmark fixtures unchanged.
    // The existing TN sale in month 1212 contributes 999; NULL states must not
    // match the lookup, even though its window produces a NULL-state group.
    for (const auto& [date, shop, profit] : std::initializer_list<std::tuple<i64, std::optional<i64>, const char*>>{
            {60, 100, "2"}, {64, 100, "3"}, {61, 101, "10"}, {62, 102, "-4"},
            {63, 103, "0"}, {60, 104, nullptr}, {64, 91, "8"},
            {65, 100, "999"}, {66, 100, "999"}, {4, 100, "999"}, {999, 100, "999"},
            {60, 92, "999"}, {60, 999, "999"}, {60, std::nullopt, "999"}})
        store.push_back({99990, order++, date, 1, nullptr, nullptr, nullptr, shop, profit});
    // Q23: >4 frequent-item rows and >95% customer revenue are strict.
    // Its maximum uses the year filter; the best-customer sum uses all years.
    auto frequentSale = [&](i64 item, std::optional<i64> buyer, i64 date, std::optional<i64> quantity, const char* price) {
        TSale sale{item, order++, date, 1, price, nullptr}; sale.Customer = buyer; sale.Quantity = quantity;
        store.push_back(sale);
    };
    for (int i = 0; i < 5; ++i) frequentSale(1100, 1100, 73, 2, "10");
    frequentSale(1100, 1101, 73, 1, "96");
    frequentSale(1100, 1102, 73, 1, "95");
    frequentSale(1100, 1103, 73, std::nullopt, "999");
    for (int i = 0; i < 5; ++i) {
        frequentSale(1102, 1104, 92, 20, "10");
        frequentSale(1103, std::nullopt, 999, std::nullopt, nullptr);
        frequentSale(1104, std::nullopt, 73, std::nullopt, nullptr);
        if (i < 4) frequentSale(1101, std::nullopt, 73, std::nullopt, nullptr);
    }
    auto channelSale = [&](bool online, i64 item, std::optional<i64> buyer, std::optional<i64> date,
            std::optional<i64> quantity, const char* listPrice) {
        TSale sale{item, order++, date, 1, nullptr, nullptr};
        sale.Customer = buyer; sale.Quantity = quantity; sale.ListPrice = listPrice;
        (online ? web : catalog).push_back(sale);
    };
    channelSale(false, 1100, 1100, 73, 2, "10");
    channelSale(false, 1100, 1100, 73, 1, "10");
    channelSale(false, 1100, 1101, 73, 3, "10");
    channelSale(false, 1100, 1104, 73, 1, "7");
    channelSale(true, 1100, 1100, 73, 1, "4");
    channelSale(true, 1100, 1101, 73, 2, "5");
    channelSale(true, 1100, 1104, 73, 3, "2");
    channelSale(false, 1104, 1100, 73, std::nullopt, "100");
    channelSale(true, 1104, 1101, 73, 1, nullptr);
    for (const bool online : {false, true}) {
        for (const auto buyer : std::initializer_list<std::optional<i64>>{1102, 1103, 99999, std::nullopt})
            channelSale(online, 1100, buyer, 73, 9, "99");
        for (const i64 item : {1101, 1102, 1103, 99999}) channelSale(online, item, 1100, 73, 9, "99");
        for (const auto date : std::initializer_list<std::optional<i64>>{90, 70, 80, 4, 999, std::nullopt})
            channelSale(online, 1100, 1100, date, 9, "99");
    }
    // Q62/Q99 group on the computed warehouse substring. Two different full
    // names collapse to one key. Cover every bucket boundary and NULL delta,
    // NULL grouping fields, both month bounds and each missing dimension.
    for (const bool online : {false, true}) {
        auto shipment = [&](std::optional<i64> shipDate, std::optional<i64> delta,
                std::optional<i64> warehouse, std::optional<i64> mode, std::optional<i64> site) {
            TSale sale{99990, order++, shipDate && delta ? std::optional<i64>{*shipDate - *delta} : std::nullopt,
                warehouse, nullptr, nullptr};
            sale.ShipDate = shipDate; sale.ShipMode = mode; sale.Site = site;
            (online ? web : catalog).push_back(sale);
        };
        for (const i64 delta : {-1, 0, 30, 31, 60, 61, 90, 91, 120, 121}) shipment(60, delta, 90, 90, 90);
        shipment(64, 30, 91, 90, 90);
        shipment(60, std::nullopt, 90, 90, 90);
        shipment(60, std::nullopt, 92, 92, 92);
        shipment(64, 121, 92, 92, 92);
        shipment(60, 0, 93, 90, 92);
        shipment(60, 61, 93, 91, 91);
        for (const auto date : std::initializer_list<std::optional<i64>>{65, 66, 4, 999, std::nullopt}) shipment(date, 1, 90, 90, 90);
        for (const auto missing : std::initializer_list<std::optional<i64>>{999, std::nullopt}) {
            shipment(60, 1, missing, 90, 90);
            shipment(60, 1, 90, missing, 90);
            shipment(60, 1, 90, 90, missing);
        }
    }
    // Q47/Q57: rank offsets must mix Uint64 ranks with integer literals.
    // Duplicate January sales aggregate to 10; the 2000 mean is 25. Missing
    // adjacent ranks, exactly 10% deviation, zero/NULL means and NULL join keys
    // must not leak into the result. Existing benchmark fixtures remain intact.
    auto adjacentSale = [&](i64 item, i64 date, const char* price, bool nullLocation = false) {
        store.push_back({item, order++, date, 1, price, nullptr, nullptr, nullLocation ? 4 : 110});
        TSale sale{item, order++, date, 1, price, nullptr}; sale.Site = nullLocation ? 92 : 93;
        catalog.push_back(sale);
    };
    for (const auto& [date, price] : std::initializer_list<std::pair<i64, const char*>>{
            {94, "8"}, {70, "5"}, {70, "5"}, {71, "30"}, {72, "20"}, {65, "40"}})
        adjacentSale(1200, date, price);
    for (const auto& [item, first, second, last] : std::initializer_list<std::tuple<i64, const char*, const char*, const char*>>{
            {1201, "9", "11", "10"}, {1202, "-1", "1", "0"}, {1203, nullptr, nullptr, nullptr}}) {
        adjacentSale(item, 94, "2"); adjacentSale(item, 70, first); adjacentSale(item, 71, second); adjacentSale(item, 65, last);
    }
    adjacentSale(1206, 70, "3"); adjacentSale(1206, 71, "9");
    for (const i64 item : {1200, 1204, 1205}) for (const auto& [date, price] :
            std::initializer_list<std::pair<i64, const char*>>{{94, "1"}, {70, "10"}, {71, "30"}})
        adjacentSale(item, date, price, item == 1200);
    // Q90: five morning rows and three afternoon rows yield 5/(1+3)=1.25.
    // Exercise both hour/character-count bounds, duplicates, NULLs and each
    // missing dimension. Other web queries cannot join these item/date keys.
    auto hourlySale = [&](std::optional<i64> time, std::optional<i64> household, std::optional<i64> page) {
        TSale sale{99990, order++, 4, 1, nullptr, nullptr};
        sale.SoldTime = time; sale.ShipHousehold = household; sale.WebPage = page; web.push_back(sale);
    };
    for (const auto& [time, page] : std::initializer_list<std::pair<i64, i64>>{
            {1, 1}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {4, 2}}) hourlySale(time, 1, page);
    for (const auto time : std::initializer_list<std::optional<i64>>{5, 6, 7, 8, 9, 999, std::nullopt}) hourlySale(time, 1, 1);
    for (const i64 time : {1, 3}) {
        for (const auto household : std::initializer_list<std::optional<i64>>{2, 3, 4, 999, std::nullopt}) hourlySale(time, household, 1);
        for (const auto page : std::initializer_list<std::optional<i64>>{3, 4, 5, 999, std::nullopt}) hourlySale(time, 1, page);
    }
    // Q54: duplicate customers across both channels must not multiply revenue;
    // the two March dates require DISTINCT in both scalar month subqueries.
    // The original SQL joins stores by address county/state, not ss_store_sk.
    auto segmentCustomer = [&](std::optional<i64> buyer, i64 item, std::optional<i64> date, bool online = false) {
        TSale sale{item, order++, date, 1, nullptr, nullptr}; sale.Customer = buyer;
        (online ? web : catalog).push_back(sale);
    };
    for (i64 buyer = 1300; buyer <= 1313; ++buyer) segmentCustomer(buyer, 1300, 95, buyer % 2);
    segmentCustomer(1300, 1300, 95); segmentCustomer(1300, 1300, 96, true);
    for (i64 buyer = 1314; buyer <= 1317; ++buyer) segmentCustomer(buyer, 1301 + buyer - 1314, 95);
    segmentCustomer(1318, 99990, 95); segmentCustomer(1319, 1300, 80); segmentCustomer(1320, 1300, 999);
    segmentCustomer(1321, 1300, std::nullopt); segmentCustomer(1322, 1300, 97);
    segmentCustomer(99999, 1300, 95); segmentCustomer(std::nullopt, 1300, 95);
    auto segmentRevenue = [&](std::optional<i64> buyer, i64 date, const char* revenue) {
        TSale sale{99990, order++, date, 1, nullptr, nullptr, revenue, 999}; sale.Customer = buyer;
        store.push_back(sale);
    };
    for (const auto& [buyer, date, revenue] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {1300, 81, "50"}, {1300, 86, "75"}, {1300, 95, "999"}, {1300, 87, "999"},
            {1301, 84, "125"}, {1302, 85, "175"}, {1303, 81, "-125"}, {1304, 81, nullptr},
            {1305, 81, "0"}, {1306, 85, "75"}, {1307, 87, "999"}, {1308, 96, "999"}})
        segmentRevenue(buyer, date, revenue);
    for (i64 buyer = 1309; buyer <= 1322; ++buyer) segmentRevenue(buyer, 81, "999");
    segmentRevenue(99999, 81, "999"); segmentRevenue(std::nullopt, 81, "999");
    // Q24: ten non-null grouped amounts total 1000, so AVG is 100 and the
    // strict 5% threshold is 5. Red/NULL colors affect that scalar average.
    // Unicode equality, all join keys and NULLs are exercised before grouping.
    TValueBuilder storeReturns; storeReturns.BeginList();
    auto returnedSale = [&](i64 item, i64 ticket, std::optional<i64> quantity = std::nullopt,
            std::optional<i64> customer = std::nullopt, std::optional<i64> date = std::nullopt, const char* loss = nullptr,
            std::optional<i64> shop = std::nullopt, const char* fee = nullptr) {
        storeReturns.AddListItem().BeginStruct().AddMember("sr_item_sk").Int64(item).AddMember("sr_ticket_number").Int64(ticket)
            .AddMember("sr_return_quantity").OptionalInt64(quantity).AddMember("sr_customer_sk").OptionalInt64(customer)
            .AddMember("sr_returned_date_sk").OptionalInt64(date).AddMember("sr_net_loss");
        decimal(storeReturns, loss);
        storeReturns.AddMember("sr_store_sk").OptionalInt64(shop).AddMember("sr_fee"); decimal(storeReturns, fee);
        storeReturns.EndStruct();
    };
    auto caseSale = [&](std::optional<i64> buyer, i64 item, const char* price, std::optional<i64> shop = 140, bool returned = true) {
        const auto ticket = order++;
        store.push_back({item, ticket, std::nullopt, 1, price, nullptr, nullptr, shop, nullptr, buyer});
        if (returned) returnedSale(item, ticket);
        return ticket;
    };
    const auto firstCaseTicket = caseSale(1400, 1400, "5");
    returnedSale(1401, firstCaseTicket); // Same ticket, wrong item must not multiply the sale.
    caseSale(1400, 1400, "5"); caseSale(1401, 1401, "20");
    caseSale(1402, 1400, "50"); caseSale(1402, 1400, nullptr);
    for (const auto& [buyer, price] : std::initializer_list<std::pair<i64, const char*>>{
            {1403, "5"}, {1404, "4"}, {1405, "0"}, {1406, "-10"}, {1407, nullptr}, {1408, "21"}})
        caseSale(buyer, 1400, price);
    caseSale(1409, 1402, "800"); caseSale(1417, 1403, "100");
    for (i64 buyer = 1410; buyer <= 1416; ++buyer) caseSale(buyer, 1400, "999");
    for (const auto shop : std::initializer_list<std::optional<i64>>{141, 142, 143, 144, 99999, std::nullopt})
        caseSale(1400, 1400, "999", shop);
    caseSale(99999, 1400, "999"); caseSale(std::nullopt, 1400, "999"); caseSale(1400, 99990, "999");
    caseSale(1400, 1400, "999", 140, false);
    // Q78 needs only fact tables and dates. Unrelated dimension joins exclude
    // these new item/customer IDs from the earlier populated fixtures.
    auto unreturnedSale = [&](TVector<TSale>& channel, i64 item, std::optional<i64> quantity,
            const char* cost, const char* price, std::optional<i64> buyer = 20000,
            std::optional<i64> date = 70, i64 ticket = 0) {
        TSale sale{item, ticket ? ticket : order++, date, std::nullopt, price, nullptr};
        sale.Customer = buyer; sale.Quantity = quantity; sale.WholesaleCost = cost;
        channel.push_back(sale);
    };
    unreturnedSale(store, 20000, 8, "10", "15", 20000, 70, 500000);
    unreturnedSale(store, 20000, 4, "20", "25", 20000, 71, 500001);
    unreturnedSale(store, 20000, 999, "999", "999", 20000, 70, 500002);
    unreturnedSale(web, 20000, 2, "1", "3", 20000, 70, 600000);
    unreturnedSale(web, 20000, 1, "2", "4", 20000, 72, 600001);
    unreturnedSale(web, 20000, 999, "999", "999", 20000, 70, 600002);
    unreturnedSale(catalog, 20000, 2, "5", "6", 20000, 70, 700000);
    unreturnedSale(catalog, 20000, 999, "999", "999", 20000, 70, 700001);
    returnedSale(20000, 500002); returnedSale(20099, 500000); returnedSale(20000, 500099);
    unreturnedSale(store, 20001, 9, nullptr, nullptr); unreturnedSale(web, 20001, 3, nullptr, nullptr);
    unreturnedSale(store, 20002, 5, "1", "2"); unreturnedSale(catalog, 20002, 2, "4", "6");
    unreturnedSale(store, 20003, std::nullopt, "-1", "-2");
    unreturnedSale(web, 20003, 1, "-3", "-4"); unreturnedSale(catalog, 20003, 0, "5", "6");
    unreturnedSale(store, 20004, 7, "1", "2");
    unreturnedSale(web, 20004, 1, "1", "1"); unreturnedSale(catalog, 20004, -1, "2", "2");
    for (i64 item = 20005; item <= 20009; ++item)
        unreturnedSale(store, item, 999, "999", "999", item == 20008 ? std::nullopt : std::optional<i64>{20000});
    // No competing channel, nonpositive or NULL quantities, and NULL/mismatched
    // customer keys must fail the outer WHERE. The zero denominator above passes it.
    unreturnedSale(web, 20006, -1, "999", "999"); unreturnedSale(catalog, 20006, -2, "999", "999");
    unreturnedSale(web, 20007, std::nullopt, "999", "999"); unreturnedSale(catalog, 20007, std::nullopt, "999", "999");
    unreturnedSale(web, 20008, 999, "999", "999", std::nullopt);
    unreturnedSale(web, 20009, 999, "999", "999", 20001);
    for (const auto& [item, date] : std::initializer_list<std::pair<i64, std::optional<i64>>>{{20010, 73}, {20011, std::nullopt}, {20012, 99999}}) {
        unreturnedSale(store, item, 999, "999", "999", 20000, date);
        unreturnedSale(web, item, 999, "999", "999", 20000, date);
    }
    unreturnedSale(web, 20000, 999, "999", "999", 20001); // same year/item, different customer
    unreturnedSale(web, 20000, 999, "999", "999", 20000, 73); // same item/customer, different year
    unreturnedSale(web, 20099, 999, "999", "999"); // same year/customer, different item
    // Q17: store/return tickets join to two catalog rows. The ten joined
    // quantities have exact means 1/2/5 and sample deviations 2/4/0.
    // Separate groups cover singleton NaNs, all-NULL measures and zero means.
    auto joinedStoreSale = [&](i64 item, std::optional<i64> quantity, std::optional<i64> buyer = 2000,
            std::optional<i64> date = 180, std::optional<i64> shop = 1) {
        const auto ticket = order++;
        TSale sale{item, ticket, date, std::nullopt, nullptr, nullptr};
        sale.Quantity = quantity; sale.Customer = buyer; sale.Store = shop;
        store.push_back(sale);
        return ticket;
    };
    auto joinedCatalogSale = [&](i64 item, std::optional<i64> quantity, std::optional<i64> date,
            std::optional<i64> buyer = 2000) {
        TSale sale{item, order++, date, std::nullopt, nullptr, nullptr};
        sale.Quantity = quantity; sale.Customer = buyer;
        catalog.push_back(sale);
    };
    i64 quarter = 0;
    for (const i64 quantity : {-2, 1, 1, 1, 4}) {
        const auto ticket = joinedStoreSale(1900, quantity);
        returnedSale(1900, ticket, quantity * 2, 2000, 180 + quarter++ % 3);
    }
    joinedCatalogSale(1900, 5, 181); joinedCatalogSale(1900, 5, 182);
    const auto singleton = joinedStoreSale(1901, 7, 2000, 180, 90);
    returnedSale(1901, singleton, std::nullopt, 2000, 180);
    joinedCatalogSale(1901, 9, 180);
    for (const i64 quantity : {1, 3, 5}) {
        const auto ticket = joinedStoreSale(1902, std::nullopt);
        returnedSale(1902, ticket, quantity, 2000, 180 + quantity / 2);
    }
    joinedCatalogSale(1902, 0, 181);
    for (const auto date : std::initializer_list<std::optional<i64>>{181, 182, 183, 184, 90, 99999, std::nullopt}) {
        const auto ticket = joinedStoreSale(1900, 999, 2000, date);
        returnedSale(1900, ticket, 999, 2000, 180);
    }
    for (const auto date : std::initializer_list<std::optional<i64>>{183, 184, 90, 99999, std::nullopt}) {
        const auto ticket = joinedStoreSale(1900, 999);
        returnedSale(1900, ticket, 999, 2000, date);
        joinedCatalogSale(1900, 999, date);
    }
    joinedStoreSale(1900, 999); // No matching return ticket.
    const auto wrongCustomer = joinedStoreSale(1900, 999);
    returnedSale(1900, wrongCustomer, 999, 2001, 180);
    joinedCatalogSale(1900, 999, 180, 2001);
    const auto wrongItem = joinedStoreSale(1900, 999);
    returnedSale(39999, wrongItem, 999, 2000, 180);
    joinedCatalogSale(39999, 999, 180);
    const auto missingItem = joinedStoreSale(39999, 999);
    returnedSale(39999, missingItem, 999, 2000, 180);
    for (const auto shop : std::initializer_list<std::optional<i64>>{99999, std::nullopt}) {
        const auto ticket = joinedStoreSale(1900, 999, 2000, 180, shop);
        returnedSale(1900, ticket, 999, 2000, 180);
    }
    const auto nullCustomer = joinedStoreSale(1900, 999, std::nullopt);
    returnedSale(1900, nullCustomer, 999, std::nullopt, 180);
    joinedCatalogSale(1900, 999, 180, std::nullopt);
    // Q25/Q29 share the three-fact join but use different date bounds and
    // Decimal profits / integer quantities. Every catalog row multiplies the
    // matching tickets; equal store names must not collapse distinct IDs.
    for (const bool profits : {true, false}) {
        const i64 item = profits ? 2100 : 2200, buyer = profits ? 21000 : 22000;
        const i64 soldDate = profits ? 191 : 201, lastReturn = profits ? 194 : 202;
        auto sale = [&](i64 product, std::optional<i64> quantity, const char* profit,
                std::optional<i64> date, std::optional<i64> shop = 1, std::optional<i64> customer = std::nullopt) {
            const auto ticket = joinedStoreSale(product, profits ? std::nullopt : quantity,
                customer, date, shop);
            store.back().NetProfit = profits ? profit : nullptr;
            return ticket;
        };
        auto catalogSale = [&](i64 product, std::optional<i64> quantity, const char* profit,
                std::optional<i64> date, std::optional<i64> customer) {
            joinedCatalogSale(product, profits ? std::nullopt : quantity, date, customer);
            catalog.back().NetProfit = profits ? profit : nullptr;
        };
        auto pair = [&](i64 product, std::optional<i64> quantity, std::optional<i64> returned,
                const char* profit, const char* loss, i64 returnedDate, i64 shop = 1) {
            const auto ticket = sale(product, quantity, profit, soldDate, shop, buyer);
            returnedSale(product, ticket, profits ? std::nullopt : returned, buyer, returnedDate, profits ? loss : nullptr);
        };
        pair(item, 1, 2, "1.25", "2", soldDate);
        pair(item, 3, -1, "-2.50", "-0.50", lastReturn);
        pair(item, std::nullopt, std::nullopt, nullptr, nullptr, soldDate);
        pair(item, 7, 5, "5", "1", lastReturn, 2);
        catalogSale(item, 4, "3.50", profits ? 191 : 73, buyer);
        catalogSale(item, -1, "-1", profits ? 193 : 70, buyer);
        catalogSale(item, std::nullopt, nullptr, profits ? 194 : 64, buyer);
        for (int i = 0; i < 2; ++i) pair(item + 1, std::nullopt, std::nullopt, nullptr, nullptr, soldDate, 4);
        catalogSale(item + 1, std::nullopt, nullptr, soldDate, buyer);
        pair(item + 2, 9000000000000LL, 1, "99999.99", "0.01", soldDate);
        pair(item + 2, -9000000000001LL, -1, "99999.99", "-0.01", lastReturn);
        catalogSale(item + 2, std::nullopt, nullptr, soldDate, buyer);
        catalogSale(item + 2, 0, "0.01", profits ? 194 : 64, buyer);
        for (const auto date : std::initializer_list<std::optional<i64>>{
                profits ? 190 : 200, profits ? 192 : 202, profits ? 195 : 203,
                profits ? 201 : 191, 62, 90, 4, 99999, std::nullopt}) {
            const auto ticket = sale(item, 999, "999", date, 1, buyer);
            returnedSale(item, ticket, 999, buyer, soldDate, "999");
        }
        for (const auto date : std::initializer_list<std::optional<i64>>{
                profits ? 190 : 200, profits ? 195 : 203, profits ? 201 : 191, 62, 90, 4, 99999, std::nullopt}) {
            const auto ticket = sale(item, 999, "999", soldDate, 1, buyer);
            returnedSale(item, ticket, 999, buyer, date, "999");
        }
        for (const auto date : (profits ? TVector<std::optional<i64>>{190, 195, 201, 62, 90, 4, 99999, std::nullopt}
                                       : TVector<std::optional<i64>>{90, 66, 4, 99999, std::nullopt}))
            catalogSale(item, 999, "999", date, buyer);
        const auto wrongTicket = sale(item, 999, "999", soldDate, 1, buyer);
        returnedSale(item, wrongTicket + 1000000, 999, buyer, soldDate, "999");
        const auto wrongCustomer = sale(item, 999, "999", soldDate, 1, buyer);
        returnedSale(item, wrongCustomer, 999, buyer + 1, soldDate, "999");
        catalogSale(item, 999, "999", soldDate, buyer + 1);
        const auto wrongItem = sale(item, 999, "999", soldDate, 1, buyer);
        returnedSale(39998, wrongItem, 999, buyer, soldDate, "999");
        const auto missingItem = sale(39998, 999, "999", soldDate, 1, buyer);
        returnedSale(39998, missingItem, 999, buyer, soldDate, "999");
        catalogSale(39998, 999, "999", soldDate, buyer);
        for (const auto shop : std::initializer_list<std::optional<i64>>{99999, std::nullopt}) {
            const auto ticket = sale(item, 999, "999", soldDate, shop, buyer);
            returnedSale(item, ticket, 999, buyer, soldDate, "999");
        }
        const auto nullCustomer = sale(item, 999, "999", soldDate);
        returnedSale(item, nullCustomer, 999, std::nullopt, soldDate, "999");
        catalogSale(item, 999, "999", soldDate, std::nullopt);
    }
    // Q1: TN store 90 has seven non-NULL customer totals summing to 70,
    // hence the strict threshold is 12. A missing customer participates in
    // that mean but cannot appear in the output. Store 180 has a different
    // mean; the same winning customer must occur twice (there is no DISTINCT).
    auto returnFee = [&](std::optional<i64> buyer, std::optional<i64> shop, const char* fee,
            std::optional<i64> date = 70) { returnedSale(39997, order++, std::nullopt, buyer, date, nullptr, shop, fee); };
    for (const auto& [buyer, fee] : std::initializer_list<std::pair<std::optional<i64>, const char*>>{
            {2500, "10"}, {2500, "20"}, {2500, nullptr}, {2501, "20"}, {2502, "12"}, {2503, nullptr},
            {2504, "-12"}, {2505, "0"}, {2506, "-20"}, {99999, "40"}, {std::nullopt, nullptr}})
        returnFee(buyer, 90, fee);
    returnFee(2500, 180, "50"); returnFee(2500, 180, "50", 65); returnFee(2507, 180, "0");
    returnFee(2509, 181, "-10"); returnFee(2508, 181, "-30"); returnFee(2510, 182, nullptr);
    for (const auto shop : std::initializer_list<std::optional<i64>>{91, 92, 99999, std::nullopt}) {
        returnFee(2508, shop, "100"); returnFee(2509, shop, "0");
    }
    for (const auto date : std::initializer_list<std::optional<i64>>{73, 60, 4, 99999, std::nullopt}) returnFee(2502, 90, "999", date);
    load("store_returns", storeReturns);
    // Q66 divides each channel's aggregate before the outer sum. Round's
    // .01/2 + .01/2 is zero; dividing the combined .02 would produce .01.
    // Distinct warehouse IDs with identical attributes must form one group.
    auto monthlySale = [&](bool online, std::optional<i64> shop, std::optional<i64> date,
            std::optional<i64> time, std::optional<i64> mode, const char* price,
            std::optional<i64> quantity, const char* paid) {
        TSale sale{99990, order++, date, shop, online ? price : "999", nullptr, online ? "999" : price};
        sale.SoldTime = time; sale.ShipMode = mode; sale.Quantity = quantity; sale.NetPaidTax = paid;
        (online ? web : catalog).push_back(sale);
    };
    for (i64 date = 100; date <= 111; ++date) {
        monthlySale(true, 150, date, 150, 150, "0.02", 1, "0.20");
        monthlySale(true, 151, date, 151, 151, "-0.01", 1, "-0.10");
        monthlySale(false, 150, date, 151, 151, "0.01", 1, "0.10");
        monthlySale(true, 152, date, 150, 150, "0.02", 1, "0.20");
        monthlySale(false, 152, date, 151, 151, "0.04", 1, "0.40");
        for (const i64 shop : {153, 155}) {
            monthlySale(true, shop, date, 150, 150, "0.01", 1, "0.10");
            monthlySale(false, shop, date, 151, 151, "0.02", 1, "0.20");
        }
        for (const bool online : {false, true}) monthlySale(online, 154, date, 150, 150, "0", 1, "0");
    }
    for (const bool online : {false, true}) {
        monthlySale(online, 156, 100, 150, 150, nullptr, 1, nullptr);
        monthlySale(online, 156, 100, 150, 150, "999", std::nullopt, "999");
        monthlySale(online, 150, 100, 150, 150, nullptr, 1, nullptr);
        monthlySale(online, 150, 100, 150, 150, "999", std::nullopt, "999");
        for (const auto date : std::initializer_list<std::optional<i64>>{70, 99999, std::nullopt})
            monthlySale(online, 150, date, 150, 150, "999", 1, "999");
        for (const auto time : std::initializer_list<std::optional<i64>>{152, 153, 154, 99999, std::nullopt})
            monthlySale(online, 150, 100, time, 150, "999", 1, "999");
        for (const auto mode : std::initializer_list<std::optional<i64>>{152, 153, 99999, std::nullopt})
            monthlySale(online, 150, 100, 150, mode, "999", 1, "999");
        for (const auto shop : std::initializer_list<std::optional<i64>>{99999, std::nullopt})
            monthlySale(online, shop, 100, 150, 150, "999", 1, "999");
    }
    monthlySale(true, 157, 105, 150, 150, "2", 3, "4");
    monthlySale(false, 157, 105, 151, 151, "5", 2, "6");
    // Q88: eight independent scalar counts are cross-joined under SELECT *.
    // Exercise all household branches and both sides of each half-hour bound.
    auto halfHourSale = [&](std::optional<i64> time, std::optional<i64> household, std::optional<i64> shop = 160) {
        TSale sale{99990, order++, std::nullopt, std::nullopt, nullptr, nullptr};
        sale.SoldTime = time; sale.Household = household; sale.Store = shop;
        store.push_back(sale);
    };
    for (i64 bucket = 0; bucket < 8; ++bucket) for (i64 row = 0; row <= bucket; ++row)
        halfHourSale(bucket == 1 && row == 0 ? 174 : 160 + bucket, 160 + (bucket + row) % 3);
    for (const auto time : std::initializer_list<std::optional<i64>>{168, 169, 170, 171, 172, 173, 99999, std::nullopt})
        halfHourSale(time, 160);
    for (const auto household : std::initializer_list<std::optional<i64>>{163, 164, 165, 166, 167, 168, 169, 170, 99999, std::nullopt})
        halfHourSale(160, household);
    for (const auto shop : std::initializer_list<std::optional<i64>>{1, 99999, std::nullopt}) halfHourSale(160, 160, shop);
    // Q28: all six quantity ranges, duplicate and NULL prices, every OR arm,
    // inclusive bounds and Decimal rounding. NULL dimension keys keep these
    // fact-only rows out of the earlier benchmark joins.
    auto averageBucketSale = [&](std::optional<i64> quantity, const char* price, const char* coupon = nullptr, const char* cost = nullptr) {
        TSale sale{30000, order++, std::nullopt, std::nullopt, nullptr, nullptr};
        sale.Quantity = quantity; sale.ListPrice = price; sale.Coupon = coupon; sale.WholesaleCost = cost;
        store.push_back(sale);
    };
    struct TAverageBucket {
        i64 Low, High;
        const char *PriceLow, *PriceHigh, *PriceBelow, *PriceAbove;
        const char *CouponLow, *CouponHigh, *CouponBelow, *CouponAbove;
        const char *CostLow, *CostHigh, *CostBelow, *CostAbove;
    };
    for (const auto& bucket : std::initializer_list<TAverageBucket>{
            {0, 5, "11", "21", "10.99", "21.01",
                "460", "1460", "459.99", "1460.01",
                "14", "34", "13.99", "34.01"},
            {6, 10, "91", "101", "90.99", "101.01",
                "1430", "2430", "1429.99", "2430.01",
                "32", "52", "31.99", "52.01"},
            {11, 15, "66", "76", "65.99", "76.01",
                "920", "1920", "919.99", "1920.01",
                "4", "24", "3.99", "24.01"},
            {16, 20, "142", "152", "141.99", "152.01",
                "3054", "4054", "3053.99", "4054.01",
                "80", "100", "79.99", "100.01"},
            {21, 25, "135", "145", "134.99", "145.01",
                "14180", "15180", "14179.99", "15180.01",
                "38", "58", "37.99", "58.01"},
            {26, 30, "28", "38", "27.99", "38.01",
                "2513", "3513", "2512.99", "3513.01",
                "42", "62", "41.99", "62.01"}}) {
        averageBucketSale(bucket.Low, bucket.PriceLow);
        averageBucketSale(bucket.High, bucket.PriceHigh);
        averageBucketSale(bucket.Low + 1, bucket.PriceLow);
        averageBucketSale(bucket.Low, "0", bucket.CouponLow);
        averageBucketSale(bucket.High, "2.50", nullptr, bucket.CostHigh);
        averageBucketSale(bucket.Low + 2, nullptr, bucket.CouponHigh);
        averageBucketSale(bucket.High, nullptr, nullptr, bucket.CostLow);
        averageBucketSale(bucket.Low, bucket.PriceBelow, bucket.CouponBelow, bucket.CostBelow);
        averageBucketSale(bucket.High, bucket.PriceAbove, bucket.CouponAbove, bucket.CostAbove);
        averageBucketSale(bucket.Low - 1, bucket.PriceLow);
        averageBucketSale(bucket.High + 1, bucket.PriceHigh);
        averageBucketSale(std::nullopt, bucket.PriceLow, bucket.CouponLow, bucket.CostLow);
    }
    averageBucketSale(2, "13.01");                         // 58.51 / 6 -> 9.75
    averageBucketSale(8, "57.19", "1430");               // 342.69 / 6 -> 57.12 (odd tie)
    averageBucketSale(13, "42.13", "920");               // 252.63 / 6 -> 42.10 (even tie)
    averageBucketSale(18, "-99999.99", nullptr, "80");    // -99561.49 / 6 -> -16593.58
    averageBucketSale(28, "99999.99", "3513");           // 100096.49 / 6 -> 16682.75
    auto loadSales = [&](const char* table, const std::string& prefix, const char* number, const TVector<TSale>& data) {
        TValueBuilder rows;
        rows.BeginList();
        for (const auto& row : data) {
            rows.AddListItem().BeginStruct().AddMember(prefix + "item_sk").Int64(row.Item)
                .AddMember(number).Int64(row.Order).AddMember(prefix + "sold_date_sk").OptionalInt64(row.Date);
            if (prefix != "ss_") {
                rows.AddMember(prefix + "warehouse_sk").OptionalInt64(row.Warehouse)
                    .AddMember(prefix + "ship_date_sk").OptionalInt64(row.ShipDate)
                    .AddMember(prefix + "ship_mode_sk").OptionalInt64(row.ShipMode)
                    .AddMember(prefix + "ship_addr_sk").OptionalInt64(row.ShipAddress)
                    .AddMember(prefix + (prefix == "cs_" ? "call_center_sk" : "web_site_sk")).OptionalInt64(row.Site)
                    .AddMember(prefix + "ext_ship_cost");
                decimal(rows, row.ShipCost);
                rows.AddMember(prefix + (prefix == "ws_" ? "net_paid_inc_tax" : "net_paid_inc_ship_tax")); decimal(rows, row.NetPaidTax);
                if (prefix == "cs_") {
                    rows.AddMember("cs_sold_time_sk").OptionalInt64(row.SoldTime)
                        .AddMember("cs_bill_cdemo_sk").OptionalInt64(row.Demographics).AddMember("cs_promo_sk").OptionalInt64(row.Promotion);
                    rows.AddMember("cs_coupon_amt"); decimal(rows, row.Coupon);
                }
            }
            if (prefix == "ss_") {
                rows.AddMember("ss_store_sk").OptionalInt64(row.Store)
                    .AddMember("ss_cdemo_sk").OptionalInt64(row.Demographics).AddMember("ss_addr_sk").OptionalInt64(row.Address)
                    .AddMember("ss_sold_time_sk").OptionalInt64(row.SoldTime).AddMember("ss_hdemo_sk").OptionalInt64(row.Household)
                    .AddMember("ss_promo_sk").OptionalInt64(row.Promotion);
                rows.AddMember("ss_coupon_amt"); decimal(rows, row.Coupon);
                rows.AddMember("ss_ext_wholesale_cost"); decimal(rows, row.ExtendedCost);
            }
            rows.AddMember(prefix + "quantity").OptionalInt64(row.Quantity).AddMember(prefix + "list_price"); decimal(rows, row.ListPrice);
            if (prefix == "ws_") {
                rows.AddMember("ws_net_paid"); decimal(rows, row.NetPaid);
                rows.AddMember("ws_sold_time_sk").OptionalInt64(row.SoldTime).AddMember("ws_ship_hdemo_sk").OptionalInt64(row.ShipHousehold)
                    .AddMember("ws_web_page_sk").OptionalInt64(row.WebPage);
            }
            rows.AddMember(prefix + (prefix == "ss_" ? "customer_sk" : "bill_customer_sk")).OptionalInt64(row.Customer);
            rows.AddMember(prefix + "sales_price"); decimal(rows, row.Price);
            rows.AddMember(prefix + "wholesale_cost"); decimal(rows, row.WholesaleCost);
            rows.AddMember(prefix + "ext_discount_amt"); decimal(rows, row.Discount);
            rows.AddMember(prefix + "ext_sales_price"); decimal(rows, row.ExtendedPrice);
            rows.AddMember(prefix + "net_profit"); decimal(rows, row.NetProfit);
            rows.EndStruct();
        }
        load(table, rows);
    };
    // Revenue queries use different manager/year predicates. Cover both ends
    // of December, NULL/negative sums, sort ties, and rejected join/filter rows.
    // Other fact measures and foreign keys remain NULL on these new rows.
    for (const i64 first : {2300, 2400}) {
        const bool recent = first == 2400;
        const i64 firstDate = recent ? 213 : 210, lastDate = recent ? 64 : 91;
        auto sale = [&](i64 item, std::optional<i64> date, const char* amount) {
            store.push_back({item, order++, date, std::nullopt, nullptr, nullptr, amount});
        };
        for (const auto& [offset, amount] : std::initializer_list<std::pair<i64, const char*>>{
                {0, "10.25"}, {0, "-2.50"}, {0, nullptr}, {1, "5.25"}, {2, "13"}, {3, "11"},
                {4, "-4.50"}, {5, "1.25"}, {5, "-1.25"}, {6, nullptr}, {6, nullptr}, {7, "3"}})
            sale(first + offset, offset % 2 ? lastDate : firstDate, amount);
        for (const auto date : std::initializer_list<std::optional<i64>>{
                recent ? 214 : 211, recent ? 215 : 212, recent ? 210 : 213, 4, 99999, std::nullopt})
            sale(first, date, "999");
        sale(first + 8, firstDate, "999"); // Wrong manager.
        sale(first + 9, firstDate, "999"); // NULL manager.
        sale(first + 99, firstDate, "999"); // Missing item.
    }
    auto brandSale = [&](i64 item, std::optional<i64> date, const char* amount) {
        store.push_back({item, order++, date, std::nullopt, nullptr, nullptr, amount});
    };
    for (const auto& [offset, amount] : std::initializer_list<std::pair<i64, const char*>>{
            {0, "10.25"}, {0, "-2.50"}, {0, nullptr}, {1, "5.25"}, {2, "13"}, {3, "11"},
            {4, "-4.50"}, {5, "1.25"}, {5, "-1.25"}, {6, nullptr}, {6, nullptr}, {7, "3"}})
        brandSale(2600 + offset, offset % 2 ? 91 : 210, amount);
    for (const auto& [offset, amount] : std::initializer_list<std::pair<i64, const char*>>{{0, "7"}, {0, "3"}, {2, "20"}, {6, nullptr}})
        brandSale(2600 + offset, 212, amount);
    for (const auto date : std::initializer_list<std::optional<i64>>{211, 93, 4, 99999, std::nullopt}) brandSale(2600, date, "999");
    for (const i64 item : {2608, 2609, 2699}) brandSale(item, 210, "999");
    // Q7: both promotion OR arms, including NULL/true, repeated rows and
    // two physical items with the same output item ID. Quantities outside
    // Q28's buckets keep its fact-only AVG expectations unchanged.
    const auto promotionBegin = store.size();
    auto promotionSale = [&](i64 item, std::optional<i64> promotion, std::optional<i64> quantity,
            const char* list, const char* coupon, const char* price, std::optional<i64> demo = 90, std::optional<i64> date = 90) {
        TSale sale{item, order++, date, std::nullopt, price, nullptr};
        sale.Quantity = quantity; sale.ListPrice = list; sale.Coupon = coupon;
        sale.Demographics = demo; sale.Promotion = promotion; store.push_back(sale);
    };
    for (int i = 0; i < 2; ++i) {
        promotionSale(2700, 2700, 40, "1000.01", "1", "2");
        promotionSale(2700, 2701, 80, "1000.02", "3", "4", 90, 91);
    }
    promotionSale(3700, 2703, -90, nullptr, "7", nullptr);
    promotionSale(2700, 2704, std::nullopt, nullptr, nullptr, nullptr);
    promotionSale(2701, 2702, std::nullopt, nullptr, "-1", "10");
    promotionSale(2701, 2704, std::nullopt, nullptr, "1", "20");
    promotionSale(2702, 2703, std::nullopt, nullptr, nullptr, nullptr);
    promotionSale(2703, 2700, -40, "-10", "-2", "-1"); promotionSale(2703, 2700, 0, "0", "0", "1");
    promotionSale(2704, 2700, 40, "99999.99", "99999.99", "99999.99");
    for (const auto promotion : std::initializer_list<std::optional<i64>>{2705, 2706, 2707, 2708, 99999, std::nullopt})
        promotionSale(2700, promotion, 999, "999", "999", "999");
    for (const auto demo : std::initializer_list<std::optional<i64>>{91, 92, 93, 94, 99999, std::nullopt})
        promotionSale(2700, 2700, 999, "999", "999", "999", demo);
    for (const auto date : std::initializer_list<std::optional<i64>>{92, 93, 4, 99999, std::nullopt})
        promotionSale(2700, 2700, 999, "999", "999", "999", 90, date);
    promotionSale(2799, 2700, 999, "999", "999", "999");
    // Q26 applies the same demographic/promotion contract to catalog sales.
    // Reuse the complete Q7 data, including its rejected rows and NULL cases.
    catalog.insert(catalog.end(), store.begin() + promotionBegin, store.end());
    // Q19's ZIP comparison uses offset one and five bytes, not the entire
    // strings. Keep all manufacturer/brand grouping fields independently live.
    auto zipSale = [&](i64 item, const char* amount, std::optional<i64> buyer = 2800,
            std::optional<i64> shop = 190, std::optional<i64> date = 216) {
        store.push_back({item, order++, date, std::nullopt, nullptr, nullptr, amount, shop, nullptr, buyer});
    };
    zipSale(2800, "10.25"); zipSale(2800, "-2.50"); zipSale(2800, nullptr);
    zipSale(2801, "5.25", 2800, 190, 217); zipSale(2802, "13"); zipSale(2803, "13", 2804);
    zipSale(2804, "3", 2803, 194); zipSale(2805, "-4.50"); zipSale(2806, nullptr);
    for (const auto buyer : std::initializer_list<std::optional<i64>>{2801, 2802, 2805, 2806, 99999, std::nullopt}) zipSale(2800, "999", buyer);
    for (const auto shop : std::initializer_list<std::optional<i64>>{193, 99999, std::nullopt}) zipSale(2800, "999", 2800, shop);
    zipSale(2800, "999", 2804, 192); // Different strings, equal five-byte ZIPs.
    for (const auto date : std::initializer_list<std::optional<i64>>{218, 219, 220, 4, 99999, std::nullopt}) zipSale(2800, "999", 2800, 190, date);
    for (const i64 item : {2807, 2808, 2899}) zipSale(item, "999");
    // Q15 admits rows through ZIP, state or a strict price threshold. Equal
    // ZIPs on different addresses group together; NULL ZIPs can still qualify.
    auto zipCatalogSale = [&](std::optional<i64> buyer, const char* amount, std::optional<i64> date = 191) {
        catalog.push_back({39996, order++, date, std::nullopt, amount, nullptr, nullptr, std::nullopt, nullptr, buyer});
    };
    for (const auto& [buyer, amount] : std::initializer_list<std::pair<i64, const char*>>{
            {2900, "10"}, {2900, "-2.50"}, {2900, nullptr}, {2902, "5.25"}, {2901, "4"}, {2903, "7"},
            {2904, "500.01"}, {2904, "500"}, {2904, nullptr}, {2905, "2.50"}, {2906, nullptr},
            {2907, "-4.50"}, {2908, "1.25"}, {2908, "-1.25"}, {2909, "500"}, {2910, "3"},
            {2911, nullptr}, {2912, "500.01"}})
        zipCatalogSale(buyer, amount, buyer == 2902 ? 221 : 191);
    for (const auto buyer : std::initializer_list<std::optional<i64>>{2914, 2915, 99999, std::nullopt}) zipCatalogSale(buyer, "999");
    for (const auto date : std::initializer_list<std::optional<i64>>{190, 193, 181, 201, 4, 99999, std::nullopt}) zipCatalogSale(2913, "999", date);
    loadSales("catalog_sales", "cs_", "cs_order_number", catalog);
    // Q43: seven independent CASE/SUM states, duplicate/NULL/negative sales,
    // absent weekdays, a NULL grouping key, and rejected offset/date/join rows.
    auto weekdaySale = [&](std::optional<i64> shop, std::optional<i64> date, const char* price) {
        store.push_back({30000, order++, date, std::nullopt, price, nullptr, nullptr, shop});
    };
    const char* daily[]{"10.25", "-2", "0", "3.75", "4", "5", "6"};
    for (i64 day = 0; day != 7; ++day) weekdaySale(3000, 300 + day, daily[day]);
    weekdaySale(3000, 300, "1.25"); weekdaySale(3000, 300, nullptr);
    weekdaySale(3001, 300, "-2"); weekdaySale(3001, 301, "0");
    weekdaySale(3002, 302, "3"); weekdaySale(3003, 300, nullptr);
    weekdaySale(3004, 300, "1"); weekdaySale(3004, 300, "-1");
    weekdaySale(3006, 307, "999"); weekdaySale(3006, 308, "999");
    for (const auto shop : std::initializer_list<std::optional<i64>>{3005, 1, 99999, std::nullopt}) weekdaySale(shop, 300, "999");
    for (const auto date : std::initializer_list<std::optional<i64>>{309, 4, 99999, std::nullopt}) weekdaySale(3000, date, "999");
    // Q48: every demographic/price band crossed with every state/profit band,
    // inclusive endpoints, duplicate attributes and all nullable join/filter inputs.
    // Unrelated fact measures and keys remain NULL; Q28 quantities stay out of range.
    auto bandSale = [&](std::optional<i64> demo, std::optional<i64> address, const char* price, const char* profit,
            std::optional<i64> quantity, std::optional<i64> date = 300, std::optional<i64> shop = 1) {
        TSale sale{30000, order++, date, std::nullopt, price, nullptr, nullptr, shop, profit};
        sale.Demographics = demo; sale.Address = address; sale.Quantity = quantity; store.push_back(sale);
    };
    const char* priceBounds[3][4]{{"100", "150", "99.99", "150.01"}, {"50", "100", "49.99", "100.01"},
        {"150", "200", "149.99", "200.01"}};
    const char* profitBounds[3][4]{{"0", "2000", "-0.01", "2000.01"}, {"150", "3000", "149.99", "3000.01"},
        {"50", "25000", "49.99", "25000.01"}};
    i64 quantity = 40;
    for (i64 demo = 0; demo != 3; ++demo) for (i64 address = 0; address != 3; ++address)
        for (i64 endpoint = 0; endpoint != 2; ++endpoint)
            bandSale(3000 + demo, 3100 + address, priceBounds[demo][endpoint], profitBounds[address][endpoint], quantity++);
    bandSale(3000, 3100, "100", "0", 40); // A duplicate fact still contributes.
    bandSale(3000, 3100, "125", "100", -13); bandSale(3000, 3100, "125", "100", 0);
    bandSale(3000, 3100, "125", "100", std::nullopt); bandSale(3003, 3103, "100", "0", 70);
    for (i64 address = 3104; address <= 3109; ++address) bandSale(3000, address, "125", "500", 100);
    for (i64 band = 0; band != 3; ++band) for (i64 endpoint = 2; endpoint != 4; ++endpoint) {
        bandSale(3000 + band, 3100, priceBounds[band][endpoint], "100", 10000);
        bandSale(3000, 3100 + band, "125", profitBounds[band][endpoint], 10000);
    }
    for (const auto demo : std::initializer_list<std::optional<i64>>{3004, 3005, 3006, 99999, std::nullopt})
        bandSale(demo, 3100, "125", "100", 10000);
    for (const auto address : std::initializer_list<std::optional<i64>>{3110, 3111, 3112, 3113, 3114, 99999, std::nullopt})
        bandSale(3000, address, "125", "100", 10000);
    bandSale(3000, 3100, nullptr, "100", 10000); bandSale(3000, 3100, "125", nullptr, 10000);
    for (const auto date : std::initializer_list<std::optional<i64>>{309, 4, 99999, std::nullopt}) bandSale(3000, 3100, "125", "100", 10000, date);
    for (const auto shop : std::initializer_list<std::optional<i64>>{99999, std::nullopt}) bandSale(3000, 3100, "125", "100", 10000, 300, shop);
    // Q13: common equality predicates occur inside each demographic and
    // address OR arm. Cross all price/profit bands and retain distinct AVG
    // denominators. No item/customer matches keep previous fixtures isolated.
    auto commonBandSale = [&](std::optional<i64> demo, std::optional<i64> household, std::optional<i64> address,
            const char* price, const char* profit, std::optional<i64> quantity, const char* extended, const char* cost,
            std::optional<i64> date = 60, std::optional<i64> shop = 1) {
        TSale sale{32000, order++, date, std::nullopt, price, nullptr, extended, shop, profit};
        sale.Demographics = demo; sale.Household = household; sale.Address = address;
        sale.Quantity = quantity; sale.ExtendedCost = cost; store.push_back(sale);
    };
    const char* commonProfitBounds[3][4]{{"100", "200", "99.99", "200.01"},
        {"150", "300", "149.99", "300.01"}, {"50", "250", "49.99", "250.01"}};
    i64 commonQuantity = 40;
    for (i64 demo = 0; demo != 3; ++demo) for (i64 address = 0; address != 3; ++address)
        for (i64 endpoint = 0; endpoint != 2; ++endpoint)
            commonBandSale(3200 + demo, 3200 + demo, 3300 + address, priceBounds[demo][endpoint],
                commonProfitBounds[address][endpoint], commonQuantity++, "10", "20");
    commonBandSale(3200, 3200, 3300, "100", "100", 40, "10", "20");
    commonBandSale(3200, 3200, 3300, "125", "175", -13, "-2.50", "-5");
    commonBandSale(3200, 3200, 3300, "125", "175", 0, "0", "0");
    commonBandSale(3200, 3200, 3300, "125", "175", std::nullopt, nullptr, nullptr);
    commonBandSale(3203, 3203, 3303, "125", "175", 70, "5", "10");
    for (i64 address = 3304; address <= 3309; ++address)
        commonBandSale(3200, 3200, address, "125", "175", 0, "0", "0");
    commonBandSale(3200, 3200, 3300, "125", "175", std::nullopt, "7.50", nullptr);
    for (i64 band = 0; band != 3; ++band) for (i64 endpoint = 2; endpoint != 4; ++endpoint) {
        commonBandSale(3200 + band, 3200 + band, 3300, priceBounds[band][endpoint], "175", 10000, "999", "999");
        commonBandSale(3200, 3200, 3300 + band, "125", commonProfitBounds[band][endpoint], 10000, "999", "999");
    }
    for (const auto demo : std::initializer_list<std::optional<i64>>{3204, 3205, 3206, 99999, std::nullopt})
        commonBandSale(demo, 3200, 3300, "125", "175", 10000, "999", "999");
    for (const auto household : std::initializer_list<std::optional<i64>>{3201, 3204, 3205, 99999, std::nullopt})
        commonBandSale(3200, household, 3300, "125", "175", 10000, "999", "999");
    for (const auto address : std::initializer_list<std::optional<i64>>{3310, 3311, 3312, 3313, 3314, 99999, std::nullopt})
        commonBandSale(3200, 3200, address, "125", "175", 10000, "999", "999");
    commonBandSale(3200, 3200, 3300, nullptr, "175", 10000, "999", "999");
    commonBandSale(3200, 3200, 3300, "125", nullptr, 10000, "999", "999");
    for (const auto date : std::initializer_list<std::optional<i64>>{65, 66, 4, 99999, std::nullopt})
        commonBandSale(3200, 3200, 3300, "125", "175", 10000, "999", "999", date);
    for (const auto shop : std::initializer_list<std::optional<i64>>{99999, std::nullopt})
        commonBandSale(3200, 3200, 3300, "125", "175", 10000, "999", "999", 60, shop);
    // Q96: inclusive minute boundary, duplicate matching facts, independent
    // household IDs with equal attributes, and every nullable join/filter key.
    for (const i64 time : {3300, 3301}) for (const i64 household : {3300, 3301}) halfHourSale(time, household);
    halfHourSale(3300, 3300);
    for (const auto time : std::initializer_list<std::optional<i64>>{3302, 3303, 3304, 3305, 99999, std::nullopt}) halfHourSale(time, 3300);
    for (const auto household : std::initializer_list<std::optional<i64>>{3302, 3303, 99999, std::nullopt}) halfHourSale(3300, household);
    for (const auto shop : std::initializer_list<std::optional<i64>>{1, 4, 99999, std::nullopt}) halfHourSale(3300, 3300, shop);
    loadSales("store_sales", "ss_", "ss_ticket_number", store);
    loadSales("web_sales", "ws_", "ws_order_number", web);
    TValueBuilder returns;
    returns.BeginList();
    for (const auto& [item, order, refund] : std::initializer_list<std::tuple<i64, i64, const char*>>{
            {101, 4001, "20"}, {101, 4003, nullptr}, {101, 4004, "25"}, {102, 4010, "50"}, {102, 4011, "10"},
            {10000, 100005, nullptr}, {10001, 100005, nullptr}, {10002, 100007, nullptr}, {10003, 100010, nullptr},
            {20000, 700001, nullptr}, {20099, 700000, nullptr}, {20000, 700099, nullptr}}) {
        returns.AddListItem().BeginStruct().AddMember("cr_item_sk").Int64(item).AddMember("cr_order_number").Int64(order)
            .AddMember("cr_refunded_cash");
        decimal(returns, refund);
        returns.EndStruct();
    }
    load("catalog_returns", returns);
    TValueBuilder webReturns; webReturns.BeginList();
    for (const auto& [item, number] : std::initializer_list<std::pair<i64, i64>>{
            {10000, 200005}, {10001, 200005}, {10002, 200007}, {10003, 200010},
            {20000, 600002}, {20099, 600000}, {20000, 600099}})
        webReturns.AddListItem().BeginStruct().AddMember("wr_item_sk").Int64(item).AddMember("wr_order_number").Int64(number).EndStruct();
    load("web_returns", webReturns);
    const TString prelude = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> {return $x;};
)";
    TVector<TResultSnapshot> snapshots;
    TVector<std::pair<int, int>> queries{std::pair{21, 1}, {32, 1}, {37, 3}, {40, 3}, {82, 3}, {92, 1},
            {12, 8}, {20, 8}, {98, 8}, {53, 7}, {63, 8}, {89, 6}, {51, 4}, {41, 9}, {8, 3}, {38, 1}, {87, 1}, {16, 1}, {94, 1}, {95, 1},
            {22, 26}, {27, 7}, {36, 13}, {86, 13}, {70, 13}, {62, 4}, {99, 4}, {23, 1}, {47, 3}, {57, 3}, {90, 1}, {54, 5}, {24, 3}, {66, 7}, {88, 1}, {39, 3}, {78, 5}, {28, 1}, {17, 3}, {25, 4}, {29, 4}, {42, 7}, {52, 7}, {55, 7},
            {1, 4}, {3, 10}, {7, 5}, {15, 9}, {19, 6}, {43, 6}, {48, 1}, {13, 1}, {26, 5}, {96, 1}};
    if (remaining) {
        queries.clear();
        const THashMap<int, int> nonempty{{5, 1}, {14, 1}, {18, 1}, {44, 1}, {61, 1}, {65, 4},
            {67, 58}, {76, 96}, {77, 1}, {80, 1}, {97, 1}};
        for (int query : {2, 4, 5, 6, 9, 10, 11, 14, 18, 30, 31, 33, 34, 35, 44, 45, 46, 49, 50, 56,
                58, 59, 60, 61, 64, 65, 67, 68, 69, 71, 72, 73, 74, 75, 76, 77, 79, 80, 81, 83, 84, 85, 91, 93, 97}) {
            const auto& only = GetTestParam("NATIVE_TPCDS_QUERY");
            if (only.empty() || FromString<int>(only) == query)
                queries.emplace_back(query, nonempty.contains(query) ? nonempty.at(query) : 0);
        }
    }
    for (const auto [query, expectedRows] : queries) {
        Cerr << "TPC-DS Q" << query << ", native mode " << int(mode) << ", new RBO " << newRbo << Endl;
        const auto sql = prelude + TFileInput(SRC_(TStringBuilder() << "data/yql-tpcds/q" << query << ".yql")).ReadAll();
        const auto result = kikimr.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), query == 14 || query == 23 || query == 39 ? 2 : 1);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSet(0).RowsCount(), expectedRows, SnapshotRows(result.GetResultSet(0)).Values);
        TResultSetParser values(result.GetResultSet(0));
        auto assertDecimal = [&](const char* column, const std::optional<std::string>& expected) {
            const auto actual = values.ColumnParser(column).GetOptionalDecimal();
            UNIT_ASSERT_VALUES_EQUAL_C(bool(actual), bool(expected), column);
            if (expected) UNIT_ASSERT_VALUES_EQUAL_C(actual->ToString(), *expected, column);
        };
        if (query == 1) {
            for (const i64 buyer : {2500, 2500, 2501, 2509}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("c_customer_id").GetUtf8(), "Customer" + std::to_string(buyer));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 3) {
            for (const auto& [year, id, brand, amount] :
                    std::initializer_list<std::tuple<i64, std::optional<i64>, const char*, const char*>>{
                    {1998, 10, "Same", "13"}, {1998, 20, "Same", "13"}, {1998, 10, "Other", "11"},
                    {1998, std::nullopt, nullptr, "3"}, {1998, 40, "Zero", "0"}, {1998, 30, "Negative", "-4.5"},
                    {1998, 50, "AllNull", nullptr}, {1999, 20, "Same", "20"}, {1999, 10, "Same", "10"}, {1999, 50, "AllNull", nullptr}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("dt.d_year").GetOptionalInt64().value(), year);
                UNIT_ASSERT(values.ColumnParser("brand_id").GetOptionalInt64() == id);
                UNIT_ASSERT(values.ColumnParser("brand").GetOptionalUtf8() == optionalText(brand));
                assertDecimal("sum_agg", optionalText(amount));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 7 || query == 26) {
            for (const auto& [item, quantity, list, coupon, price] :
                    std::initializer_list<std::tuple<i64, std::optional<double>, const char*, const char*, const char*>>{
                    {2700, 30, "1000.02", "3", "3"}, {2701, std::nullopt, nullptr, "0", "15"},
                    {2702, std::nullopt, nullptr, nullptr, nullptr}, {2703, -20, "-5", "-1", "0"},
                    {2704, 40, "99999.99", "99999.99", "99999.99"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), itemName(item));
                UNIT_ASSERT(values.ColumnParser("agg1").GetOptionalDouble() == quantity);
                assertDecimal("agg2", optionalText(list)); assertDecimal("agg3", optionalText(coupon)); assertDecimal("agg4", optionalText(price));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 13) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_DOUBLES_EQUAL(values.ColumnParser("avg_ss_q").GetOptionalDouble().value(), 970.0 / 28, 1e-10);
            assertDecimal("avg_ss_s", "6.9"); assertDecimal("avg_ss_w", "13.75"); assertDecimal("sum_ss_w", "385");
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 96) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetUint64(), 5);
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 15) {
            for (const auto& [zip, amount] : std::initializer_list<std::pair<const char*, const char*>>{
                    {nullptr, "502.51"}, {"allnull", nullptr}, {"costly", "500.01"}, {"negative", "-4.5"},
                    {"outside", "7"}, {"x85669A", "12.75"}, {"x86197B", "3"}, {"y85669A", "4"}, {"zero", "0"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("ca_zip").GetOptionalUtf8() == optionalText(zip));
                assertDecimal("column1", optionalText(amount));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 19) {
            for (const auto& [brandId, brand, manufacturer, name, amount] :
                    std::initializer_list<std::tuple<std::optional<i64>, const char*, std::optional<i64>, const char*, const char*>>{
                    {10, "Zip", 100, "Maker", "13"}, {10, "Zip", 100, "Other", "13"}, {10, "Zip", 200, "Maker", "13"},
                    {std::nullopt, nullptr, std::nullopt, nullptr, "3"}, {30, "Neg", 300, "Neg", "-4.5"}, {50, "Null", 500, "Null", nullptr}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("brand_id").GetOptionalInt64() == brandId);
                UNIT_ASSERT(values.ColumnParser("brand").GetOptionalUtf8() == optionalText(brand));
                UNIT_ASSERT(values.ColumnParser("i_manufact_id").GetOptionalInt64() == manufacturer);
                UNIT_ASSERT(values.ColumnParser("i_manufact").GetOptionalUtf8() == optionalText(name));
                assertDecimal("ext_price", optionalText(amount));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 43) {
            struct TWeek { i64 Store; const char* Name; const char* Sales[7]; };
            const char* columns[]{"sun_sales", "mon_sales", "tue_sales", "wed_sales", "thu_sales", "fri_sales", "sat_sales"};
            for (const auto& week : std::initializer_list<TWeek>{
                    {3002, nullptr, {nullptr, nullptr, "3"}}, {3003, "AllNull", {}}, {3006, "Unknown day", {}},
                    {3000, "Week A", {"11.5", "-2", "0", "3.75", "4", "5", "6"}},
                    {3001, "Week A", {"-2", "0"}}, {3004, "Zero", {"0"}}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("s_store_name").GetOptionalUtf8() == optionalText(week.Name));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_store_id").GetUtf8(), "Store" + std::to_string(week.Store));
                for (size_t day = 0; day != 7; ++day) assertDecimal(columns[day], optionalText(week.Sales[day]));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 48) {
            UNIT_ASSERT(values.TryNextRow());
            // 40..57 + duplicate 40 - 13 + 70 + six other state matches * 100.
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetOptionalInt64().value(), 1570);
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 42 || query == 52 || query == 55) {
            const bool category = query == 42;
            const char* idName = category ? "item.i_category_id" : "brand_id";
            const char* textName = category ? "item.i_category" : "brand";
            const char* sumName = category ? "column3" : "ext_price";
            TVector<std::pair<TString, TString>> columns;
            if (query != 55) columns.emplace_back("dt.d_year", "Int64?");
            columns.emplace_back(idName, "Int64?");
            columns.emplace_back(textName, "Utf8?");
            columns.emplace_back(sumName, TTypeBuilder().BeginOptional().Decimal({35, 2}).EndOptional().Build().ToString());
            UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(result.GetResultSet(0)).Columns, columns);
            for (const auto& [id, categoryName, brandName, amount] :
                    std::initializer_list<std::tuple<std::optional<i64>, const char*, const char*, const char*>>{
                    {10, "Revenue A", "Same", "13"}, {20, "Revenue A", "Same", "13"},
                    {10, "Revenue B", "Other", "11"}, {std::nullopt, nullptr, nullptr, "3"},
                    {40, "Revenue Zero", "Zero", "0"}, {30, "Revenue Negative", "Negative", "-4.5"},
                    {50, "Revenue Null", "AllNull", nullptr}}) {
                UNIT_ASSERT(values.TryNextRow());
                if (query != 55) UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("dt.d_year").GetOptionalInt64().value(), 1998);
                UNIT_ASSERT(values.ColumnParser(idName).GetOptionalInt64() == id);
                UNIT_ASSERT(values.ColumnParser(textName).GetOptionalUtf8() == optionalText(category ? categoryName : brandName));
                assertDecimal(sumName, optionalText(amount));
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 25 || query == 29) {
            const bool profits = query == 25;
            const i64 firstItem = profits ? 2100 : 2200;
            const char* profitValues[4][3]{{"-3.75", "4.5", "7.5"}, {"15", "3", "2.5"},
                {nullptr, nullptr, nullptr}, {"399999.96", "0", "0.02"}};
            const std::optional<i64> quantities[4][3]{{12, 3, 9}, {21, 15, 3}, {{}, {}, {}}, {-2, 0, 0}};
            const TString measures[]{profits ? "store_sales_profit" : "store_sales_quantity",
                profits ? "store_returns_loss" : "store_returns_quantity", profits ? "catalog_sales_profit" : "catalog_sales_quantity"};
            TVector<std::pair<TString, TString>> columns{{"i_item_id", "Utf8"}, {"i_item_desc", "Utf8?"},
                {"s_store_id", "Utf8"}, {"s_store_name", "Utf8?"}};
            for (const auto& measure : measures) columns.emplace_back(measure,
                profits ? TTypeBuilder().BeginOptional().Decimal({35, 2}).EndOptional().Build().ToString() : "Int64?");
            UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(result.GetResultSet(0)).Columns, columns);
            for (std::size_t row = 0; row < 4; ++row) {
                UNIT_ASSERT(values.TryNextRow());
                const auto item = firstItem + (row < 2 ? 0 : row - 1);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), itemName(item));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_desc").GetOptionalUtf8().value(), "Description" + std::to_string(item));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_store_id").GetUtf8(), row == 1 ? "Store2" : row == 2 ? "Store4" : "Store1");
                UNIT_ASSERT(values.ColumnParser("s_store_name").GetOptionalUtf8() == optionalText(row == 2 ? nullptr : "Store A"));
                for (std::size_t channel = 0; channel < 3; ++channel) {
                    if (profits) {
                        const auto actual = values.ColumnParser(measures[channel]).GetOptionalDecimal();
                        UNIT_ASSERT_VALUES_EQUAL(bool(actual), profitValues[row][channel] != nullptr);
                        if (actual) UNIT_ASSERT_VALUES_EQUAL(actual->ToString(), TDecimalValue(profitValues[row][channel], 35, 2).ToString());
                    } else UNIT_ASSERT_C(values.ColumnParser(measures[channel]).GetOptionalInt64() == quantities[row][channel], measures[channel]);
                }
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 17) {
            struct TMeasures {
                ui64 Count;
                std::optional<double> Mean, Deviation, Coefficient;
            };
            const TMeasures expected[3][3]{
                {{10, 1, 2, 2}, {10, 2, 4, 2}, {10, 5, 0, 0}},
                {{1, 7, {}, {}}, {0, {}, {}, {}}, {1, 9, {}, {}}},
                {{0, {}, {}, {}}, {3, 3, 2, 2.0 / 3.0}, {3, 0, 0, std::numeric_limits<double>::quiet_NaN()}}};
            const TString prefixes[]{"store_sales_quantity", "store_returns_quantity", "catalog_sales_quantity"};
            TVector<std::pair<TString, TString>> columns{{"i_item_id", "Utf8"}, {"i_item_desc", "Utf8?"}, {"s_state", "Utf8?"}};
            for (const auto& prefix : prefixes) {
                columns.emplace_back(prefix + "count", "Uint64");
                for (const auto* suffix : {"ave", "stdev", "cov"}) columns.emplace_back(prefix + suffix, "Double?");
            }
            UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(result.GetResultSet(0)).Columns, columns);
            for (std::size_t row = 0; row < 3; ++row) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), itemName(1900 + row));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_desc").GetOptionalUtf8().value(), "Description" + std::to_string(1900 + row));
                UNIT_ASSERT(values.ColumnParser("s_state").GetOptionalUtf8() == optionalText(row == 1 ? "TN" : nullptr));
                for (std::size_t channel = 0; channel < 3; ++channel) {
                    const auto& prefix = prefixes[channel];
                    const auto& measures = expected[row][channel];
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(prefix + "count").GetUint64(), measures.Count);
                    for (const auto& [suffix, expectedValue] : std::initializer_list<std::pair<const char*, std::optional<double>>>{
                            {"ave", measures.Mean}, {"stdev", measures.Deviation}, {"cov", measures.Coefficient}}) {
                        const auto actual = values.ColumnParser(prefix + suffix).GetOptionalDouble();
                        UNIT_ASSERT_VALUES_EQUAL_C(bool(actual), bool(expectedValue), prefix + suffix);
                        if (actual) UNIT_ASSERT_C((std::isnan(*expectedValue) && std::isnan(*actual)) || *actual == *expectedValue,
                            prefix << suffix << ": " << *actual << " != " << *expectedValue);
                    }
                }
            }
            UNIT_ASSERT(!values.TryNextRow());
        } else if (query == 28) {
            UNIT_ASSERT(values.TryNextRow());
            const auto decimal = TTypeBuilder().BeginOptional().Decimal({7, 2}).EndOptional().Build().ToString();
            TVector<std::pair<TString, TString>> columns;
            std::size_t bucket = 0;
            for (const auto* mean : {"9.75", "57.12", "42.1", "-16593.58", "83.5", "16682.75"}) {
                const TString prefix = TStringBuilder() << "B" << ++bucket << "_";
                assertDecimal((prefix + "LP").c_str(), std::string(mean));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(prefix + "CNT").GetUint64(), bucket == 5 ? 5 : 6);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(prefix + "CNTD").GetUint64(), bucket == 5 ? 4 : 5);
                columns.emplace_back(prefix + "LP", decimal);
                columns.emplace_back(prefix + "CNT", "Uint64");
                columns.emplace_back(prefix + "CNTD", "Uint64");
            }
            UNIT_ASSERT(!values.TryNextRow());
            if (!newRbo) std::sort(columns.begin(), columns.end());
            UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(result.GetResultSet(0)).Columns, columns);
        } else if (query == 78) {
            struct TExpectedChannel {
                std::optional<i64> StoreQuantity;
                const char* StoreCost;
                const char* StorePrice;
                i64 OtherQuantity;
                const char* OtherCost;
                const char* OtherPrice;
                std::optional<i64> Ratio;
            };
            i64 item = 20000;
            for (const auto& row : std::initializer_list<TExpectedChannel>{
                    {12, "30", "40", 5, "8", "13", 2}, {9, nullptr, nullptr, 3, "0", "0", 3},
                    {5, "1", "2", 2, "4", "6", 2}, {std::nullopt, "-1", "-2", 1, "2", "2", std::nullopt},
                    {7, "1", "2", 0, "3", "3", std::nullopt}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("ss_sold_year").GetOptionalInt64().value(), 2000);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("ss_item_sk").GetInt64(), item++);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("ss_customer_sk").GetOptionalInt64().value(), 20000);
                UNIT_ASSERT(values.ColumnParser("ratio").GetOptionalInt64() == row.Ratio);
                UNIT_ASSERT(values.ColumnParser("store_qty").GetOptionalInt64() == row.StoreQuantity);
                assertDecimal("store_wholesale_cost", optionalText(row.StoreCost));
                assertDecimal("store_sales_price", optionalText(row.StorePrice));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("other_chan_qty").GetInt64(), row.OtherQuantity);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("other_chan_wholesale_cost").GetDecimal().ToString(), row.OtherCost);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("other_chan_sales_price").GetDecimal().ToString(), row.OtherPrice);
            }
            UNIT_ASSERT(!values.TryNextRow());
            const auto decimal = TTypeBuilder().Decimal({35, 2}).Build();
            const TVector<std::pair<TString, TString>> columns{{"ss_sold_year", "Int64?"}, {"ss_item_sk", "Int64"}, {"ss_customer_sk", "Int64?"},
                {"ratio", "Int64?"}, {"store_qty", "Int64?"}, {"store_wholesale_cost", TTypeBuilder().Optional(decimal).Build().ToString()},
                {"store_sales_price", TTypeBuilder().Optional(decimal).Build().ToString()}, {"other_chan_qty", "Int64"},
                {"other_chan_wholesale_cost", decimal.ToString()}, {"other_chan_sales_price", decimal.ToString()}};
            UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(result.GetResultSet(0)).Columns, columns);
        } else if (query == 39) {
            for (std::size_t setIndex = 0; setIndex < 2; ++setIndex) {
                const auto& set = result.GetResultSet(setIndex);
                UNIT_ASSERT_VALUES_EQUAL(set.RowsCount(), setIndex == 0 ? 3 : 2);
                TVector<std::pair<TString, TString>> columns;
                for (const auto* prefix : {"inv1_", "inv2_"}) {
                    columns.emplace_back(TString(prefix) + "w_warehouse_sk", "Int64");
                    columns.emplace_back(TString(prefix) + "i_item_sk", "Int64");
                    columns.emplace_back(TString(prefix) + "d_moy", "Int64?");
                    columns.emplace_back(TString(prefix) + "mean", "Double?");
                    columns.emplace_back(TString(prefix) + "cov", "Double?");
                }
                UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(set).Columns, columns);
                TResultSetParser pairs(set);
                for (const auto& [warehouse, item, april, may, coefficient] :
                        std::initializer_list<std::tuple<i64, i64, double, double, double>>{
                            {170, 1800, 1, 2, 2}, {170, 1801, 3, 6, 4.0 / 3.0}, {171, 1800, 3, 4, 2}}) {
                    if (setIndex == 1 && item == 1801) continue;
                    UNIT_ASSERT(pairs.TryNextRow());
                    for (const bool second : {false, true}) {
                        const TString prefix = second ? "inv2_" : "inv1_";
                        UNIT_ASSERT_VALUES_EQUAL(pairs.ColumnParser(prefix + "w_warehouse_sk").GetInt64(), warehouse);
                        UNIT_ASSERT_VALUES_EQUAL(pairs.ColumnParser(prefix + "i_item_sk").GetInt64(), item);
                        UNIT_ASSERT_VALUES_EQUAL(pairs.ColumnParser(prefix + "d_moy").GetOptionalInt64().value(), second ? 5 : 4);
                        UNIT_ASSERT_VALUES_EQUAL(pairs.ColumnParser(prefix + "mean").GetOptionalDouble().value(), second ? may : april);
                        UNIT_ASSERT_VALUES_EQUAL(pairs.ColumnParser(prefix + "cov").GetOptionalDouble().value(), coefficient);
                    }
                }
                UNIT_ASSERT(!pairs.TryNextRow());
            }
        } else if (query == 88) {
            TVector<TString> names{"h8_30_to_9", "h9_to_9_30", "h9_30_to_10", "h10_to_10_30", "h10_30_to_11", "h11_to_11_30", "h11_30_to_12", "h12_to_12_30"};
            UNIT_ASSERT(values.TryNextRow());
            for (std::size_t i = 0; i < names.size(); ++i) UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(names[i]).GetUint64(), i + 1);
            UNIT_ASSERT(!values.TryNextRow());
            if (!newRbo) std::sort(names.begin(), names.end());
            const auto& columns = result.GetResultSet(0).GetColumnsMeta();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), names.size());
            for (std::size_t i = 0; i < names.size(); ++i) UNIT_ASSERT_VALUES_EQUAL(columns[i].Name, names[i]);
        } else if (query == 66) {
            struct TExpectedWarehouse {
                const char* Name;
                std::optional<i64> Area;
                const char* Sales;
                const char* PerFoot;
                const char* Net;
                int Month = 0;
            };
            for (const auto& row : std::initializer_list<TExpectedWarehouse>{
                    {"Negative", -3, "0.06", "-0.01", "0.6"}, {"Null", std::nullopt, "0.03", nullptr, "0.3"},
                    {"NullAmount", 2, nullptr, nullptr, nullptr, 1}, {"Qty", 100, "16", "0.16", "24", 6},
                    {"Round", 2, "0.02", "0", "0.2"}, {"Zero", 0, "0.03", "inf", "0.3"}, {"ZeroZero", 0, "0", "nan", "0"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("w_warehouse_name").GetOptionalUtf8().value(), row.Name);
                UNIT_ASSERT(values.ColumnParser("w_warehouse_sq_ft").GetOptionalInt64() == row.Area);
                for (const auto* column : {"w_city", "w_county", "w_state", "w_country"}) UNIT_ASSERT(!values.ColumnParser(column).GetOptionalUtf8());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("ship_carriers").GetString(), "DIAMOND,AIRBORNE");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("year").GetOptionalInt64().value(), 2002);
                int month = 0;
                for (const auto* name : {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}) {
                    ++month;
                    const bool selected = !row.Month || month == row.Month;
                    assertDecimal((TString(name) + "_sales").c_str(), optionalText(selected ? row.Sales : "0"));
                    assertDecimal((TString(name) + "_sales_per_sq_foot").c_str(), optionalText(selected ? row.PerFoot : "0"));
                    assertDecimal((TString(name) + "_net").c_str(), optionalText(selected ? row.Net : "0"));
                }
            }
        } else if (query == 24) {
            for (const auto& [last, first, paid] : std::initializer_list<std::tuple<const char*, const char*, const char*>>{
                    {"Alpha", "Alice", "30"}, {"Bravo", nullptr, "50"}, {"Lower", "Country", "21"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("c_last_name").GetOptionalUtf8() == optionalText(last));
                UNIT_ASSERT(values.ColumnParser("c_first_name").GetOptionalUtf8() == optionalText(first));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_store_name").GetOptionalUtf8().value(), "Case Shop");
                assertDecimal("paid", paid);
            }
        } else if (query == 54) {
            for (const auto& [segment, count] : std::initializer_list<std::pair<std::optional<i32>, ui64>>{
                    {std::nullopt, 1}, {-2, 1}, {0, 1}, {2, 3}, {4, 1}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("segment").GetOptionalInt32() == segment);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("num_customers").GetUint64(), count);
                const auto base = segment ? std::optional<i32>{*segment * 50} : std::nullopt;
                UNIT_ASSERT(values.ColumnParser("segment_base").GetOptionalInt32() == base);
            }
        } else if (query == 90) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("am_pm_ratio").GetDecimal().ToString(), "1.25");
        } else if (query == 47 || query == 57) {
            for (const auto& [month, sum, previous, next] : std::initializer_list<std::tuple<i64, const char*, const char*, const char*>>{
                    {1, "10", "8", "30"}, {3, "20", "30", "40"}, {2, "30", "10", "20"}}) {
                UNIT_ASSERT(values.TryNextRow());
                if (query == 47) {
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_category").GetOptionalUtf8().value(), "Adjacent");
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_brand").GetOptionalUtf8().value(), "Main");
                } else UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("cc_name").GetOptionalUtf8().value(), "Adjacent");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("d_year").GetOptionalInt64().value(), 2000);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("d_moy").GetOptionalInt64().value(), month);
                assertDecimal("avg_monthly_sales", "25"); assertDecimal("sum_sales", sum);
                assertDecimal("psum", previous); assertDecimal("nsum", next);
            }
        } else if (query == 23) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetOptionalDecimal()->ToString(), "87");
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).RowsCount(), 6);
            TResultSetParser buyers(result.GetResultSet(1));
            for (const auto& [last, revenue] : std::initializer_list<std::pair<const char*, const char*>>{
                    {"A", "4"}, {"A", "30"}, {"B", "10"}, {"B", "30"}, {"E", "6"}, {"E", "7"}}) {
                UNIT_ASSERT(buyers.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(buyers.ColumnParser("c_last_name").GetOptionalUtf8().value(), last);
                UNIT_ASSERT_VALUES_EQUAL(buyers.ColumnParser("c_first_name").GetOptionalUtf8().value(), "Buyer");
                UNIT_ASSERT_VALUES_EQUAL(buyers.ColumnParser("sales").GetOptionalDecimal()->ToString(), revenue);
            }
        } else if (query == 62 || query == 99) {
            for (const auto& [warehouse, modeName, site, buckets] :
                    std::initializer_list<std::tuple<const char*, const char*, const char*, std::array<i64, 5>>>{
                    {nullptr, nullptr, nullptr, {0, 0, 0, 0, 1}},
                    {"ABCDEFGHIJKLMNOPQRST", "AIR", "A", {4, 2, 2, 2, 1}},
                    {"Other", "AIR", nullptr, {1, 0, 0, 0, 0}}, {"Other", "SEA", "B", {0, 0, 1, 0, 0}}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser(0).GetOptionalString() == optionalText(warehouse));
                UNIT_ASSERT(values.ColumnParser("sm_type").GetOptionalUtf8() == optionalText(modeName));
                UNIT_ASSERT(values.ColumnParser(query == 62 ? "web_name" : "cc_name").GetOptionalUtf8() == optionalText(site));
                for (std::size_t i = 0; i < buckets.size(); ++i) UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(i + 3).GetInt64(), buckets[i]);
            }
        } else if (query == 70) {
            for (const auto& [state, county, total, hierarchy, rank] :
                    std::initializer_list<std::tuple<const char*, const char*, const char*, ui64, ui64>>{
                    {nullptr, nullptr, "1018", 2, 1},
                    {"TN", nullptr, "999", 1, 1}, {"CA", nullptr, "15", 1, 2}, {"TX", nullptr, "8", 1, 3},
                    {"WA", nullptr, "-4", 1, 4}, {"OR", nullptr, nullptr, 1, 5},
                    {"CA", "Beta", "10", 0, 1}, {"CA", "Alpha", "5", 0, 2}, {"OR", nullptr, nullptr, 0, 1},
                    {"TN", nullptr, "999", 0, 1}, {"TX", nullptr, "8", 0, 1}, {"WA", nullptr, "0", 0, 1}, {"WA", "Alpha", "-4", 0, 2}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("s_state").GetOptionalUtf8() == optionalText(state));
                UNIT_ASSERT(values.ColumnParser("s_county").GetOptionalUtf8() == optionalText(county));
                assertDecimal("total_sum", optionalText(total));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("lochierarchy").GetUint64(), hierarchy);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("rank_within_parent").GetUint64(), rank);
            }
        } else if (query == 16 || query == 94 || query == 95) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("order count").GetUint64(), query == 95 ? 2 : 3);
            assertDecimal("total shipping cost", query == 95 ? "5" : "3");
            assertDecimal("total net profit", query == 95 ? "3" : "9");
        } else if (query == 38 || query == 87) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetUint64(), query == 38 ? 4 : 3);
        } else if (query == 8) {
            for (const auto& [name, profit] : std::initializer_list<std::pair<const char*, const char*>>{
                    {"Intersect A", "10"}, {"Intersect B", "-4"}, {"Intersect null", nullptr}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("s_store_name").GetOptionalUtf8().value(), name);
                assertDecimal("column1", optionalText(profit));
            }
        } else if (query == 21) {
            const TVector<TString> columnNames = newRbo ?
                TVector<TString>{"w_warehouse_name", "i_item_id", "inv_before", "inv_after"} :
                TVector<TString>{"i_item_id", "inv_after", "inv_before", "w_warehouse_name"};
            const auto& columns = result.GetResultSet(0).GetColumnsMeta();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), columnNames.size());
            for (std::size_t i = 0; i < columnNames.size(); ++i) UNIT_ASSERT_VALUES_EQUAL(columns[i].Name, columnNames[i]);
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("w_warehouse_name").GetOptionalUtf8().value(), "Warehouse A");
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), "Item101");
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("inv_before").GetOptionalInt64().value(), 11);
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("inv_after").GetOptionalInt64().value(), 11);
        }
        if (query == 22) {
            using TRow = std::tuple<std::optional<std::string>, std::optional<std::string>, std::optional<std::string>, std::optional<std::string>, std::optional<double>>;
            TVector<TRow> expected, actual;
            auto row = [&](const char* product, const char* brand, const char* itemClass, const char* category, std::optional<double> qoh) {
                expected.emplace_back(optionalText(product), optionalText(brand), optionalText(itemClass), optionalText(category), qoh);
            };
            row("P", "B", "C1", "Roll A", 20); row("P", "B", "C1", nullptr, 20);
            row("P", "B", "C2", "Roll A", 60); row("P", "B", "C2", nullptr, 60);
            row("P", "B", nullptr, nullptr, 100. / 3);
            row("P", "Other", "C1", "Roll B", 20); row("P", "Other", "C1", nullptr, 20);
            row("P", "Other", nullptr, nullptr, 20); row("P", nullptr, nullptr, nullptr, 30);
            row("Q", nullptr, nullptr, "Roll B", 50);
            for (int n = 0; n < 3; ++n) row("Q", nullptr, nullptr, nullptr, 50);
            for (int n = 0; n < 2; ++n) { row(nullptr, nullptr, "C3", nullptr, 0); row(nullptr, nullptr, nullptr, nullptr, 0); }
            row("S", "B", "C5", "Roll D", 0); row("S", "B", "C5", nullptr, 0);
            row("S", "B", nullptr, nullptr, 0); row("S", nullptr, nullptr, nullptr, 0);
            row("R", "B", "C4", "Roll C", std::nullopt); row("R", "B", "C4", nullptr, std::nullopt);
            row("R", "B", nullptr, nullptr, std::nullopt); row("R", nullptr, nullptr, nullptr, std::nullopt);
            row(nullptr, nullptr, nullptr, nullptr, 21.25);
            while (values.TryNextRow()) actual.emplace_back(values.ColumnParser("i_product_name").GetOptionalUtf8(),
                values.ColumnParser("i_brand").GetOptionalUtf8(), values.ColumnParser("i_class").GetOptionalUtf8(),
                values.ColumnParser("i_category").GetOptionalUtf8(), values.ColumnParser("qoh").GetOptionalDouble());
            std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
            UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
            for (std::size_t i = 0; i < actual.size(); ++i) {
                UNIT_ASSERT(std::get<0>(actual[i]) == std::get<0>(expected[i]));
                UNIT_ASSERT(std::get<1>(actual[i]) == std::get<1>(expected[i]));
                UNIT_ASSERT(std::get<2>(actual[i]) == std::get<2>(expected[i]));
                UNIT_ASSERT(std::get<3>(actual[i]) == std::get<3>(expected[i]));
                const auto a = std::get<4>(actual[i]), e = std::get<4>(expected[i]);
                UNIT_ASSERT(bool(a) == bool(e));
                if (e) UNIT_ASSERT_DOUBLES_EQUAL(*a, *e, 1e-12);
            }
        }
        if (query == 27) {
            for (const auto& [item, state, grouping, quantity] :
                    std::initializer_list<std::tuple<const char*, const char*, ui64, std::optional<double>>>{
                    {nullptr, nullptr, 1, 6}, {"Item970", nullptr, 1, 4}, {"Item970", "TN", 0, 4},
                    {"Item971", nullptr, 1, 10}, {"Item971", "TN", 0, 10},
                    {"Item975", nullptr, 1, std::nullopt}, {"Item975", "TN", 0, std::nullopt}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("i_item_id").GetOptionalUtf8() == optionalText(item));
                UNIT_ASSERT(values.ColumnParser("s_state").GetOptionalUtf8() == optionalText(state));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("g_state").GetUint64(), grouping);
                UNIT_ASSERT(values.ColumnParser("agg1").GetOptionalDouble() == quantity);
                assertDecimal("agg2", quantity ? optionalText("20") : std::nullopt);
                assertDecimal("agg3", quantity ? optionalText("2") : std::nullopt);
                assertDecimal("agg4", quantity ? optionalText("10") : std::nullopt);
            }
        }
        if (query == 36 || query == 86) {
            using TRow = std::tuple<const char*, const char*, const char*, ui64, ui64>;
            const TVector<TRow> expected = query == 36 ? TVector<TRow>{
                {"0.15", nullptr, nullptr, 2, 1},
                {nullptr, "Roll C", nullptr, 1, 1}, {"0", "Roll D", nullptr, 1, 2}, {"0.1", "Roll B", nullptr, 1, 3},
                {"0.2", "Roll A", nullptr, 1, 4}, {"0.3", nullptr, nullptr, 1, 5},
                {"0.3", nullptr, "C3", 0, 1}, {"0.1", "Roll A", "C1", 0, 1}, {"0.3", "Roll A", "C2", 0, 2},
                {"-0.1", "Roll B", "C1", 0, 1}, {"0.3", "Roll B", nullptr, 0, 2},
                {nullptr, "Roll C", "C4", 0, 1}, {"0", "Roll D", "C5", 0, 1}} : TVector<TRow>{
                {"62", nullptr, nullptr, 2, 1},
                {"45", "Roll A", nullptr, 1, 1}, {"10", "Roll B", nullptr, 1, 2}, {"7", nullptr, nullptr, 1, 3},
                {"0", "Roll D", nullptr, 1, 4}, {nullptr, "Roll C", nullptr, 1, 5},
                {"7", nullptr, "C3", 0, 1}, {"30", "Roll A", "C2", 0, 1}, {"15", "Roll A", "C1", 0, 2},
                {"20", "Roll B", nullptr, 0, 1}, {"-10", "Roll B", "C1", 0, 2},
                {nullptr, "Roll C", "C4", 0, 1}, {"0", "Roll D", "C5", 0, 1}};
            for (const auto& [amount, category, itemClass, hierarchy, rank] : expected) {
                UNIT_ASSERT(values.TryNextRow());
                assertDecimal(query == 36 ? "gross_margin" : "total_sum", optionalText(amount));
                UNIT_ASSERT(values.ColumnParser("i_category").GetOptionalUtf8() == optionalText(category));
                UNIT_ASSERT(values.ColumnParser("i_class").GetOptionalUtf8() == optionalText(itemClass));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("lochierarchy").GetUint64(), hierarchy);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("rank_within_parent").GetUint64(), rank);
            }
        }
        if (query == 32 || query == 92) {
            UNIT_ASSERT(values.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetOptionalDecimal()->ToString(), query == 32 ? "18" : "36");
        }
        if (query == 41) {
            for (const auto* product : std::initializer_list<const char*>{nullptr, "Product0", "Product1", "Product2", "Product3", "Product4", "Product5", "Product6", "Product7"}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser("i_product_name").GetOptionalUtf8() == optionalText(product));
            }
        }
        if (query == 37 || query == 82) {
            for (const auto& [item, price] : query == 37 ? TVector<std::pair<i64, const char*>>{{1, "22"}, {2, "52"}, {15, "30"}} :
                    TVector<std::pair<i64, const char*>>{{21, "30"}, {22, "60"}, {35, "38"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), itemName(item));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_desc").GetOptionalUtf8().value(), "Description" + std::to_string(item));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_current_price").GetOptionalDecimal()->ToString(), price);
            }
        }
        if (query == 40) {
            for (const auto& [item, before, after] : std::initializer_list<std::tuple<i64, const char*, const char*>>{
                    {101, "130", "275"}, {102, "-10", "0"}, {104, nullptr, "0"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("w_state").GetOptionalUtf8().value(), "CA");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), itemName(item));
                const auto actualBefore = values.ColumnParser("sales_before").GetOptionalDecimal();
                if (before) UNIT_ASSERT_VALUES_EQUAL(actualBefore->ToString(), before);
                else UNIT_ASSERT(!actualBefore);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("sales_after").GetOptionalDecimal()->ToString(), after);
            }
        }
        if (query == 12 || query == 20 || query == 98) {
            const int multiplier = query == 12 ? 1 : query == 20 ? 2 : 3;
            for (const auto& [item, price, revenue, ratio] :
                    std::initializer_list<std::tuple<i64, const char*, std::optional<i64>, const char*>>{
                    {403, "30", 20, "25"}, {400, "10", 15, "25"}, {406, "60", 0, "0"},
                    {404, "40", 60, "75"}, {402, nullptr, std::nullopt, nullptr}, {407, "70", -4, "100"},
                    {405, "50", std::nullopt, nullptr}, {401, "20", 45, "75"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_id").GetUtf8(), itemName(item));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_item_desc").GetOptionalUtf8().value(), "Description" + std::to_string(item));
                const auto [category, itemClass] = revenueItems.at(item - 400);
                UNIT_ASSERT(values.ColumnParser("i_category").GetOptionalUtf8() == optionalText(category));
                UNIT_ASSERT(values.ColumnParser("i_class").GetOptionalUtf8() == optionalText(itemClass));
                assertDecimal("i_current_price", optionalText(price));
                assertDecimal("itemrevenue", revenue ? std::optional<std::string>{std::to_string(*revenue * multiplier)} : std::nullopt);
                assertDecimal("revenueratio", optionalText(ratio));
            }
        }
        if (query == 51) {
            for (const auto& [item, date, webSales, storeSales, webCumulative, storeCumulative] :
                    std::initializer_list<std::tuple<i64, const char*, const char*, const char*, const char*, const char*>>{
                    {600, "2001-01-01", "15", "12", "15", "12"}, {600, "2001-02-28", "15", nullptr, "15", "12"},
                    {600, "2001-04-01", "35", "22", "35", "22"}, {603, "2001-02-28", "20", nullptr, "20", "10"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("item_sk").GetOptionalInt64().value(), item);
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("d_date").GetOptionalUtf8().value(), date);
                assertDecimal("web_sales", optionalText(webSales));
                assertDecimal("store_sales", optionalText(storeSales));
                assertDecimal("web_cumulative", optionalText(webCumulative));
                assertDecimal("store_cumulative", optionalText(storeCumulative));
            }
        }
        if (query == 53 || query == 63) {
            using TExpected = std::tuple<std::optional<i64>, const char*, const char*>;
            const TVector<TExpected> expected = query == 53 ? TVector<TExpected>{
                {std::nullopt, "4", "8"}, {std::nullopt, "12", "8"}, {11, "10", "20"}, {11, "30", "20"},
                {10, "0", "40"}, {10, "30", "40"}, {10, "90", "40"}} : TVector<TExpected>{
                {std::nullopt, "4", "8"}, {std::nullopt, "12", "8"}, {20, "0", "30"}, {20, "10", "30"},
                {20, "20", "30"}, {20, "90", "30"}, {21, "10", "20"}, {21, "30", "20"}};
            for (const auto& [key, sum, mean] : expected) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT(values.ColumnParser(query == 53 ? "i_manufact_id" : "i_manager_id").GetOptionalInt64() == key);
                assertDecimal("sum_sales", optionalText(sum));
                assertDecimal(query == 53 ? "avg_quarterly_sales" : "avg_monthly_sales", optionalText(mean));
            }
        }
        if (query == 89) {
            for (const auto& [itemClass, brand, shop, company, month, sum, mean] :
                    std::initializer_list<std::tuple<const char*, const char*, const char*, const char*, i64, const char*, const char*>>{
                    {"parenting", "Family", "Store A", "Company A", 1, "10", "20"},
                    {"parenting", nullptr, nullptr, nullptr, 1, "8", "16"},
                    {"parenting", "Family", "Store A", "Company B", 1, "4", "8"},
                    {"parenting", "Family", "Store A", "Company B", 2, "12", "8"},
                    {"parenting", nullptr, nullptr, nullptr, 2, "24", "16"},
                    {"wallpaper", "Family", "Store A", "Company A", 1, "30", "20"}}) {
                UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_category").GetOptionalUtf8().value(), "Books");
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("i_class").GetOptionalUtf8().value(), itemClass);
                UNIT_ASSERT(values.ColumnParser("i_brand").GetOptionalUtf8() == optionalText(brand));
                UNIT_ASSERT(values.ColumnParser("s_store_name").GetOptionalUtf8() == optionalText(shop));
                UNIT_ASSERT(values.ColumnParser("s_company_name").GetOptionalUtf8() == optionalText(company));
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("d_moy").GetOptionalInt64().value(), month);
                assertDecimal("sum_sales", optionalText(sum));
                assertDecimal("avg_monthly_sales", optionalText(mean));
            }
        }
        if (query == 47 || query == 57 || query == 51 || query == 53 || query == 63 || query == 89) {
            TVector<TString> names = query == 51 ? TVector<TString>{"item_sk", "d_date", "web_sales", "store_sales", "web_cumulative", "store_cumulative"} :
                query == 53 ? TVector<TString>{"i_manufact_id", "sum_sales", "avg_quarterly_sales"} :
                query == 63 ? TVector<TString>{"i_manager_id", "sum_sales", "avg_monthly_sales"} :
                query == 47 ? TVector<TString>{"i_category", "i_brand", "d_year", "d_moy", "avg_monthly_sales", "sum_sales", "psum", "nsum"} :
                query == 57 ? TVector<TString>{"cc_name", "d_year", "d_moy", "avg_monthly_sales", "sum_sales", "psum", "nsum"} :
                TVector<TString>{"i_category", "i_class", "i_brand", "s_store_name", "s_company_name", "d_moy", "sum_sales", "avg_monthly_sales"};
            if (!newRbo) std::sort(names.begin(), names.end());
            const auto& columns = result.GetResultSet(0).GetColumnsMeta();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), names.size());
            for (std::size_t i = 0; i < names.size(); ++i) UNIT_ASSERT_VALUES_EQUAL(columns[i].Name, names[i]);
        }
        if (newRbo) {
            const auto explain = kikimr.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
            UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetPlan());
            const TString plan{*explain.GetStats()->GetPlan()};
            NJson::TJsonValue root;
            NJson::ReadJsonTree(plan, &root, true);
            const auto& stats = root.GetMapSafe().at("SimplifiedPlan").GetMapSafe().at("OptimizerStats").GetMapSafe();
            const auto total = stats.at("CBOTreesTotal").GetUIntegerSafe();
            UNIT_ASSERT_C(total > 0, plan);
            UNIT_ASSERT_VALUES_EQUAL_C(stats.at("CBOTreesOptimized").GetUIntegerSafe(), total, plan);
        }
        for (const auto& set : result.GetResultSets()) snapshots.push_back(SnapshotRows(set));
    }
    return snapshots;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNativeHost) {
    Y_UNIT_TEST_TWIN(RowParametersMatchBothCompilers, Peephole) {
        const TString declaration = "DECLARE $p AS List<Struct<z:Int32?,a:Int32>>; ";
        const TString queries[]{
            "SELECT * FROM AS_TABLE($p) ORDER BY a",
            "SELECT a+1 AS a, COALESCE(z,0) AS v FROM AS_TABLE($p) ORDER BY a",
            "SELECT COUNT(*) AS n, SUM(z) AS s FROM AS_TABLE($p)",
            "SELECT a.a AS a,b.a AS b FROM AS_TABLE($p) a JOIN AS_TABLE($p) b ON a.a=b.a ORDER BY a,b",
            "$q=SELECT * FROM AS_TABLE($p); SELECT a.a AS a,b.a AS b FROM $q a JOIN $q b ON a.a=b.a ORDER BY a,b",
            "PRAGMA YqlSelect='force'; WITH q AS (SELECT * FROM AS_TABLE($p)) SELECT a FROM q WHERE z IS NULL ORDER BY a",
            "SELECT a FROM AS_TABLE($p) UNION ALL SELECT a FROM AS_TABLE($p) ORDER BY a",
            "SELECT a FROM AS_TABLE($p) WHERE a IN (SELECT a FROM AS_TABLE($p)) ORDER BY a",
            "DECLARE $x AS Int32; SELECT a FROM AS_TABLE($p) WHERE a > $x ORDER BY a LIMIT 2"
        };
        const auto parameters = [&](bool empty, bool scalar, bool dotted = false) {
            const auto* name = dotted ? "z.z" : "z";
            TParamsBuilder builder;
            auto& rows = builder.AddParam("$p");
            if (empty) rows.EmptyList(TTypeBuilder().BeginStruct()
                .AddMember(name).BeginOptional().Primitive(EPrimitiveType::Int32).EndOptional()
                .AddMember("a").Primitive(EPrimitiveType::Int32).EndStruct().Build());
            else {
                rows.BeginList();
                for (const i32 id : {3,1,2}) rows.AddListItem().BeginStruct()
                    .AddMember(name).OptionalInt32(id == 2 ? std::nullopt : std::optional<i32>{id * 10})
                    .AddMember("a").Int32(id).EndStruct();
                rows.EndList();
            }
            rows.Build();
            if (scalar) builder.AddParam("$x").Int32(1).Build();
            return builder.Build();
        };
        TVector<TResultSnapshot> reference;
        std::string parameterType;
        for (const unsigned strategy : {0u, 2u}) {
            NKikimrConfig::TAppConfig app;
            auto* config = app.MutableTableServiceConfig();
            config->SetEnableNewRBO(strategy != 0);
            config->SetEnableFallbackToYqlOptimizer(false);
            config->SetEnableNewRBOPhysicalStagePeephole(Peephole);
            config->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            TKikimrRunner kikimr(TKikimrSettings(app).SetWithSampleTables(false));
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            size_t next = 0;
            for (const bool declared : {true, false}) for (const bool empty : {false, true}) {
                for (size_t i = 0; i < std::size(queries); ++i) {
                    const auto sql = (declared ? declaration : TString{}) + queries[i];
                    const auto params = parameters(empty, queries[i].Contains("$x"));
                    Cerr << "ROW_PARAM strategy=" << strategy << " declared=" << declared << " empty=" << empty << " query=" << i << Endl;
                    const auto result = session.ExecuteQuery(sql, NQuery::TTxControl::NoTx(), params).GetValueSync();
                    if (strategy == 0 && i == 7) {
                        // The original optimizer confuses this subquery's row
                        // with a tuple. Pin that existing failure; both RBO
                        // paths must still produce the independently known rows.
                        UNIT_ASSERT(!result.IsSuccess());
                        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Expected tuple type, but got: Struct<'a':Int32>");
                        reference.push_back({empty ? "[]" : "[[1];[2];[3]]", {{"a", "Int32"}}});
                        session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
                        ++next;
                        continue;
                    }
                    UNIT_ASSERT_C(result.IsSuccess(), sql << '\n' << result.GetIssues().ToString());
                    UNIT_ASSERT(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
                    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                    const auto snapshot = SnapshotRows(result.GetResultSet(0));
                    if (i == 0) UNIT_ASSERT_VALUES_EQUAL(snapshot.Values, empty ? "[]" : "[[1;[10]];[2;#];[3;[30]]]");
                    if (i == 2) UNIT_ASSERT_VALUES_EQUAL(snapshot.Values, empty ? "[[0u;#]]" : "[[3u;[40]]]");
                    if (strategy == 0) reference.push_back(snapshot); else AssertSame(snapshot, reference.at(next));
                    ++next;
                    if (strategy == 2 && !empty) {
                        const auto explain = session.ExecuteQuery(sql, NQuery::TTxControl::NoTx(), params,
                            NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                        UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetAst());
                        UNIT_ASSERT(explain.GetStats()->GetAst()->find("native frontend: native-read-v1") != std::string::npos);
                    }
                }
            }
            for (const bool empty : {false, true}) {
                const TString sql = "DECLARE $p AS List<Struct<`z.z`:Int32?,a:Int32>>; SELECT * FROM AS_TABLE($p) ORDER BY a";
                const auto result = session.ExecuteQuery(sql, NQuery::TTxControl::NoTx(), parameters(empty, false, true)).GetValueSync();

                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    const auto actual = SnapshotRows(result.GetResultSet(0));
                    UNIT_ASSERT_VALUES_EQUAL(actual.Values, empty ? "[]" : "[[1;[10]];[2;#];[3;[30]]]");
                    UNIT_ASSERT_VALUES_EQUAL(actual.Columns, (TVector<std::pair<TString, TString>>{{"a", "Int32"}, {"z.z", "Int32?"}}));
                    UNIT_ASSERT(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);

            }
            auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            const auto prepared = tableSession.PrepareDataQuery(declaration + queries[0]).GetValueSync();
            UNIT_ASSERT_C(prepared.IsSuccess(), prepared.GetIssues().ToString());
            auto query = prepared.GetQuery();
            UNIT_ASSERT_VALUES_EQUAL(query.GetParameterTypes().size(), 1);
            const auto type = query.GetParameterTypes().at("$p").ToString();
            if (strategy == 0) parameterType = type;
            else UNIT_ASSERT_VALUES_EQUAL(type, parameterType);
            for (const bool empty : {false,true,false}) {
                const auto actual = Snapshot(query.Execute(TTxControl::BeginTx().CommitTx(), parameters(empty, false)).GetValueSync());
                AssertSame(actual, reference.at(empty ? std::size(queries) : 0));
            }
            // A list of empty rows still has its supplied cardinality.
            for (const bool empty : {false,true}) {
                TParamsBuilder builder;
                auto& rows = builder.AddParam("$p");
                if (empty) rows.EmptyList(TTypeBuilder().BeginStruct().EndStruct().Build());
                else rows.BeginList().AddListItem().BeginStruct().EndStruct()
                    .AddListItem().BeginStruct().EndStruct().EndList();
                rows.Build();
                const auto result = session.ExecuteQuery("DECLARE $p AS List<Struct<>>; SELECT COUNT(*) AS n FROM AS_TABLE($p)",
                    NQuery::TTxControl::NoTx(), builder.Build()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), empty ? "[[0u]]" : "[[2u]]");
            }
        }
    }

    void RunJsonValuesMatchBothCompilersOnPopulatedTables(bool pushProjections) {
        struct TCase { const char* Json; const char* Path; std::optional<std::string> Value; bool StorageWildcard = false; };
        const TCase cases[]{
            {R"({"a":"alpha"})", "$.a", "alpha"}, {R"({"a.b.c":"dotted"})", "$.\"a.b.c\"", "dotted"},
            {R"({"a":1.25})", "$.a", "1.25"}, {R"({"a":false})", "$.a", "false"},
            {R"({"a":null})", "$.a", std::nullopt}, {"{}", "$.missing", std::nullopt},
            {"{}", "strict $.missing", std::nullopt}, {R"([{"a":"one"}])", "lax $.a", "one"},
            {R"([{"a":"one"}])", "strict $.a", std::nullopt}, {R"({"a":[1,2]})", "$.a[*]", std::nullopt, true},
            {R"({"a":[1,2]})", "$.a[1]", "2"}, {nullptr, "$.a", std::nullopt},
            {R"({"a":"x\u0000y"})", "$.a", std::string("x\0y", 3)}, {R"({"a":""})", "$.a", ""},
            {R"({"a":"one","b":"two"})", "$.*", std::nullopt, true}, {R"({"*":"star"})", "$.\"*\"", "star"},
            {"7", "$", "7"}, {R"("root")", "strict $", "root"}, {R"({"a":1})", "$", std::nullopt},
            {R"({"a":{"b":[1,2]}})", "$.a.b[1]", "2"}, {"[1,2]", "$[0]", "1"},
            {"[]", "$[4294967295]", std::nullopt}};
        TVector<TResultSnapshot> reference;
        for (const int strategy : {0, 2}) {
            auto settings = NativeSettings(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(strategy != 0);
            settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
            NKikimrKqp::TKqpSetting pushdown;
            pushdown.SetName("OptEnableOlapPushdown"); pushdown.SetValue(pushProjections ? "true" : "false");
            if (pushProjections) {
                NKikimrKqp::TKqpSetting projections;
                projections.SetName("OptEnableOlapPushdownProjections"); projections.SetValue("true");
                settings.SetKqpSettings({pushdown, projections});
            } else settings.SetKqpSettings({pushdown});
            TKikimrRunner runner(settings);
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            size_t index = 0;
            for (const bool column : {false, true}) {
                const TString table = column ? "/Root/JsonColumn" : "/Root/JsonRow";
                const TString schema = TStringBuilder() << "CREATE TABLE `" << table
                    << "` (id Uint64 NOT NULL, j Json, d JsonDocument, PRIMARY KEY(id))" << (column ? " WITH (STORE = COLUMN)" : "");
                const auto created = session.ExecuteSchemeQuery(schema).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                TValueBuilder rows; rows.BeginList();
                ui64 id = 0;
                for (const auto& test : cases) {
                    const auto json = test.Json ? std::optional<std::string>{test.Json} : std::nullopt;
                    rows.AddListItem().BeginStruct().AddMember("id").Uint64(id++)
                        .AddMember("j").OptionalJson(json).AddMember("d").OptionalJsonDocument(json).EndStruct();
                }
                const auto loaded = client.BulkUpsert(table, rows.EndList().Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
                id = 0;
                for (const auto& test : cases) {
                    const TString sql = TStringBuilder() << "SELECT JSON_VALUE(j, '" << test.Path << "') AS text, JSON_VALUE(d, '"
                        << test.Path << "') AS binary FROM `" << table << "` WHERE id = " << id++ << "ul";
                    const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                    if (pushProjections && column && strategy == 0 && test.StorageWildcard) {
                        // The legacy optimizer sends wildcard paths to a
                        // storage accessor that rejects them. Pin that failure;
                        // native must still match the successful row-table
                        // scalar execution, without optimizer fallback.
                        UNIT_ASSERT_C(!result.IsSuccess(), sql);
                        UNIT_ASSERT_C(result.GetIssues().ToString().find("Unsupported path: ") != std::string::npos,
                            result.GetIssues().ToString());
                        reference.push_back(reference.at(index - std::size(cases)));
                        ++index;
                        continue;
                    }
                    UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "strategy=" << strategy << " " << sql << ": " << result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, sql);
                    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
                    TResultSetParser values(result.GetResultSet(0)); UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_C(values.ColumnParser("text").GetOptionalUtf8() == test.Value, sql);
                    UNIT_ASSERT_C(values.ColumnParser("binary").GetOptionalUtf8() == test.Value, sql);
                    const auto actual = SnapshotRows(result.GetResultSet(0));
                    if (strategy == 0) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                    ++index;
                }
                // Invalid literal paths must fail even when the scan is empty.
                for (const auto* path : {"$.", "$[", "$[]"}) {
                    const TString sql = TStringBuilder() << "SELECT JSON_VALUE(j, '" << path << "') FROM `" << table << "` WHERE false";
                    const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                    UNIT_ASSERT_C(!result.IsSuccess(), sql);
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, sql);
                }
            }
            for (const bool binary : {false, true}) {
                const TString sql = TStringBuilder() << "DECLARE $json AS " << (binary ? "JsonDocument" : "Json")
                    << "?; SELECT JSON_VALUE($json, '$.a') AS value";
                const auto prepared = session.PrepareDataQuery(sql).GetValueSync();
                UNIT_ASSERT_C(prepared.IsSuccess(), prepared.GetIssues().ToString());
                auto query = prepared.GetQuery();
                for (const auto* json : {"{\"a\":\"first\"}", static_cast<const char*>(nullptr), "{\"a\":\"last\"}"}) {
                    auto params = query.GetParamsBuilder();
                    const auto data = json ? std::optional<std::string>{json} : std::nullopt;
                    if (binary) params.AddParam("$json").OptionalJsonDocument(data).Build();
                    else params.AddParam("$json").OptionalJson(data).Build();
                    const auto result = query.Execute(TTxControl::BeginTx().CommitTx(), params.Build()).GetValueSync();
                    const auto snapshot = Snapshot(result);
                    if (strategy == 0) reference.push_back(snapshot); else AssertSame(snapshot, reference.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST(JsonValuesMatchBothCompilersOnPopulatedTables) { RunJsonValuesMatchBothCompilersOnPopulatedTables(false); }
    Y_UNIT_TEST(JsonProjectionsPreserveScalarResultsAndKernelBoundaries) { RunJsonValuesMatchBothCompilersOnPopulatedTables(true); }

    Y_UNIT_TEST(OlapJsonProjectionKeepsOriginalDocumentSemantics) {
        // The immutable pre-PR compiler returns NULL for rows 2 and 3 in the
        // first query when projection overwrites doc before its outer NULL test.
        // Pin SQL results independently of that known baseline error.
        for (const bool push : {false, true}) {
            auto settings = NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED);
            auto* service = settings.AppConfig.MutableTableServiceConfig();
            service->SetAllowOlapDataQuery(true);
            service->SetEnableFallbackOnMultipleStatements(false);
            service->SetEnableNewRBOPhysicalStagePeephole(false);
            // The existing setting combines the pragma with this server flag
            // using OR. Disable the default so the pragma controls each case.
            service->SetEnableOlapPushdownProjections(false);
            TKikimrRunner runner(settings);
            auto tableClient = runner.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/JsonProjection` (id Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY(id)) WITH (STORE = COLUMN);").GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TValueBuilder rows; rows.BeginList();
            ui64 id = 1;
            for (const auto* document : {"{\"a\":\"value\"}", "{}", "{\"a\":null}", static_cast<const char*>(nullptr)}) {
                rows.AddListItem().BeginStruct().AddMember("id").Uint64(id++)
                    .AddMember("doc").OptionalJsonDocument(document ? std::optional<std::string>{document} : std::nullopt).EndStruct();
            }
            const auto loaded = tableClient.BulkUpsert("/Root/JsonProjection", rows.EndList().Build()).GetValueSync();
            UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            struct TCase {
                const char* Expression;
                const char* Filter;
                std::vector<std::optional<std::string>> Expected;
                bool StorageProjection;
            };
            const TCase cases[]{
                {"IF(doc IS NULL, JSON_VALUE(doc, '$.a'), 'fallback'u)", "", {"fallback", "fallback", "fallback", std::nullopt}, false},
                {"JSON_VALUE(doc, '$.a') || JSON_VALUE(doc, '$.a')", "", {"valuevalue", std::nullopt, std::nullopt, std::nullopt}, true},
                {"JSON_VALUE(doc, '$.a')", " WHERE doc IS NOT NULL", {"value", std::nullopt, std::nullopt}, true},
            };
            for (const auto& test : cases) {
                const TString sql = TStringBuilder() << "PRAGMA ydb.OptEnableOlapPushdown='true'; "
                    << "PRAGMA ydb.OptEnableOlapPushdownProjections='" << (push ? "true" : "false") << "'; "
                    << "SELECT id, " << test.Expression << " AS value FROM `/Root/JsonProjection`" << test.Filter << " ORDER BY id;";
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, sql);
                const auto& resultSet = result.GetResultSet(0);
                const auto explain = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetAst());
                const auto ast = *explain.GetStats()->GetAst();
                UNIT_ASSERT_C(ast.find("native frontend: native-read-v1") != std::string::npos, sql << '\n' << ast);
                UNIT_ASSERT_VALUES_EQUAL_C(ast.find("KqpOlapProjections") != std::string::npos,
                    push && test.StorageProjection, sql);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), test.Expected.size());
                TResultSetParser values(resultSet);
                std::size_t row = 0;
                while (values.TryNextRow()) {
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("id").GetUint64(), row + 1);
                    UNIT_ASSERT_C(values.ColumnParser("value").GetOptionalUtf8() == test.Expected[row], sql << " row " << row);
                    ++row;
                }
            }
        }
    }

    Y_UNIT_TEST(NativeCompilationBenchmark) {
        const auto& requested = GetTestParam("NATIVE_COMPILATION_BENCH_ITERATIONS");
        if (requested.empty()) return;
        const auto iterations = FromString<ui32>(requested);
        UNIT_ASSERT(iterations > 0 && iterations <= 100);
        std::array<std::unique_ptr<TKikimrRunner>, 3> runners;
        const char* modes[]{"legacy", "native-auto", "native-required"};
        for (size_t mode = 0; mode < runners.size(); ++mode) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(mode != 0);
            service->SetNativeFrontendMode(mode == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetEnableFallbackOnMultipleStatements(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            // Every sample includes parsing and compilation. In particular,
            // the legacy path cannot reuse a separately cached YQL AST.
            service->SetCompileQueryCacheSize(0);
            service->SetEnableAstCache(false);
            auto settings = TKikimrSettings(config).SetWithSampleTables(false);
            settings.LogSettings.emplace().DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;
            runners[mode] = std::make_unique<TKikimrRunner>(settings);
            auto session = runners[mode]->GetTableClient().CreateSession().GetValueSync().GetSession();
            for (const char* name : {"tpch", "tpcds"}) {
                auto schema = TFileInput(SRC_(TStringBuilder() << "data/schema/" << name << ".sql")).ReadAll();
                SubstGlobal(schema, "\n);", "\n) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
                const auto created = session.ExecuteSchemeQuery(schema).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            }
        }
        const TString prelude = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> {return $x;};
)";
        struct TCase { const char* Suite; int Query; TString Sql; };
        TVector<TCase> cases;
        for (const auto& [suite, query] : std::initializer_list<std::pair<const char*, int>>{
                {"tpch", 1}, {"tpch", 5}, {"tpch", 9}, {"tpch", 21},
                {"tpcds", 17}, {"tpcds", 28}, {"tpcds", 51}, {"tpcds", 52}})
            cases.push_back({suite, query, prelude + TFileInput(SRC_(TStringBuilder() << "data/yql-" << suite << "/q" << query << ".yql")).ReadAll()});
        // Warm all metadata and code paths once, then rotate compiler order
        // within each query/round. No timing assertion belongs in a unit test.
        Cout << "native_compile_bench,suite,query,mode,round,compile_cpu_us,compile_wall_us,request_wall_us\n";
        for (ui32 round = 0; round <= iterations; ++round) {
            for (size_t query = 0; query < cases.size(); ++query) {
                const auto& test = cases[query];
                for (size_t step = 0; step < runners.size(); ++step) {
                    const size_t mode = (round + query + step) % runners.size();
                    const auto started = std::chrono::steady_clock::now();
                    // Explain does not expose compilation counters. Execute on
                    // empty tables and report compiler counters separately from
                    // request time, which also includes query execution.
                    const auto result = runners[mode]->GetQueryClient().ExecuteQuery(test.Sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)).GetValueSync();
                    const auto requestUs = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count();
                    const TString context = TStringBuilder() << test.Suite << " Q" << test.Query << " " << modes[mode];
                    UNIT_ASSERT_C(result.IsSuccess(), context << ": " << result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, context);
                    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetCompilation() && result.GetStats()->GetPlan(), context);
                    const auto compilation = *result.GetStats()->GetCompilation();
                    UNIT_ASSERT_C(!compilation.IsFromCache(), context);
                    UNIT_ASSERT_C(compilation.GetCpuTimeUs() > 0 && compilation.GetDurationUs() > 0, context);
                    if (mode != 0) {
                        NJson::TJsonValue root;
                        NJson::ReadJsonTree(TString(*result.GetStats()->GetPlan()), &root, true);
                        const auto& stats = root.GetMapSafe().at("SimplifiedPlan").GetMapSafe().at("OptimizerStats").GetMapSafe();
                        const auto total = stats.at("CBOTreesTotal").GetUIntegerSafe();
                        if (query != 0) UNIT_ASSERT_C(total > 0, context);
                        UNIT_ASSERT_VALUES_EQUAL_C(stats.at("CBOTreesOptimized").GetUIntegerSafe(), total, context);
                    }
                    if (round) Cout << "native_compile_bench," << test.Suite << ',' << test.Query << ',' << modes[mode] << ',' << round << ','
                        << compilation.GetCpuTimeUs() << ',' << compilation.GetDurationUs() << ',' << requestUs << '\n';
                }
            }
        }
    }

    Y_UNIT_TEST(NativeKeyRangesMatchBothCompilersOnPopulatedCompositeKeys) {
        struct TCase {
            const char* Predicate;
            TVector<i32> Tags;
            bool Range = true;
            const char* Tail = "ORDER BY tag";
            size_t MaxRanges = 1;
        };
        const TVector<TCase> cases{
            {"key = 1", {3, 4, 5, 6, 7, 8}},
            {"key = 1 AND suffix < 5", {4, 5, 6}},
            {"key = 1 AND suffix <= 5", {4, 5, 6, 7}},
            {"key = 1 AND suffix > 0 AND suffix < 7", {6, 7}},
            {"key = 1 AND suffix >= 0 AND suffix <= 7", {5, 6, 7, 8}},
            {"key < 1", {1, 2}},
            {"key <= 0", {1, 2}},
            {"key > 1 AND key < 3", {9}},
            {"key >= 3 AND key <= 1", {}},
            {"1 = key AND 2 <= suffix", {6, 7, 8}},
            {"key = 1 AND suffix < 5 AND tag != 5", {4, 6}},
            {"key = 1 AND key = 2", {}},
            {"key = 1 AND suffix < 7 AND suffix < 2", {4, 5}},
            {"key = 9007199254740993ul", {11}},
            {"key = 9223372036854775807ul", {12}},
            {"key = -2", {1}},
            {"key IS NULL", {0}},
            {"key = 1 AND suffix IS NULL", {3}},
            {"key IS NULL AND suffix >= 0", {0}},
            {"key IS NULL OR key = 1", {0, 3, 4, 5, 6, 7, 8}, true, "ORDER BY tag", 2},
            {"key IS NULL OR (key = 1 AND suffix IS NULL)", {0, 3}, true, "ORDER BY tag", 2},
            {"suffix < 5", {0, 1, 2, 4, 5, 6, 9, 10, 11, 12}, false},
            {"CAST(key AS Int32) = 1", {3, 4, 5, 6, 7, 8}, false},
            {"key < 18446744073709551615ul", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
            {"key = 1 OR key = 2", {3, 4, 5, 6, 7, 8, 9}, true, "ORDER BY tag", 2},
            {"key < 0 OR key >= 3", {1, 10, 11, 12}, true, "ORDER BY tag", 2},
            {"key >= 3", {10, 11, 12}},
            {"key > 3", {11, 12}},
            {"(key >= 0 AND key <= 1) OR (key >= 1 AND key < 3)", {2, 3, 4, 5, 6, 7, 8, 9}, true, "ORDER BY tag", 2},
            {"key IN (2, 1, 2)", {3, 4, 5, 6, 7, 8, 9}, true, "ORDER BY tag", 3},
            {"key = 1 AND (suffix < 0 OR suffix >= 5)", {4, 7, 8}, true, "ORDER BY tag", 2},
            {"(key = 1 OR key = 2) AND (suffix = 0 OR suffix = 5)", {5, 7, 9}, true, "ORDER BY tag", 4},
            {"(key = 1 AND tag = 4) OR (key = 2 AND tag = 9)", {4, 9}, true, "ORDER BY tag", 2},
            {"key = 1 OR tag = 0", {0, 3, 4, 5, 6, 7, 8}, false},
            {"key = 1 OR key = 2", {9, 8, 7}, true, "ORDER BY key DESC, suffix DESC LIMIT 3", 2},
            {"key = 1 AND suffix > 0", {8, 7}, true, "ORDER BY suffix DESC LIMIT 2"},
            {"key = 1 AND tag != 8", {7, 6}, true, "ORDER BY suffix DESC LIMIT 2"},
        };
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/KeyRanges` (key Int64, suffix Int32, tag Int32, PRIMARY KEY (key, suffix));
                CREATE TABLE `/Root/UnsignedRanges` (key Uint64 NOT NULL, PRIMARY KEY (key));
            )").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            const TVector<std::pair<std::optional<i64>, std::optional<i32>>> data{
                {std::nullopt, 0}, {-2, 0}, {0, 0}, {1, std::nullopt}, {1, -1}, {1, 0}, {1, 2}, {1, 5}, {1, 7},
                {2, 0}, {3, 0}, {9007199254740993LL, 0}, {std::numeric_limits<i64>::max(), 0}
            };
            TValueBuilder rows;
            rows.BeginList();
            for (size_t i = 0; i < data.size(); ++i) {
                rows.AddListItem().BeginStruct()
                    .AddMember("key").OptionalInt64(data[i].first)
                    .AddMember("suffix").OptionalInt32(data[i].second)
                    .AddMember("tag").OptionalInt32(i).EndStruct();
            }
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/KeyRanges", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            const TVector<ui64> unsignedKeys{0, 9007199254740993ULL, 9223372036854775808ULL, std::numeric_limits<ui64>::max()};
            TValueBuilder unsignedRows;
            unsignedRows.BeginList();
            for (const auto key : unsignedKeys) unsignedRows.AddListItem().BeginStruct().AddMember("key").Uint64(key).EndStruct();
            unsignedRows.EndList();
            const auto unsignedInserted = client.BulkUpsert("/Root/UnsignedRanges", unsignedRows.Build()).GetValueSync();
            UNIT_ASSERT_C(unsignedInserted.IsSuccess(), unsignedInserted.GetIssues().ToString());
            size_t snapshot = 0;
            const auto assertTags = [&](const TDataQueryResult& result, const TVector<i32>& expected) {
                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), expected.size());
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns[0].first, "tag");
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns[0].second, TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Int32).EndOptional().Build().ToString());
                TResultSetParser values(result.GetResultSet(0));
                for (const auto tag : expected) {
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("tag").GetOptionalInt32().value(), tag);
                }
                if (strategy == 2) reference.push_back(actual);
                else AssertSame(actual, reference.at(snapshot));
                ++snapshot;
            };
            for (const auto& item : cases) {
                const TString sql = TStringBuilder() << "SELECT tag FROM `/Root/KeyRanges` WHERE " << item.Predicate << ' ' << item.Tail;
                const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
                if (result.GetResultSet(0).RowsCount() != item.Tags.size()) {
                    const auto diagnostic = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                    Cerr << "Range mismatch, strategy=" << strategy << ": " << sql << " actual=" << Snapshot(result).Values << Endl;
                    if (diagnostic.GetStats() && diagnostic.GetStats()->GetAst()) Cerr << *diagnostic.GetStats()->GetAst() << Endl;
                    if (diagnostic.GetStats() && diagnostic.GetStats()->GetPlan()) Cerr << *diagnostic.GetStats()->GetPlan() << Endl;
                }
                assertTags(result, item.Tags);
                if (strategy == 2) {
                    const auto explain = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                    UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                    UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetPlan());
                    const TString plan{*explain.GetStats()->GetPlan()};
                    NJson::TJsonValue json;
                    NJson::ReadJsonTree(plan, &json, true);
                    size_t reads = 0;
                    const auto inspect = [&](auto&& self, const NJson::TJsonValue& node) -> void {
                        if (node.IsMap()) {
                            const auto& fields = node.GetMapSafe();
                            const auto name = fields.find("Name");
                            if (name != fields.end() && (name->second.GetStringSafe() == "TableRangeScan" || name->second.GetStringSafe() == "TableFullScan")) {
                                ++reads;
                                UNIT_ASSERT_VALUES_EQUAL_C(name->second.GetStringSafe(), item.Range ? "TableRangeScan" : "TableFullScan", sql << plan);
                                if (item.Range) {
                                    const auto count = FromString<size_t>(fields.at("ReadRangesExpectedSize").GetStringSafe());
                                    UNIT_ASSERT_C(count > 0 && count <= item.MaxRanges, sql << plan);
                                    UNIT_ASSERT_VALUES_EQUAL(fields.at("ReadRanges").GetArraySafe().size(), count);
                                    if (item.MaxRanges == 1)
                                        UNIT_ASSERT(fields.contains("ReadRangeLower") && fields.contains("ReadRangeUpper"));
                                }
                            }
                            for (const auto& [key, value] : fields) { Y_UNUSED(key); self(self, value); }
                        } else if (node.IsArray()) for (const auto& value : node.GetArraySafe()) self(self, value);
                    };
                    inspect(inspect, json.GetMapSafe().at("SimplifiedPlan"));
                    UNIT_ASSERT_VALUES_EQUAL_C(reads, 1, sql << plan);
                }
            }
            // Prepared bounds must retain their parameter binding across executions,
            // including reversed intervals and exact values above Double precision.
            const auto prepared = session.PrepareDataQuery(R"(
                DECLARE $lo AS Int64; DECLARE $hi AS Int64;
                SELECT tag FROM `/Root/KeyRanges` WHERE key >= $lo AND key < $hi ORDER BY tag;
            )").GetValueSync();
            UNIT_ASSERT_C(prepared.IsSuccess(), prepared.GetIssues().ToString());
            auto query = prepared.GetQuery();
            const TVector<std::tuple<i64, i64, TVector<i32>>> bounds{
                {1, 2, {3, 4, 5, 6, 7, 8}}, {-2, 1, {1, 2}}, {3, 1, {}},
                {9007199254740993LL, std::numeric_limits<i64>::max(), {11}}, {1, 2, {3, 4, 5, 6, 7, 8}}
            };
            for (const auto& [lo, hi, expected] : bounds) {
                auto params = query.GetParamsBuilder().AddParam("$lo").Int64(lo).Build().AddParam("$hi").Int64(hi).Build().Build();
                assertTags(query.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync(), expected);
            }
            const auto unionPrepared = session.PrepareDataQuery(R"(
                DECLARE $lo AS Int64; DECLARE $hi AS Int64;
                SELECT tag FROM `/Root/KeyRanges` WHERE (key >= $lo AND key < $hi) OR key = 1 ORDER BY tag;
            )").GetValueSync();
            UNIT_ASSERT_C(unionPrepared.IsSuccess(), unionPrepared.GetIssues().ToString());
            auto unionQuery = unionPrepared.GetQuery();
            for (const auto& [lo, hi, expected] : TVector<std::tuple<i64, i64, TVector<i32>>>{
                    {1, 2, {3, 4, 5, 6, 7, 8}}, {-2, 1, {1, 2, 3, 4, 5, 6, 7, 8}}, {3, 1, {3, 4, 5, 6, 7, 8}},
                    {9007199254740993LL, std::numeric_limits<i64>::max(), {3, 4, 5, 6, 7, 8, 11}}, {1, 2, {3, 4, 5, 6, 7, 8}}}) {
                auto params = unionQuery.GetParamsBuilder().AddParam("$lo").Int64(lo).Build().AddParam("$hi").Int64(hi).Build().Build();
                assertTags(unionQuery.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync(), expected);
            }
            const auto nullablePrepared = session.PrepareDataQuery(R"(
                DECLARE $bound AS Int64?;
                SELECT tag FROM `/Root/KeyRanges` WHERE key = $bound ORDER BY tag;
            )").GetValueSync();
            UNIT_ASSERT_C(nullablePrepared.IsSuccess(), nullablePrepared.GetIssues().ToString());
            auto nullableQuery = nullablePrepared.GetQuery();
            for (const auto bound : {std::optional<i64>{1}, std::optional<i64>{}, std::optional<i64>{1}}) {
                auto params = nullableQuery.GetParamsBuilder().AddParam("$bound").OptionalInt64(bound).Build().Build();
                assertTags(nullableQuery.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync(),
                    bound ? TVector<i32>{3, 4, 5, 6, 7, 8} : TVector<i32>{});
            }
            for (const auto key : unsignedKeys) {
                const TString sql = TStringBuilder() << "SELECT key FROM `/Root/UnsignedRanges` WHERE key = " << key << "ul";
                const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
                TResultSetParser values(result.GetResultSet(0)); UNIT_ASSERT(values.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("key").GetUint64(), key);
                if (strategy == 2) reference.push_back(actual);
                else AssertSame(actual, reference.at(snapshot));
                ++snapshot;
            }
        }
    }

    Y_UNIT_TEST(PlannerPragmasPreserveIndexChoiceAndSessionDefaults) {
        const TString select = "SELECT id, payload FROM `/Root/NativeOptions` WHERE g = 2 ORDER BY id";
        TVector<TResultSnapshot> reference;
        for (int strategy : {0, 2}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetCompileQueryCacheSize(0);
            service->SetEnableAstCache(false);
            TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/NativeOptions` (id Int64 NOT NULL, g Int32, payload Utf8, PRIMARY KEY(id));
            )").GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TValueBuilder rows; rows.BeginList();
            for (i64 id = 1; id <= 4; ++id) rows.AddListItem().BeginStruct().AddMember("id").Int64(id)
                .AddMember("g").OptionalInt32(id == 4 ? std::nullopt : std::optional<i32>{id < 3 ? 2 : 3})
                .AddMember("payload").OptionalUtf8(std::string("row") + std::to_string(id)).EndStruct();
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/NativeOptions", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            const auto indexed = session.ExecuteSchemeQuery(
                "ALTER TABLE `/Root/NativeOptions` ADD INDEX ByGroup GLOBAL ON (g) COVER (payload)").GetValueSync();
            UNIT_ASSERT_C(indexed.IsSuccess(), indexed.GetIssues().ToString());
            size_t snapshot = 0;
            auto check = [&](const TString& header, const TString& tail, bool index) {
                // Distinct request texts plus disabled AST caching make reset checks
                // compile again instead of reusing an earlier default-settings plan.
                const TString sql = TStringBuilder() << "-- option case " << snapshot << '\n' << header << select << ';' << tail;
                Cerr << "Planner pragma strategy " << strategy << ": " << sql << Endl;

                const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[1;[\"row1\"]];[2;[\"row2\"]]]");
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns, (TVector<std::pair<TString, TString>>{{"id", "Int64"}, {"payload", "Utf8?"}}));
                if (strategy == 0) reference.push_back(actual); else AssertSame(actual, reference.at(snapshot));
                ++snapshot;
                const auto explain = kikimr.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetAst());
                const TString ast{*explain.GetStats()->GetAst()};
                UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("/ByGroup/indexImplTable"), index, sql << '\n' << ast);
                UNIT_ASSERT(explain.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
                if (strategy == 2) UNIT_ASSERT_C(ast.Contains("native frontend: native-read-v1"), ast);
            };
            for (const auto& [header, index] : std::initializer_list<std::pair<const char*, bool>>{
                    {"", true}, {"PRAGMA ydb.OptDisableAutoIndexSelection='false';", true},
                    {"PRAGMA ydb.OptDisableAutoIndexSelection='true';", false},
                    {"PRAGMA ydb.OptDisableAutoIndexSelection;", false},
                    {"PRAGMA KiKiMr.Opt_Disable_Auto_Index_Selection='OFF';", true},
                    {"PRAGMA ydb.OptDisableAutoIndexSelection='true'; PRAGMA ydb.OptDisableAutoIndexSelection='false';", true},
                    {"PRAGMA ydb.OptDisableAutoIndexSelection='false'; PRAGMA ydb.OptDisableAutoIndexSelection='true';", false},
                    {"", true}, {"PRAGMA ydb.CostBasedOptimizationLevel='0'; PRAGMA ydb.OptFallbackToLegacyOptimizer='false';", true},
                    {"PRAGMA ydb.HashJoinMode='grace'; PRAGMA ydb.OptShuffleElimination='true'; PRAGMA ydb.UseBlockHashJoin='false'; "
                     "PRAGMA ydb.DisableBlockExecution='true'; PRAGMA ydb.DqChannelVersion='1';", true},
                    {"PRAGMA ydb.OptDisableAutoIndexSelection='';", false}})
                check(header, "", index);
            const TString trailingSql = TStringBuilder() << "PRAGMA ydb.OptDisableAutoIndexSelection='true';" << select
                << ";PRAGMA ydb.OptDisableAutoIndexSelection='false';";
            const auto trailing = session.ExecuteDataQuery(trailingSql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            if (strategy < 2) {
                // Existing compiler failure: a trailing configuration operation
                // leaves ResWrite! unexecuted. Do not change that pipeline.
                UNIT_ASSERT(!trailing.IsSuccess());
                UNIT_ASSERT_C(trailing.GetIssues().ToString().find("Failed to execute callable with name: ResWrite!") != std::string::npos,
                    trailing.GetIssues().ToString());
            } else {
                AssertSame(Snapshot(trailing), reference.front());
                const auto explain = kikimr.GetQueryClient().ExecuteQuery(trailingSql, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetAst());
                UNIT_ASSERT_C(explain.GetStats()->GetAst()->find("/ByGroup/indexImplTable") != std::string::npos, *explain.GetStats()->GetAst());
            }
            for (const char* pragma : {"ydb.WindowFunctionsV2='maybe'", "ydb.CostBasedOptimizationLevel='4294967296'",
                    "ydb.HashJoinMode='unknown'", "ydb.OptimizerHints='JoinType(t u invalid)'",
                    "ydb.OptDisableAutoIndexSelection='true'; PRAGMA ydb.OptimizerHints='broken'; PRAGMA ydb.OptimizerHints=''"}) {
                const auto bad = session.ExecuteDataQuery(TStringBuilder() << "PRAGMA " << pragma << ';' << select,
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(!bad.IsSuccess(), pragma);
                UNIT_ASSERT(bad.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
                if (strategy == 2) UNIT_ASSERT_C(bad.GetIssues().ToString().find("[native_frontend]") != std::string::npos, bad.GetIssues().ToString());
            }
            if (strategy == 2) {
                const auto bad = session.ExecuteDataQuery("PRAGMA ydb.OptFallbackToLegacyOptimizer='true'; " + select,
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT(!bad.IsSuccess());
                UNIT_ASSERT_C(bad.GetIssues().ToString().find("native plans cannot request fallback") != std::string::npos, bad.GetIssues().ToString());
                UNIT_ASSERT(bad.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
            }
            check("", "", true);
            UNIT_ASSERT_VALUES_EQUAL(snapshot, reference.size());
        }
    }

    Y_UNIT_TEST(NativeIndexRangesMatchBothCompilersOnPopulatedTables) {
        struct TRow { std::optional<i64> Id; std::optional<i32> Group, X; const char* Payload; };
        const TVector<TRow> data{
            {std::nullopt, 2, 1, "null-key"}, {1, 2, std::nullopt, nullptr}, {2, 2, 0, "zero"}, {3, 2, 2, "two"},
            {4, 2, 5, "five"}, {5, 1, 2, "one"}, {6, std::nullopt, 2, "null-group"}, {7, 3, -1, "negative"},
            {8, 3, 2, "three"}, {9007199254740993LL, 2, 2, "large"}, {std::numeric_limits<i64>::max(), 2, 2, "max"},
            {-1, 2, -5, "minus"}, {9, 2, 2, nullptr}, {10, 2, 2, ""}};
        const TVector<i32> groupTwo{1, 2, 3, 4, 5, 10, 11, 12, 13, 14};
        struct TCase {
            const char* Predicate;
            TVector<i32> Tags;
            const char* Index;
            const char* Tail = "ORDER BY tag";
            const char* PayloadIndex = nullptr;
        };
        const TVector<TCase> cases{
            {"g = 2", groupTwo, "AGroup"},
            {"g = 1 OR g = 3", {6, 8, 9}, "AGroup"},
            {"g >= 2 OR g <= 2", {1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14}, "AGroup"},
            {"(g = 2 AND x < 2) OR (g = 3 AND x >= 0)", {1, 3, 9, 12}, "AGroup"},
            {"(g = 2 AND x < 2) OR (g = 3 AND tag > 8)", {1, 3, 9, 12}, "AGroup"},
            {"g = 2 AND x >= 0 AND x < 5", {1, 3, 4, 10, 11, 13, 14}, "AGroup"},
            {"x = 2", {4, 6, 7, 9, 10, 11, 13, 14}, "ByX"},
            {"g < 3", {1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14}, "AGroup"},
            {"g = 2 AND Length(payload) > 0", {1, 3, 4, 5, 10, 11, 12}, nullptr},
            {"g = 2 AND x IS NULL", {2}, "AGroup"},
            {"g = 2 AND x < 0", {12}, "AGroup"},
            {"g = 2 AND id = 9007199254740993", {10}, nullptr},
            {"x = 2 AND id > 3", {6, 7, 9, 10, 11, 13, 14}, "ByX"},
            {"2 = g AND 5 > x AND x >= 0", {1, 3, 4, 10, 11, 13, 14}, "AGroup"},
            {"g = 2", {1, 12, 2}, "GroupOrder", "ORDER BY id LIMIT 3", "AGroup"},
            {"g = 2", {11, 10, 14}, "AGroup", "ORDER BY id DESC LIMIT 3"},
            {"g = 2 AND x > 0", {11, 10}, "AGroup", "ORDER BY id DESC LIMIT 2"},
            {"id >= 2 AND id <= 4 AND g = 2", {3, 4, 5}, "GroupOrder"},
            {"g = 2 AND x >= 5 AND x < 0", {}, "AGroup"},
            {"g = 2 AND x = 2 AND id > 3", {10, 11, 13, 14}, "AGroup"}};
        TVector<TResultSnapshot> reference;
        for (int strategy : {0, 2}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnablePruneKeyColumns(true);
            service->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/NativeIndexed` (id Int64, g Int32, x Int32, tag Int32 NOT NULL, payload Utf8, PRIMARY KEY(id));
            )").GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TValueBuilder rows;
            rows.BeginList();
            for (size_t i = 0; i < data.size(); ++i) rows.AddListItem().BeginStruct()
                .AddMember("id").OptionalInt64(data[i].Id).AddMember("g").OptionalInt32(data[i].Group)
                .AddMember("x").OptionalInt32(data[i].X).AddMember("tag").Int32(i + 1)
                .AddMember("payload").OptionalUtf8(data[i].Payload ? std::optional<std::string>{data[i].Payload} : std::nullopt).EndStruct();
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/NativeIndexed", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            // Load first: row-table BulkUpsert does not maintain secondary indexes.
            // Declare Z before A to exercise deterministic candidate tie-breaking.
            for (const char* index : {"ZGroup GLOBAL ON (g, x) COVER (tag)", "ByX GLOBAL ON (x) COVER (tag)",
                    "GroupOrder GLOBAL ON (g, id) COVER (x, tag)", "AGroup GLOBAL ON (g, x) COVER (tag)"}) {
                const auto added = session.ExecuteSchemeQuery(TStringBuilder() << "ALTER TABLE `/Root/NativeIndexed` ADD INDEX " << index << ';').GetValueSync();
                UNIT_ASSERT_C(added.IsSuccess(), added.GetIssues().ToString());
            }
            size_t snapshot = 0;
            const auto compare = [&](const TResultSnapshot& actual) {
                if (strategy == 0) reference.push_back(actual); else AssertSame(actual, reference.at(snapshot));
                ++snapshot;
            };
            const auto assertRows = [&](const TResultSet& rows, const TVector<i32>& expected, bool payload) {
                UNIT_ASSERT_VALUES_EQUAL_C(rows.RowsCount(), expected.size(), FormatResultSetYson(rows));
                TVector<std::pair<TString, TString>> columns{{"tag", "Int32"}};
                if (payload) columns.emplace_back("payload", "Utf8?");
                UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(rows).Columns, columns);
                TResultSetParser parser(rows);
                for (const auto tag : expected) {
                    UNIT_ASSERT(parser.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("tag").GetInt32(), tag);
                    if (payload) {
                        const auto* text = data.at(tag - 1).Payload;
                        UNIT_ASSERT(parser.ColumnParser("payload").GetOptionalUtf8() ==
                            (text ? std::optional<std::string>{text} : std::nullopt));
                    }
                }
                UNIT_ASSERT(!parser.TryNextRow());
            };
            for (const auto& test : cases) for (const bool payload : {false, true}) {
                const TString sql = TStringBuilder() << "SELECT tag" << (payload ? ", payload" : "")
                    << " FROM `/Root/NativeIndexed` WHERE " << test.Predicate << ' ' << test.Tail;
                Cerr << "Native index range strategy " << strategy << ": " << sql << Endl;
                const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                const auto actual = Snapshot(result);
                assertRows(result.GetResultSet(0), test.Tags, payload);
                compare(actual);
                if (strategy == 2) {
                    const auto explain = kikimr.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                    UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                    UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetPlan() && explain.GetStats()->GetAst());
                    const TString plan{*explain.GetStats()->GetPlan()}, ast{*explain.GetStats()->GetAst()};
                    const char* index = payload && test.PayloadIndex ? test.PayloadIndex : test.Index;
                    if (index) {
                        UNIT_ASSERT_C(ast.Contains(TStringBuilder() << '/' << index << "/indexImplTable"), sql << '\n' << ast);
                        UNIT_ASSERT_VALUES_EQUAL_C(plan.Contains("TableLookup"), payload, sql << '\n' << plan);
                        UNIT_ASSERT_C(plan.Contains("\"ReadRanges\""), sql << '\n' << plan);
                        if (!TStringBuf(test.Predicate).Contains(" OR "))
                            UNIT_ASSERT_C(plan.Contains("ReadRangeLower") && plan.Contains("ReadRangeUpper"), sql << '\n' << plan);
                    } else UNIT_ASSERT_C(!ast.Contains("indexImplTable"), sql << '\n' << ast);
                }
            }
            const auto prepared = session.PrepareDataQuery(R"(
                DECLARE $lo AS Int32; DECLARE $hi AS Int32;
                SELECT tag, payload FROM `/Root/NativeIndexed` WHERE g >= $lo AND g < $hi ORDER BY tag;
            )").GetValueSync();
            UNIT_ASSERT_C(prepared.IsSuccess(), prepared.GetIssues().ToString());
            auto query = prepared.GetQuery();
            for (const auto& [lo, hi, tags] : TVector<std::tuple<i32, i32, TVector<i32>>>{
                    {2, 3, groupTwo}, {1, 2, {6}}, {3, 4, {8, 9}}, {3, 1, {}}, {2, 3, groupTwo}}) {
                auto params = query.GetParamsBuilder().AddParam("$lo").Int32(lo).Build().AddParam("$hi").Int32(hi).Build().Build();
                const auto result = query.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                const auto actual = Snapshot(result);
                assertRows(result.GetResultSet(0), tags, true);
                compare(actual);
            }
            for (const auto& [sql, expected] : std::initializer_list<std::pair<const char*, const char*>>{
                    {"SELECT l.tag AS ltag, r.tag AS rtag, r.payload AS payload FROM `/Root/NativeIndexed` AS l "
                     "JOIN `/Root/NativeIndexed` AS r ON l.id = r.id WHERE l.tag = 4 AND r.g = 2 ORDER BY ltag, rtag", "[[4;4;[\"two\"]]]"},
                    {"$r = (SELECT id, tag, payload FROM `/Root/NativeIndexed` WHERE g = 2); "
                     "SELECT l.tag AS ltag, r.tag AS rtag, r.payload AS payload FROM `/Root/NativeIndexed` AS l "
                     "LEFT JOIN $r AS r ON l.id = r.id WHERE l.tag IN (1, 4, 6) ORDER BY ltag", "[[1;#;#];[4;[4];[\"two\"]];[6;#;#]]"}}) {
                const auto actual = Snapshot(session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync());
                UNIT_ASSERT_VALUES_EQUAL(actual.Values, expected);
                compare(actual);
            }
            UNIT_ASSERT_VALUES_EQUAL(snapshot, reference.size());
        }
    }

    Y_UNIT_TEST(NativeColumnKeyRangesMatchBothCompilersOnPopulatedTables) {
        struct TCase {
            const char* Predicate;
            TVector<i32> Tags;
            bool Range = true;
            const char* Tail = "ORDER BY tag";
            bool Multiple = false;
        };
        const TVector<TCase> cases{
            {"key < 0 OR key >= 3", {1, 9, 10, 11}, true, "ORDER BY tag", true},
            {"key >= 3", {9, 10, 11}},
            {"key > 3", {10, 11}},
            {"(key >= 0 AND key <= 1) OR (key >= 1 AND key < 3)", {2, 3, 4, 5, 6, 7, 8}, true, "ORDER BY tag", true},
            {"key IN (2, 1, 2)", {3, 4, 5, 6, 7, 8}, true, "ORDER BY tag", true},
            {"key = 1 AND (suffix < 0 OR suffix >= 5)", {3, 6, 7}, true, "ORDER BY tag", true},
            {"(key = 1 OR key = 2) AND (suffix = 0 OR suffix = 5)", {4, 6, 8}, true, "ORDER BY tag", true},
            {"(key = 1 AND v < 0) OR (key = 2 AND v > 0)", {4, 8}, true, "ORDER BY tag", true},
            {"key = 1 OR tag = 1", {1, 3, 4, 5, 6, 7}, false},
            {"key = 1 OR key = 2", {8, 7, 6}, true, "ORDER BY key DESC, suffix DESC LIMIT 3", true},
            {"key = 1", {3, 4, 5, 6, 7}},
            {"key = 1 AND suffix < 5", {3, 4, 5}},
            {"key = 1 AND suffix <= 5", {3, 4, 5, 6}},
            {"key = 1 AND suffix > 0 AND suffix < 7", {5, 6}},
            {"key < 1", {1, 2}},
            {"key > 1 AND key < 3", {8}},
            {"key >= 3 AND key <= 1", {}},
            {"1 = key AND 2 <= suffix", {5, 6, 7}},
            {"key = 1 AND v IS NULL", {3, 5}},
            {"key = 1 AND v > 0", {6, 7}},
            {"key = 1 AND LENGTH(CAST(suffix AS String)) > 1ul", {3}},
            {"key = 9007199254740993ul", {10}},
            {"key = 9223372036854775807ul", {11}},
            {"suffix < 5", {1, 2, 3, 4, 5, 8, 9, 10, 11}, false},
            {"tag > 5", {6, 7, 8, 9, 10, 11}, false},
            {"key = 1 AND suffix > 0", {7, 6}, true, "ORDER BY suffix DESC LIMIT 2"},
            {"key = 1 AND v IS NOT NULL", {7, 6}, true, "ORDER BY suffix DESC LIMIT 2"},
        };
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/ColumnKeyRanges` (
                    key Int64 NOT NULL, suffix Int32 NOT NULL, tag Int32 NOT NULL, v Int32,
                    PRIMARY KEY (key, suffix)
                ) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            const TVector<std::tuple<i64, i32, std::optional<i32>>> data{
                {-2, 0, 1}, {0, 0, std::nullopt}, {1, -1, std::nullopt}, {1, 0, -1}, {1, 2, std::nullopt},
                {1, 5, 2}, {1, 7, 3}, {2, 0, 4}, {3, 0, std::nullopt},
                {9007199254740993LL, 0, 6}, {std::numeric_limits<i64>::max(), 0, 7}
            };
            TValueBuilder rows;
            rows.BeginList();
            i32 tag = 0;
            for (const auto& [key, suffix, value] : data) {
                rows.AddListItem().BeginStruct().AddMember("key").Int64(key).AddMember("suffix").Int32(suffix)
                    .AddMember("tag").Int32(++tag).AddMember("v").OptionalInt32(value).EndStruct();
            }
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/ColumnKeyRanges", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            size_t snapshot = 0;
            const auto assertTags = [&](const TDataQueryResult& result, const TVector<i32>& expected) {
                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), expected.size());
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns[0].first, "tag");
                UNIT_ASSERT_VALUES_EQUAL(actual.Columns[0].second, TTypeBuilder().Primitive(EPrimitiveType::Int32).Build().ToString());
                TResultSetParser values(result.GetResultSet(0));
                for (const auto tag : expected) {
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("tag").GetInt32(), tag);
                }
                if (strategy == 2) reference.push_back(actual);
                else AssertSame(actual, reference.at(snapshot));
                ++snapshot;
            };
            for (const auto& item : cases) {
                const TString sql = TStringBuilder() << "SELECT tag FROM `/Root/ColumnKeyRanges` WHERE " << item.Predicate << ' ' << item.Tail;
                const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
                if (result.GetResultSet(0).RowsCount() != item.Tags.size()) {
                    const auto diagnostic = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                    Cerr << "Range mismatch, strategy=" << strategy << ": " << sql << " actual=" << Snapshot(result).Values << Endl;
                    if (diagnostic.GetStats() && diagnostic.GetStats()->GetAst()) Cerr << *diagnostic.GetStats()->GetAst() << Endl;
                    if (diagnostic.GetStats() && diagnostic.GetStats()->GetPlan()) Cerr << *diagnostic.GetStats()->GetPlan() << Endl;
                }
                assertTags(result, item.Tags);
                if (strategy == 2) {
                    const auto explain = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                    UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                    UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetPlan());
                    const TString plan{*explain.GetStats()->GetPlan()};
                    UNIT_ASSERT_VALUES_EQUAL_C(plan.find("\"ReadRanges\"") != TString::npos, item.Range, sql << plan);
                    if (!item.Multiple) {
                        UNIT_ASSERT_VALUES_EQUAL_C(plan.find("\"ReadRangeLower\"") != TString::npos, item.Range, sql << plan);
                        UNIT_ASSERT_VALUES_EQUAL_C(plan.find("\"ReadRangeUpper\"") != TString::npos, item.Range, sql << plan);
                    }
                    UNIT_ASSERT_C(plan.find(item.Range ? "TableRangeScan" : "TableFullScan") != TString::npos, sql << plan);
                    if (TStringBuf(item.Predicate) == "tag > 5") {
                        // A required Int32 comparison still executes as an
                        // OLAP filter when its column is not a storage key.
                        UNIT_ASSERT(explain.GetStats()->GetAst());
                        UNIT_ASSERT_C(explain.GetStats()->GetAst()->find("KqpOlapFilter") != std::string::npos, sql);
                    }
                }
            }
            const auto prepared = session.PrepareDataQuery(R"(
                DECLARE $lo AS Int64; DECLARE $hi AS Int64;
                SELECT tag FROM `/Root/ColumnKeyRanges` WHERE key >= $lo AND key < $hi ORDER BY tag;
            )").GetValueSync();
            UNIT_ASSERT_C(prepared.IsSuccess(), prepared.GetIssues().ToString());
            auto query = prepared.GetQuery();
            for (const auto& [lo, hi, expected] : TVector<std::tuple<i64, i64, TVector<i32>>>{
                    {1, 2, {3, 4, 5, 6, 7}}, {-2, 1, {1, 2}}, {3, 1, {}},
                    {9007199254740993LL, std::numeric_limits<i64>::max(), {10}}, {1, 2, {3, 4, 5, 6, 7}}}) {
                auto params = query.GetParamsBuilder().AddParam("$lo").Int64(lo).Build().AddParam("$hi").Int64(hi).Build().Build();
                assertTags(query.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync(), expected);
            }
            const auto unionPrepared = session.PrepareDataQuery(R"(
                DECLARE $lo AS Int64; DECLARE $hi AS Int64;
                SELECT tag FROM `/Root/ColumnKeyRanges` WHERE (key >= $lo AND key < $hi) OR key = 1 ORDER BY tag;
            )").GetValueSync();
            UNIT_ASSERT_C(unionPrepared.IsSuccess(), unionPrepared.GetIssues().ToString());
            auto unionQuery = unionPrepared.GetQuery();
            for (const auto& [lo, hi, expected] : TVector<std::tuple<i64, i64, TVector<i32>>>{
                    {1, 2, {3, 4, 5, 6, 7}}, {-2, 1, {1, 2, 3, 4, 5, 6, 7}}, {3, 1, {3, 4, 5, 6, 7}},
                    {9007199254740993LL, std::numeric_limits<i64>::max(), {3, 4, 5, 6, 7, 10}}, {1, 2, {3, 4, 5, 6, 7}}}) {
                auto params = unionQuery.GetParamsBuilder().AddParam("$lo").Int64(lo).Build().AddParam("$hi").Int64(hi).Build().Build();
                assertTags(unionQuery.Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync(), expected);
            }
        }
    }

    Y_UNIT_TEST(RuntimeDecimalCoalesceWideningMatchesBothExistingCompilers) {
        const TString sql = R"(
            DECLARE $a AS Decimal(7,2)?; DECLARE $b AS Decimal(35,2)?; DECLARE $c AS Decimal(12,2);
            SELECT COALESCE($a,$b) AS c0, COALESCE($b,$a) AS c1, COALESCE($a,$c,$b) AS c2,
                COALESCE($c,$b) AS c3, COALESCE($a,Decimal('0',35,2)) AS c4, COALESCE(NULL,$a,$b) AS c5,
                COALESCE($b,Decimal('0',7,2)) AS c6, COALESCE($c,Unwrap(CAST(NULL AS Decimal(35,2)))) AS c7
        )";
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto session = runner.GetTableClient().CreateSession().GetValueSync().GetSession();
            std::size_t index = 0;
            for (const auto* a : {static_cast<const char*>(nullptr), "0", "1.25", "-99999.99", "99999.99", "inf", "-inf", "nan"}) {
                for (const auto* b : {static_cast<const char*>(nullptr), "0", "-999999999999999999999999999999999.99",
                        "999999999999999999999999999999999.99", "inf", "-inf", "nan"}) {
                    auto builder = TParamsBuilder();
                    for (const auto& [name, text, precision] : std::initializer_list<std::tuple<const char*, const char*, ui8>>{
                            {"$a", a, 7}, {"$b", b, 35}}) {
                        auto& value = builder.AddParam(name);
                        if (text) value.BeginOptional().Decimal(TDecimalValue(text, precision, 2)).EndOptional().Build();
                        else value.EmptyOptional(TTypeBuilder().Decimal({precision, 2}).Build()).Build();
                    }
                    builder.AddParam("$c").Decimal(TDecimalValue("3.25", 12, 2)).Build();
                    const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), builder.Build()).GetValueSync();
                    const auto actual = Snapshot(result);
                    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
                    TResultSetParser values(result.GetResultSet(0)); UNIT_ASSERT(values.TryNextRow());
                    const std::array<const char*, 8> expected{a ? a : b, b ? b : a, a ? a : "3.25", "3.25", a ? a : "0", a ? a : b, b ? b : "0", "3.25"};
                    TVector<std::pair<TString, TString>> columns;
                    for (std::size_t col = 0; col < expected.size(); ++col) {
                        const bool optional = col < 2 || col == 5;
                        const auto type = TTypeBuilder().Decimal({35, 2}).Build();
                        columns.emplace_back("c" + ToString(col), (optional ? TTypeBuilder().Optional(type).Build() : type).ToString());
                        const auto value = optional ? values.ColumnParser(col).GetOptionalDecimal() : std::optional<TDecimalValue>{values.ColumnParser(col).GetDecimal()};
                        UNIT_ASSERT_VALUES_EQUAL(bool(value), expected[col] != nullptr);
                        if (value) UNIT_ASSERT_VALUES_EQUAL(value->ToString(), TDecimalValue(expected[col], 35, 2).ToString());
                    }
                    UNIT_ASSERT_VALUES_EQUAL(actual.Columns, columns);
                    if (strategy == 2) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(SampleStandardDeviationMatchesExistingCompilers, ColumnStore) {
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(TString("CREATE TABLE `/Root/Deviation` (id Uint64 NOT NULL, g Int32 NOT NULL, v Int64, r Int64 NOT NULL, PRIMARY KEY(id)) WITH (") +
                (ColumnStore ? "STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);" : "UNIFORM_PARTITIONS = 4);")).GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            TValueBuilder rows; rows.BeginList();
            ui64 id = 0;
            for (const auto& [group, value, required] : std::initializer_list<std::tuple<i32, std::optional<i64>, i64>>{
                    {1, 1, 1}, {1, 3, 3}, {1, 5, 5}, {1, std::nullopt, 3}, {2, 7, 7},
                    {3, std::nullopt, 0}, {3, std::nullopt, 0}, {4, 2, 2}, {4, 2, 2}}) {
                ++id;
                rows.AddListItem().BeginStruct().AddMember("id").Uint64((id % 4) * (ui64{1} << 62) + id).AddMember("g").Int32(group)
                    .AddMember("v").OptionalInt64(value).AddMember("r").Int64(required).EndStruct();
            }
            rows.EndList();
            const auto loaded = client.BulkUpsert("/Root/Deviation", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            const TString measures = "STDDEV(v) AS s, STDDEV_SAMPLE(r) AS r, STDDEV_SAMP(CAST(v AS Float)) AS f, "
                "StdDevSample(CAST(v AS Uint64)) AS u, Math::IsNaN(STDDEV(v)) AS n";
            // Extra cases combine scalar counts/sums with both tuple-state layouts.
            for (const int test : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
                const bool mixed = test >= 7;
                const int scenario = mixed ? test - 7 : test;
                const TString aggregates = measures + (mixed ? ", AVG(v) AS a, AVG(CAST(v AS Decimal(8,2))) AS d, COUNT(v) AS c, SUM(v) AS t" : "");
                const bool grouped = scenario == 0, single = scenario >= 4;
                const TString sql = "SELECT " + (grouped ? TString("g, ") : TString{}) +
                    (scenario == 4 ? "STDDEV_S(DISTINCT v) AS s" : scenario == 5 ? "STDDEV(CAST(v AS Double) / 0.0) AS s" :
                        scenario == 6 ? "STDDEV((CAST(v AS Double) - v) / 0.0) AS s" : aggregates) + " FROM `/Root/Deviation`" +
                    (grouped ? " GROUP BY g ORDER BY g" : scenario == 2 ? " WHERE g = 99" : scenario == 3 ? " WHERE g = 3" : " WHERE g = 1");
                Cerr << "stddev strategy " << strategy << ": " << sql << Endl;
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), grouped ? 4 : 1);
                TVector<std::pair<TString, TString>> columns;
                if (grouped) columns.emplace_back("g", "Int32");
                columns.emplace_back("s", "Double?");
                if (!single) {
                    columns.emplace_back("r", grouped ? "Double" : "Double?");
                    columns.emplace_back("f", "Double?"); columns.emplace_back("u", "Double?"); columns.emplace_back("n", "Bool?");
                    if (mixed) {
                        columns.emplace_back("a", "Double?");
                        columns.emplace_back("d", TTypeBuilder().BeginOptional().Decimal({8, 2}).EndOptional().Build().ToString());
                        columns.emplace_back("c", "Uint64"); columns.emplace_back("t", "Int64?");
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(result.GetResultSet(0)).Columns, columns);
                const double nan = std::numeric_limits<double>::quiet_NaN();
                const std::array<std::optional<double>, 4> optional{2.0, nan, std::nullopt, 0.0};
                const std::array<double, 4> required{std::sqrt(8.0 / 3.0), nan, 0.0, 0.0};
                TResultSetParser values(result.GetResultSet(0));
                for (int row = 0; row < (grouped ? 4 : 1); ++row) {
                    UNIT_ASSERT(values.TryNextRow());
                    if (grouped) UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("g").GetInt32(), row + 1);
                    const auto expected = grouped ? optional[row] : scenario == 2 || scenario == 3 ? std::nullopt : std::optional<double>{scenario >= 5 ? nan : 2.0};
                    auto check = [&](const char* name, std::optional<double> wanted, bool requiredType = false) {
                        const auto actual = requiredType ? std::optional<double>{values.ColumnParser(name).GetDouble()} : values.ColumnParser(name).GetOptionalDouble();
                        UNIT_ASSERT_VALUES_EQUAL_C(bool(actual), bool(wanted), name);
                        if (wanted) {
                            if (std::isnan(*wanted)) UNIT_ASSERT_C(std::isnan(*actual), name);
                            else UNIT_ASSERT_C(std::abs(*actual - *wanted) < 1e-12, TStringBuilder() << name << ": " << *actual << " != " << *wanted);
                        }
                    };
                    check("s", expected);
                    if (!single) {
                        check("f", expected); check("u", expected);
                        check("r", grouped ? std::optional<double>{required[row]} : scenario == 2 ? std::nullopt :
                            std::optional<double>{scenario == 3 ? 0.0 : required[0]}, grouped);
                        UNIT_ASSERT(values.ColumnParser("n").GetOptionalBool() == (expected ? std::optional<bool>{std::isnan(*expected)} : std::nullopt));
                    }
                    if (mixed) {
                        const std::optional<i64> means[]{3, 7, std::nullopt, 2};
                        const ui64 counts[]{3, 1, 0, 2};
                        const auto group = grouped ? row : scenario == 2 || scenario == 3 ? 2 : 0;
                        const auto mean = means[group];
                        UNIT_ASSERT(values.ColumnParser("a").GetOptionalDouble() ==
                            (mean ? std::optional<double>{double(*mean)} : std::nullopt));
                        const auto average = values.ColumnParser("d").GetOptionalDecimal();
                        UNIT_ASSERT_VALUES_EQUAL(bool(average), bool(mean));
                        if (mean) UNIT_ASSERT_VALUES_EQUAL(average->ToString(), TDecimalValue(ToString(*mean), 8, 2).ToString());
                        UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("c").GetUint64(), counts[group]);
                        UNIT_ASSERT(values.ColumnParser("t").GetOptionalInt64() ==
                            (mean ? std::optional<i64>{*mean * i64(counts[group])} : std::nullopt));
                    }
                }
                UNIT_ASSERT(!values.TryNextRow());
            }
        }
    }

    Y_UNIT_TEST(RuntimeIsNaNMatchesBothExistingCompilers) {
        const TString sql = "DECLARE $x AS Double; DECLARE $o AS Double?; DECLARE $f AS Float?; DECLARE $i AS Int64; DECLARE $u AS Uint64; "
            "SELECT Math::IsNaN($x), Math::IsNaN($o), Math::IsNaN($f), Math::IsNaN($i), Math::IsNaN($u), Math::IsNaN(CAST(NULL AS Double))";
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto session = runner.GetTableClient().CreateSession().GetValueSync().GetSession();
            std::size_t index = 0;
            for (const double value : {0.0, -0.0, 1.5, -1.5, std::numeric_limits<double>::infinity(),
                    -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN(), -std::numeric_limits<double>::quiet_NaN()}) {
                for (const bool present : {false, true}) {
                    const auto params = TParamsBuilder().AddParam("$x").Double(value).Build()
                        .AddParam("$o").OptionalDouble(present ? std::optional<double>{value} : std::nullopt).Build()
                        .AddParam("$f").OptionalFloat(present ? std::optional<float>{static_cast<float>(value)} : std::nullopt).Build()
                        .AddParam("$i").Int64(std::numeric_limits<i64>::min()).Build()
                        .AddParam("$u").Uint64(std::numeric_limits<ui64>::max()).Build().Build();
                    const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                    const auto actual = Snapshot(result);
                    TResultSetParser values(result.GetResultSet(0)); UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetBool(), std::isnan(value));
                    for (const int col : {1, 2}) UNIT_ASSERT(values.ColumnParser(col).GetOptionalBool() == (present ? std::optional<bool>{std::isnan(value)} : std::nullopt));
                    for (const int col : {3, 4}) UNIT_ASSERT(!values.ColumnParser(col).GetBool());
                    UNIT_ASSERT(!values.ColumnParser(5).GetOptionalBool());
                    if (strategy == 2) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(CommonConjunctsProduceKeyedJoinsOnPopulatedTables, ColumnStore) {
        struct TRow { i32 Id; std::optional<i32> X, Y; };
        const std::array<TRow, 7> left{{{1, 10, 1}, {2, 10, 0}, {3, 20, {}}, {4, {}, 1}, {5, 30, 0}, {6, 40, {}}, {7, 50, 1}}};
        const std::array<TRow, 8> right{{{11, 10, 2}, {12, 10, 0}, {13, 20, 2}, {14, {}, 2}, {15, 30, {}}, {16, 40, {}}, {17, 50, {}}, {18, 50, 0}}};
        const std::array<TString, 4> predicates{{
            "(a.x = b.x AND a.y = 1) OR (a.x = b.x AND b.y = 2)",
            "(a.y = 1 AND a.x = b.x) OR (b.y = 2 AND a.x = b.x)",
            "a.x = b.x OR (a.x = b.x AND a.y = 1)",
            "(a.x = b.x AND a.id > 1 AND a.y = 1) OR (a.x = b.x AND a.id > 1 AND b.y = 2)"}};
        TVector<TResultSnapshot> reference;
        for (const int strategy : {0, 2}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetAllowOlapDataQuery(true);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto load = [&](const char* path, const auto& data) {
                const auto created = session.ExecuteSchemeQuery(TString("CREATE TABLE `") + path +
                    "` (id Int32 NOT NULL, x Int32, y Int32, PRIMARY KEY(id))" +
                    (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                TValueBuilder rows; rows.BeginList();
                for (const auto& row : data)
                    rows.AddListItem().BeginStruct().AddMember("id").Int32(row.Id)
                        .AddMember("x").OptionalInt32(row.X).AddMember("y").OptionalInt32(row.Y).EndStruct();
                rows.EndList();
                const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            };
            load("/Root/FactoredLeft", left); load("/Root/FactoredRight", right);
            size_t index = 0;
            for (size_t test = 0; test < predicates.size(); ++test) for (const bool descending : {false, true}) {
                TVector<std::pair<const TRow*, const TRow*>> expected;
                for (const auto& a : left) for (const auto& b : right) {
                    if (!a.X || !b.X || *a.X != *b.X) continue;
                    if (test != 2 && !(a.Y == 1 || b.Y == 2)) continue;
                    if (test == 3 && a.Id <= 1) continue;
                    expected.emplace_back(&a, &b);
                }
                UNIT_ASSERT(!expected.empty());
                Sort(expected, [&](const auto& a, const auto& b) {
                    if (descending && a.first->X != b.first->X) return a.first->X > b.first->X;
                    if (descending && a.second->Y != b.second->Y) return a.second->Y > b.second->Y;
                    return std::pair(a.first->Id, a.second->Id) < std::pair(b.first->Id, b.second->Id);
                });
                const TString sql = TStringBuilder() << "SELECT CAST(a.id AS Int32?) AS lid, CAST(b.id AS Int32?) AS rid "
                    << "FROM `/Root/FactoredLeft` AS a CROSS JOIN `/Root/FactoredRight` AS b WHERE " << predicates[test]
                    << (descending ? " ORDER BY a.x DESC, b.y DESC, a.id, b.id" : " ORDER BY a.id, b.id");
                Cerr << "factored join strategy " << strategy << ": " << sql << Endl;
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                const auto& rows = result.GetResultSet(0);
                UNIT_ASSERT_VALUES_EQUAL_C(rows.RowsCount(), expected.size(), sql << FormatResultSetYson(rows));
                TResultSetParser values(rows);
                for (const auto& [a, b] : expected) {
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_C(values.ColumnParser("lid").GetOptionalInt32() == a->Id, sql);
                    UNIT_ASSERT_C(values.ColumnParser("rid").GetOptionalInt32() == b->Id, sql);
                }
                const auto actual = SnapshotRows(rows);
                if (strategy == 0) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                ++index;
                if (strategy != 2) continue;
                UNIT_ASSERT(result.GetStats() && result.GetStats()->GetPlan() && result.GetStats()->GetAst());
                UNIT_ASSERT(result.GetStats()->GetAst()->find("native frontend: native-read-v1") != std::string::npos);
                const auto plan = *result.GetStats()->GetPlan();
                NJson::TJsonValue json; NJson::ReadJsonTree(plan, &json, true);
                size_t joins = 0;
                const auto inspect = [&](auto&& self, const NJson::TJsonValue& node) -> void {
                    if (node.IsMap()) {
                        const auto& fields = node.GetMapSafe();
                        if (const auto found = fields.find("JoinKind"); found != fields.end()) {
                            ++joins;
                            UNIT_ASSERT_VALUES_EQUAL_C(found->second.GetStringSafe(), "Inner", sql << plan);
                            UNIT_ASSERT_C(fields.contains("Condition") && fields.at("Condition").GetStringSafe().Contains(" = "), sql << plan);
                        }
                        for (const auto& [key, value] : fields) { Y_UNUSED(key); self(self, value); }
                    } else if (node.IsArray()) for (const auto& value : node.GetArraySafe()) self(self, value);
                };
                inspect(inspect, json.GetMapSafe().at("SimplifiedPlan"));
                UNIT_ASSERT_C(joins > 0, sql << plan);
            }
        }
    }

    Y_UNIT_TEST_TWIN(ComputedJoinKeysPreserveResultsAndUseKeyedPlans, ColumnStore) {
        struct TRow { i32 Id; std::optional<i32> X, Y; };
        const std::array<TRow, 7> left{{{1, 0, 2}, {2, 1, 3}, {3, 1, 3}, {4, {}, 2}, {5, -1, {}}, {6, 10, 1}, {7, 2147483647, 0}}};
        const std::array<TRow, 7> right{{{11, 1, 3}, {12, 2, 5}, {13, 2, {}}, {14, {}, 2}, {15, -2147483647 - 1, 0}, {16, 5, 1}, {17, 11, 1}}};
        const std::array<TString, 6> predicates{{"a.x + 1 = b.x", "a.x + a.y = b.x + b.y",
            "CAST(a.x AS Int64) + 1L = CAST(b.x AS Int64)", "COALESCE(a.x, 0) + 1 = COALESCE(b.x, 0)",
            "a.x + 1 = b.x AND a.y < b.y", "a.x + 1 = b.x AND a.y + 2 = b.y"}};
        const auto add = [](std::optional<i32> a, std::optional<i32> b) -> std::optional<i32> {
            if (!a || !b) return {};
            return std::bit_cast<i32>(ui32(*a) + ui32(*b));
        };
        const auto matches = [&](const TRow& a, const TRow& b, size_t test) {
            auto lhs = add(a.X, 1), rhs = b.X;
            if (test == 1) { lhs = add(a.X, a.Y); rhs = add(b.X, b.Y); }
            if (test == 2) return a.X && b.X && i64(*a.X) + 1 == i64(*b.X);
            if (test == 3) { lhs = add(a.X.value_or(0), 1); rhs = b.X.value_or(0); }
            if (!lhs || !rhs || *lhs != *rhs) return false;
            if (test == 4) return a.Y && b.Y && *a.Y < *b.Y;
            if (test == 5) return add(a.Y, 2) && b.Y && *add(a.Y, 2) == *b.Y;
            return true;
        };
        for (size_t test = 0; test < predicates.size(); ++test) {
            bool positive = false;
            for (const auto& a : left) for (const auto& b : right) positive |= matches(a, b, test);
            UNIT_ASSERT_C(positive, predicates[test]);
        }
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetAllowOlapDataQuery(true);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            for (const bool second : {false, true}) {
                const TString path = second ? "/Root/ComputedRight" : "/Root/ComputedLeft";
                const auto created = session.ExecuteSchemeQuery("CREATE TABLE `" + path +
                    "` (id Int32 NOT NULL, x Int32, y Int32, PRIMARY KEY(id))" +
                    (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                TValueBuilder rows; rows.BeginList();
                for (const auto& row : second ? right : left)
                    rows.AddListItem().BeginStruct().AddMember("id").Int32(row.Id)
                        .AddMember("x").OptionalInt32(row.X).AddMember("y").OptionalInt32(row.Y).EndStruct();
                rows.EndList();
                const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            }
            size_t index = 0;
            for (const TString kind : {"INNER", "LEFT", "RIGHT", "LEFT SEMI", "RIGHT SEMI", "LEFT ONLY", "RIGHT ONLY", "WHERE"}) {
                const bool leftOnly = kind.StartsWith("LEFT "), rightOnly = kind.StartsWith("RIGHT ");
                for (size_t test = 0; test < predicates.size(); ++test, ++index) {
                    TVector<std::pair<std::optional<i32>, std::optional<i32>>> expected;
                    std::array<bool, 7> leftMatched{}, rightMatched{};
                    for (size_t a = 0; a < left.size(); ++a) for (size_t b = 0; b < right.size(); ++b) {
                        if (!matches(left[a], right[b], test)) continue;
                        leftMatched[a] = rightMatched[b] = true;
                        if (!leftOnly && !rightOnly) expected.emplace_back(left[a].Id, right[b].Id);
                    }
                    for (size_t a = 0; a < left.size(); ++a)
                        if ((kind == "LEFT" || kind == "LEFT ONLY") ? !leftMatched[a] : kind == "LEFT SEMI" && leftMatched[a])
                            expected.emplace_back(left[a].Id, std::nullopt);
                    for (size_t b = 0; b < right.size(); ++b)
                        if ((kind == "RIGHT" || kind == "RIGHT ONLY") ? !rightMatched[b] : kind == "RIGHT SEMI" && rightMatched[b])
                            expected.emplace_back(std::nullopt, right[b].Id);
                    Sort(expected, [](const auto& a, const auto& b) {
                        return std::pair(a.first.value_or(99), a.second.value_or(99)) < std::pair(b.first.value_or(99), b.second.value_or(99));
                    });
                    const TString sql = TStringBuilder() << "SELECT CAST(" << (rightOnly ? "NULL" : "a.id") << " AS Int32?) AS lid, CAST("
                        << (leftOnly ? "NULL" : "b.id") << " AS Int32?) AS rid FROM `/Root/ComputedLeft` AS a "
                        << (kind == "WHERE" ? "CROSS" : kind) << " JOIN `/Root/ComputedRight` AS b "
                        << (kind == "WHERE" ? "WHERE " : "ON ") << predicates[test] << " ORDER BY COALESCE(lid,99), COALESCE(rid,99)";
                    Cerr << "computed join strategy " << strategy << ": " << sql << Endl;
                    const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)).GetValueSync();
                    if (strategy == 0 && test == 4 && kind != "WHERE") {
                        // The legacy translator requires equality-only ON
                        // clauses (translation/join.cpp). Native still checks
                        // the original residual predicate against the oracle.
                        UNIT_ASSERT(!result.IsSuccess());
                        UNIT_ASSERT_C(result.GetIssues().ToString().find("JOIN ON expression must be a conjunction of equality predicates") != std::string::npos,
                            sql << ": " << result.GetIssues().ToString());
                        continue;
                    }

                    UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
                    UNIT_ASSERT(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
                    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                    const auto& rows = result.GetResultSet(0);
                    UNIT_ASSERT_VALUES_EQUAL_C(rows.RowsCount(), expected.size(), sql << FormatResultSetYson(rows));
                    TResultSetParser values(rows);
                    for (const auto& [a, b] : expected) {
                        UNIT_ASSERT(values.TryNextRow());
                        UNIT_ASSERT_C(values.ColumnParser("lid").GetOptionalInt32() == a, sql);
                        UNIT_ASSERT_C(values.ColumnParser("rid").GetOptionalInt32() == b, sql);
                    }
                    const auto actual = SnapshotRows(rows);
                    if (strategy == 2) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                    if (strategy == 2) {
                        UNIT_ASSERT(result.GetStats() && result.GetStats()->GetPlan());
                        const auto plan = *result.GetStats()->GetPlan();
                        NJson::TJsonValue json; NJson::ReadJsonTree(plan, &json, true);
                        size_t joins = 0;
                        const auto inspect = [&](auto&& self, const NJson::TJsonValue& node) -> void {
                            if (node.IsMap()) {
                                const auto& fields = node.GetMapSafe();
                                if (const auto found = fields.find("JoinKind"); found != fields.end()) {
                                    ++joins;
                                    UNIT_ASSERT_C(found->second.GetStringSafe() != "Cross", sql << plan);
                                    UNIT_ASSERT_C(fields.contains("Condition") && fields.at("Condition").GetStringSafe().Contains(" = "), sql << plan);
                                    const auto& condition = fields.at("Condition").GetStringSafe();
                                    UNIT_ASSERT_VALUES_EQUAL_C(std::count(condition.begin(), condition.end(), '='), test == 5 ? 2 : 1, sql << plan);
                                }
                                for (const auto& [key, value] : fields) { Y_UNUSED(key); self(self, value); }
                            } else if (node.IsArray()) for (const auto& value : node.GetArraySafe()) self(self, value);
                        };
                        inspect(inspect, json.GetMapSafe().at("SimplifiedPlan"));
                        UNIT_ASSERT_C(joins > 0, sql << plan);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(JoinStarsPreserveValuesNamesAndNullability, ColumnStore) {
        struct TRow { i32 Id; std::optional<i32> Key; const char* Value; };
        const std::array<TRow, 4> leftRows{{{1, 10, "A"}, {2, 20, "B"}, {3, std::nullopt, nullptr}, {4, 10, "C"}}};
        const std::array<TRow, 4> rightRows{{{11, 10, "X"}, {12, 30, "Y"}, {13, std::nullopt, nullptr}, {14, 10, nullptr}}};
        using TPairs = TVector<std::pair<std::optional<i32>, std::optional<i32>>>;
        TVector<std::pair<TString, TPairs>> cases{
            {"INNER", {{1, 11}, {1, 14}, {4, 11}, {4, 14}}},
            {"LEFT", {{1, 11}, {1, 14}, {2, {}}, {3, {}}, {4, 11}, {4, 14}}},
            {"RIGHT", {{1, 11}, {1, 14}, {4, 11}, {4, 14}, {{}, 12}, {{}, 13}}},
            {"FULL", {{1, 11}, {1, 14}, {2, {}}, {3, {}}, {4, 11}, {4, 14}, {{}, 12}, {{}, 13}}},
            {"LEFT SEMI", {{1, {}}, {4, {}}}}, {"RIGHT SEMI", {{{}, 11}, {{}, 14}}},
            {"LEFT ONLY", {{2, {}}, {3, {}}}}, {"RIGHT ONLY", {{{}, 12}, {{}, 13}}},
            {"CROSS", {}}};
        for (const auto& left : leftRows) for (const auto& right : rightRows) cases.back().second.emplace_back(left.Id, right.Id);
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            for (const bool right : {false, true}) {
                const TString prefix = right ? "b" : "a", path = right ? "/Root/StarRight" : "/Root/StarLeft";
                const auto created = session.ExecuteSchemeQuery("CREATE TABLE `" + path + "` (" + prefix + "id Int32 NOT NULL, " +
                    prefix + "k Int32, " + prefix + "v Utf8, PRIMARY KEY(" + prefix + "id))" +
                    (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                TValueBuilder rows; rows.BeginList();
                for (const auto& row : right ? rightRows : leftRows)
                    rows.AddListItem().BeginStruct().AddMember(prefix + "id").Int32(row.Id)
                        .AddMember(prefix + "k").OptionalInt32(row.Key)
                        .AddMember(prefix + "v").OptionalUtf8(row.Value ? std::optional<std::string>{row.Value} : std::nullopt).EndStruct();
                rows.EndList();
                const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            }
            for (const auto& [kind, expected] : cases) {
                const bool onlyLeft = kind.StartsWith("LEFT "), onlyRight = kind.StartsWith("RIGHT ");
                const bool outerLeft = kind == "RIGHT" || kind == "FULL", outerRight = kind == "LEFT" || kind == "FULL";
                for (const auto* projection : {"*", "l.*, r.*", "r.*, l.*"}) {
                    if ((onlyLeft || onlyRight) && TStringBuf(projection) != "*") continue;
                    // The legacy optimizer resolves ORDER BY against the
                    // emitted star names, after dropping source qualifiers.
                    const TString sql = TStringBuilder() << "SELECT " << projection << " FROM `/Root/StarLeft` AS l " << kind
                        << " JOIN `/Root/StarRight` AS r" << (kind == "CROSS" ? "" : " ON l.ak = r.bk") << " ORDER BY "
                        << (onlyLeft ? "aid" : onlyRight ? "bid" : "COALESCE(aid,99),COALESCE(bid,99)");
                    Cerr << "join star strategy " << strategy << ": " << sql << Endl;
                    const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();

                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                    const auto& set = result.GetResultSet(0);
                    UNIT_ASSERT_VALUES_EQUAL(set.RowsCount(), expected.size());
                    TVector<std::pair<TString, TString>> columns;
                    auto appendColumns = [&](bool right) {
                        if (right ? onlyLeft : onlyRight) return;
                        const TString prefix = right ? "b" : "a";
                        columns.emplace_back(prefix + "id", (right ? outerRight : outerLeft) ? "Int32?" : "Int32");
                        columns.emplace_back(prefix + "k", "Int32?");
                        columns.emplace_back(prefix + "v", "Utf8?");
                    };
                    const bool reversed = TStringBuf(projection).StartsWith("r");
                    appendColumns(reversed); appendColumns(!reversed);
                    if (strategy == 0) std::sort(columns.begin(), columns.end());
                    UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(set).Columns, columns);
                    TResultSetParser values(set);
                    for (const auto& [left, right] : expected) {
                        UNIT_ASSERT(values.TryNextRow());
                        for (const bool side : {false, true}) {
                            if (side ? onlyLeft : onlyRight) continue;
                            const auto id = side ? right : left;
                            const TRow* row = id ? &(side ? rightRows[*id - 11] : leftRows[*id - 1]) : nullptr;
                            const TString prefix = side ? "b" : "a";
                            const auto actualId = (side ? outerRight : outerLeft) ? values.ColumnParser(prefix + "id").GetOptionalInt32() :
                                std::optional<i32>{values.ColumnParser(prefix + "id").GetInt32()};
                            UNIT_ASSERT(actualId == id);
                            UNIT_ASSERT(values.ColumnParser(prefix + "k").GetOptionalInt32() == (row ? row->Key : std::nullopt));
                            UNIT_ASSERT(values.ColumnParser(prefix + "v").GetOptionalUtf8() ==
                                (row && row->Value ? std::optional<std::string>{row->Value} : std::nullopt));
                        }
                    }
                    UNIT_ASSERT(!values.TryNextRow());
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(WindowSubqueriesMatchLegacyOnPopulatedTables, ColumnStore) {
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(TString(
                "CREATE TABLE `/Root/WindowSubqueries` (id Int32 NOT NULL, p Int32, v Int32, PRIMARY KEY(id))")
                + (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TValueBuilder rows; rows.BeginList();
            i32 id = 0;
            for (const auto& [p, v] : std::initializer_list<std::pair<std::optional<i32>, std::optional<i32>>>{
                    {1, 10}, {1, std::nullopt}, {2, 30}, {2, 10}, {std::nullopt, 5}})
                rows.AddListItem().BeginStruct().AddMember("id").Int32(++id)
                    .AddMember("p").OptionalInt32(p).AddMember("v").OptionalInt32(v).EndStruct();
            rows.EndList();
            const auto loaded = client.BulkUpsert("/Root/WindowSubqueries", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            const std::pair<const char*, const char*> cases[]{
                {"SELECT (SELECT SUM(v) OVER() FROM `/Root/WindowSubqueries` LIMIT 1) AS s", "[[[55]]]"},
                {"SELECT (SELECT SUM(v) OVER() FROM `/Root/WindowSubqueries` WHERE id > 99 LIMIT 1) AS s", "[[#]]"},
                {"SELECT (SELECT n FROM (SELECT id, RowNumber() OVER(ORDER BY id) AS n FROM `/Root/WindowSubqueries`) AS w WHERE id = 3) AS n", "[[[3u]]]"},
                {"SELECT (SELECT r FROM (SELECT p, Rank() OVER(ORDER BY SUM(v) DESC) AS r FROM `/Root/WindowSubqueries` GROUP BY p) AS w WHERE p = 1) AS r", "[[[2u]]]"},
                {"SELECT id, (SELECT SUM(v) OVER() FROM `/Root/WindowSubqueries` LIMIT 1) AS s FROM `/Root/WindowSubqueries` ORDER BY id", "[[1;[55]];[2;[55]];[3;[55]];[4;[55]];[5;[55]]]"},
                {"SELECT id FROM `/Root/WindowSubqueries` WHERE v IN (SELECT v FROM (SELECT v, Rank() OVER(ORDER BY v DESC) AS r FROM `/Root/WindowSubqueries`) AS w WHERE r <= 2ul) ORDER BY id", "[[1];[3];[4]]"},
                {"SELECT id FROM `/Root/WindowSubqueries` WHERE v NOT IN (SELECT v FROM (SELECT v, Rank() OVER(ORDER BY v DESC) AS r FROM `/Root/WindowSubqueries`) AS w WHERE r <= 2ul) ORDER BY id", "[[5]]"},
                {"SELECT id FROM `/Root/WindowSubqueries` WHERE v IN (SELECT SUM(v) OVER() FROM `/Root/WindowSubqueries` WHERE id > 99)", "[]"},
                {"SELECT EXISTS (SELECT id FROM (SELECT id, RowNumber() OVER(ORDER BY id) AS n FROM `/Root/WindowSubqueries`) AS w WHERE n = 2ul) AS found", "[[%true]]"},
                {"SELECT EXISTS (SELECT id FROM (SELECT id, RowNumber() OVER(ORDER BY id) AS n FROM `/Root/WindowSubqueries`) AS w WHERE n = 99ul) AS found", "[[%false]]"},
                {"SELECT EXISTS (SELECT SUM(v) OVER() FROM `/Root/WindowSubqueries` WHERE id > 99) AS found", "[[%false]]"},
                {"$w = (SELECT v, RowNumber() OVER(ORDER BY id) AS n FROM `/Root/WindowSubqueries`); SELECT (SELECT v FROM $w WHERE n = 2ul) AS s", "[[#]]"},
                {"SELECT id FROM `/Root/WindowSubqueries` WHERE p IN (SELECT p FROM (SELECT p, Rank() OVER(ORDER BY SUM(v) DESC) AS r FROM `/Root/WindowSubqueries` GROUP BY p) AS w WHERE r <= 2ul) ORDER BY id", "[[1];[2];[3];[4]]"},
            };
            size_t index = 0;
            for (const auto& [sql, values] : cases) {
                Cerr << "window subquery strategy " << strategy << ": " << sql << Endl;
                const auto result = runner.GetQueryClient().ExecuteQuery(TString("PRAGMA YqlSelect='force'; ") + sql,
                    NQuery::TTxControl::NoTx()).GetValueSync();
                // The old RBO path does not inline projected EXISTS; the
                // existing non-window EXISTS matrix pins the same failure.

                UNIT_ASSERT_C(result.IsSuccess(), sql << "\n" << result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                const auto actual = SnapshotRows(result.GetResultSet(0));
                UNIT_ASSERT_VALUES_EQUAL_C(actual.Values, values, sql);
                if (strategy == 2) reference.push_back(actual);
                else AssertSame(actual, reference.at(index));
                ++index;
            }
        }
    }

    Y_UNIT_TEST_TWIN(GroupingSetsMatchLegacyOnPopulatedTables, ColumnStore) {
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(TString(
                "CREATE TABLE `/Root/Grouping` (id Int32 NOT NULL, k Int32, v Int32, w Int64 NOT NULL, PRIMARY KEY(id))")
                + (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TValueBuilder rows; rows.BeginList();
            i32 id = 0;
            for (const auto& [key, value, required] : std::initializer_list<std::tuple<std::optional<i32>, std::optional<i32>, i64>>{
                    {1, 2, 2}, {1, 2, 4}, {1, std::nullopt, -3},
                    {std::nullopt, -1, 6}, {std::nullopt, 3, 6}, {2, std::nullopt, 0}, {2, std::nullopt, 0}, {3, 2, 1}}) {
                rows.AddListItem().BeginStruct().AddMember("id").Int32(++id)
                    .AddMember("k").OptionalInt32(key).AddMember("v").OptionalInt32(value).AddMember("w").Int64(required).EndStruct();
            }
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/Grouping", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            size_t index = 0;
            const std::pair<const char*, const char*> cases[]{
                {"k, GROUPING(k) AS g, COUNT(*) AS n, SUM(w) AS s", "ROLLUP(k) ORDER BY g,k"},
                {"k, v, GROUPING(k,v) AS g, COUNT(*) AS n, SUM(w) AS s", "ROLLUP(k,v) ORDER BY g,k,v"},
                {"k, v, GROUPING(k,v) AS g, COUNT(*) AS n, SUM(w) AS s", "CUBE(k,v) ORDER BY g,k,v"},
                {"k, v, GROUPING(k,v) AS g, COUNT(*) AS n", "GROUPING SETS ((k,v),(k),(v),()) ORDER BY g,k,v"},
                {"w, k, GROUPING(w,k) AS g, SUM(w) AS s", "w, ROLLUP(k) ORDER BY w,g,k"},
                {"k, v, GROUPING(k,v) AS g, COUNT(*) AS n", "ROLLUP(k), ROLLUP(v) ORDER BY g,k,v"},
                {"k, v, GROUPING(k,v) AS g", "GROUPING SETS ((k,v)) ORDER BY g,k,v"},
                {"k, v, GROUPING(k,v) AS g", "ROLLUP(k,v) ORDER BY g,k,v"},
                {"w, GROUPING(w) AS g, SUM(w) AS s, MIN(w) AS lo, MAX(w) AS hi, AVG(w) AS a", "ROLLUP(w) ORDER BY g,w"},
                {"k, GROUPING(k) AS g, COUNT(DISTINCT v) AS n, SUM(w) AS s, SUM(DISTINCT v) AS vs", "ROLLUP(k) ORDER BY g,k"},
                {"k, GROUPING(k) AS g, SUM(v) AS s", "ROLLUP(k) HAVING GROUPING(k) = 1ul ORDER BY g,k"},
                {"k, GROUPING(k) AS g, SUM(w) AS s, Rank() OVER(PARTITION BY GROUPING(k) ORDER BY SUM(w) DESC) AS r", "ROLLUP(k) ORDER BY g,r,k"},
                {"k, GROUPING(k) AS g, COUNT(*) AS n", "k ORDER BY k"},
                {"k, GROUPING(k) AS g, COUNT(*) AS n", "GROUPING SETS ((k),(k),()) ORDER BY g,k"},
                {"k, v, GROUPING(k,v) AS g, COUNT(*) AS n", "ROLLUP((k,v)) ORDER BY g,k,v"},
                {"COUNT(*) AS n, SUM(w) AS s", "()"},
                {"k, v, GROUPING(k,v) AS g, COUNT(*) AS n", "GROUPING SETS (ROLLUP(k,v),()) ORDER BY g,k,v"},
                {"x, GROUPING(x) AS g, COUNT(*) AS n", "ROLLUP(k + 1 AS x) ORDER BY g,x"},
                {"k, GROUPING(k) AS g, COUNT(*) AS n", "GROUPING SETS ((k,k),(k),()) ORDER BY g,k"},
                {"COUNT(*) AS n, SUM(w) AS s", "GROUPING SETS ((),())"},
                {"k, GROUPING(k) AS g, SUM(w) AS w", "ROLLUP(k) ORDER BY g,w + 1,k"},
                {"k % 2 AS x, COUNT(*) AS n, SUM(w) AS s", "k % 2 ORDER BY x"},
                {"k % 2 AS x, v + 1 AS y, COUNT(*) AS n", "k % 2, v + 1 ORDER BY x,y"},
                {"k % 2 AS x, COUNT(*) AS n", "k % 2 HAVING k % 2 = 1 ORDER BY x"},
            };
            for (const auto& [projection, grouping] : cases) for (const bool empty : {false, true}) {
                const auto caseIndex = index / 2;
                const TString sql = TStringBuilder() << (caseIndex >= 21 ? "PRAGMA YqlSelect='force'; PRAGMA YqlSelectAllowUnnamedGroupByExpr; " : "")
                    << "SELECT " << projection << " FROM `/Root/Grouping`"
                    << (empty ? " WHERE id < 0" : "") << " GROUP BY " << grouping;
                Cerr << "grouping sets strategy " << strategy << ": " << sql << Endl;
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                // Pin existing compiler failures independently of native success.
                const char* failure = nullptr;
                if (strategy == 0) switch (caseIndex) {
                    case 10: failure = "Member not found: GroupingHint0"; break;
                    case 12: failure = "Source not support grouping hint"; break;
                    case 14: failure = "IsGroupByColumn(): requirement !GroupingCols_.empty() failed"; break;
                    case 18: failure = "Duplicate grouping column: k"; break;
                }

                if (failure) {
                    UNIT_ASSERT_C(!result.IsSuccess(), sql);
                    UNIT_ASSERT_C(result.GetIssues().ToString().find(failure) != std::string::npos, sql << "\n" << result.GetIssues().ToString());
                    ++index;
                    continue;
                }
                UNIT_ASSERT_C(result.IsSuccess(), sql << "\n" << result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                const auto actual = SnapshotRows(result.GetResultSet(0));
                if (caseIndex == 21) UNIT_ASSERT_VALUES_EQUAL(actual.Values, empty ? "[]" : "[[#;2u;12];[[0];2u;0];[[1];4u;4]]");
                if (caseIndex == 23) UNIT_ASSERT_VALUES_EQUAL(actual.Values, empty ? "[]" : "[[[1];4u]]");
                if (index == 0) UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[#;0u;2u;[12]];[[1];0u;3u;[3]];[[2];0u;2u;[0]];[[3];0u;1u;[1]];[#;1u;8u;[16]]]");
                if (index == 1) UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[#;1u;0u;#]]");
                if (strategy == 2 && index == 18) UNIT_ASSERT_VALUES_EQUAL(actual.Values,
                    "[[#;0u;2u;[12];[2]];[[1];0u;1u;[3];[2]];[[2];0u;0u;[0];#];[[3];0u;1u;[1];[2]];[#;1u;3u;[16];[4]]]");
                if (strategy == 2 && index == 28) UNIT_ASSERT_VALUES_EQUAL(actual.Values,
                    "[[#;[-1];0u;1u];[#;[3];0u;1u];[[1];#;0u;1u];[[1];[2];0u;2u];[[2];#;0u;2u];[[3];[2];0u;1u];[#;#;3u;8u]]");
                if (strategy == 2 && index == 29) UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[#;#;3u;0u]]");
                if (strategy == 2 && caseIndex == 19) UNIT_ASSERT_VALUES_EQUAL(actual.Values, empty ? "[[0u;#];[0u;#]]" : "[[8u;[16]];[8u;[16]]]");
                if (strategy == 2) reference.push_back(actual);
                else {
                    auto expected = reference.at(index);
                    if (strategy == 0 && caseIndex == 16) {
                        // The old optimizer drops duplicate empty grouping sets.
                        expected.Values = empty ? "[[#;#;3u;0u]]" :
                            "[[#;[-1];0u;1u];[#;[3];0u;1u];[[1];#;0u;1u];[[1];[2];0u;2u];[[2];#;0u;2u];[[3];[2];0u;1u];"
                            "[#;#;1u;2u];[[1];#;1u;3u];[[2];#;1u;2u];[[3];#;1u;1u];[#;#;3u;8u]]";
                    }
                    if (strategy == 0 && caseIndex == 19) expected.Values = empty ? "[[0u;#]]" : "[[8u;[16]]]";

                    AssertSame(actual, expected);
                }
                ++index;
            }
        }
    }

    Y_UNIT_TEST_TWIN(MixedDistinctAggregatesMatchLegacyOnPopulatedTables, ColumnStore) {
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(TString(
                "CREATE TABLE `/Root/MixedDistinct` (id Int32 NOT NULL, k Int32, v Int32, w Int64 NOT NULL, d Decimal(8,2), PRIMARY KEY(id))")
                + (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TValueBuilder rows; rows.BeginList();
            i32 id = 0;
            for (const auto& [key, value, required, decimal] : std::initializer_list<std::tuple<std::optional<i32>, std::optional<i32>, i64, const char*>>{
                    {1, 2, 2, "1.25"}, {1, 2, 4, "1.25"}, {1, std::nullopt, -3, nullptr},
                    {std::nullopt, -1, 6, "-2.50"}, {std::nullopt, 3, 6, "4.00"},
                    {2, std::nullopt, 0, nullptr}, {2, std::nullopt, 0, nullptr}}) {
                rows.AddListItem().BeginStruct().AddMember("id").Int32(++id)
                    .AddMember("k").OptionalInt32(key).AddMember("v").OptionalInt32(value)
                    .AddMember("w").Int64(required).AddMember("d");
                if (decimal) rows.BeginOptional().Decimal(TDecimalValue(decimal, 8, 2)).EndOptional();
                else rows.EmptyOptional(TTypeBuilder().Decimal({8, 2}).Build());
                rows.EndStruct();
            }
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/MixedDistinct", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            size_t index = 0;
            for (const auto* projection : {
                    "COUNT(DISTINCT v) AS n, COUNT(*) AS all_rows, COUNT(v) AS values, SUM(v) AS s, SUM(DISTINCT v) AS ds, MIN(DISTINCT v) AS lo, MAX(v) AS hi, SUM(w) AS ws",
                    "COUNT(DISTINCT w) AS n, SUM(DISTINCT w) AS s, MIN(w) AS lo, MAX(DISTINCT w) AS hi, COUNT(*) AS rows",
                    "COUNT(DISTINCT v) AS n, SUM(d) AS s, SUM(DISTINCT d) AS ds, MIN(DISTINCT d) AS lo, MAX(d) AS hi",
                    "COUNT(DISTINCT v) AS n, SOME(CAST(NULL AS Int32)) AS missing, SOME(7) AS present",
                    "COUNT(DISTINCT v + 1) AS n, SUM(v + 1) AS s, COUNT(DISTINCT w) AS nw",
                    "COUNT(DISTINCT v) AS n, AVG(v) AS a, AVG(DISTINCT v) AS da, COUNT(*) AS rows",
                    "COUNT(DISTINCT v) AS n, AVG(d) AS a, AVG(DISTINCT d) AS da, COUNT(d) AS rows",
                    "COUNT(DISTINCT w) AS n, AVG(w) AS a, AVG(DISTINCT w) AS da, SUM(w) AS s",
                    "AVG(DISTINCT v) AS av, AVG(DISTINCT d) AS ad, AVG(DISTINCT w) AS aw",
                    "COUNT(DISTINCT w) AS n, AVG(CAST(w AS Decimal(9,3))) AS a",
                    "COUNT(DISTINCT v) AS n, SUM(Interval('PT1S')) AS duration, SUM(CAST(NULL AS Interval)) AS missing_duration"}) {
                for (const auto* filter : {"", " WHERE k = 1", " WHERE k = 2", " WHERE id < 0"}) {
                    for (const bool grouped : {false, true}) {
                        const TString sql = TStringBuilder() << "SELECT " << (grouped ? "k, " : "") << projection
                            << " FROM `/Root/MixedDistinct`" << filter << (grouped ? " GROUP BY k ORDER BY k" : "");
                        Cerr << "mixed DISTINCT strategy " << strategy << ": " << sql << Endl;
                        const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();

                        UNIT_ASSERT_C(result.IsSuccess(), sql << "\n" << result.GetIssues().ToString());
                        UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                        const auto actual = SnapshotRows(result.GetResultSet(0));
                        if (index == 0) UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[3u;7u;4u;[6];[4];[-1];[3];[15]]]");
                        if (index == 6) UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[0u;0u;0u;#;#;#;#;#]]");
                        if (strategy == 2) reference.push_back(actual);
                        else AssertSame(actual, reference.at(index));
                        ++index;
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(DistinctAverageKeepsOriginalIntegerKeysAcrossPartitions, ColumnStore) {
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(TString(
                "CREATE TABLE `/Root/AverageKeys` (id Uint64 NOT NULL, g Int32 NOT NULL, v Int64 NOT NULL, PRIMARY KEY(id)) WITH (") +
                (ColumnStore ? "STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);" : "UNIFORM_PARTITIONS = 4);")).GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            constexpr i64 exact = i64{1} << 53;
            TValueBuilder rows; rows.BeginList();
            ui64 id = 0;
            for (const i64 value : {exact, exact + 1, -exact, exact + 1}) {
                ++id;
                rows.AddListItem().BeginStruct().AddMember("id").Uint64((id % 4) * (ui64{1} << 62) + id)
                    .AddMember("g").Int32(1).AddMember("v").Int64(value).EndStruct();
            }
            rows.EndList();
            const auto loaded = client.BulkUpsert("/Root/AverageKeys", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            std::size_t index = 0;
            for (const bool grouped : {false, true}) for (const bool empty : {false, true}) {
                const TString sql = TStringBuilder() << "SELECT " << (grouped ? "g, " : "")
                    << "AVG(DISTINCT v) AS distinct_avg, AVG(v) AS ordinary_avg, COUNT(DISTINCT v) AS n, COUNT(*) AS rows FROM `/Root/AverageKeys`"
                    << (empty ? " WHERE g = 0" : "") << (grouped ? " GROUP BY g ORDER BY g" : "");
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << "\n" << result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                const auto& set = result.GetResultSet(0);
                UNIT_ASSERT_VALUES_EQUAL(set.RowsCount(), grouped && empty ? 0 : 1);
                if (!empty) {
                    TResultSetParser values(set); UNIT_ASSERT(values.TryNextRow());
                    const auto actual = grouped ? values.ColumnParser("distinct_avg").GetDouble() : values.ColumnParser("distinct_avg").GetOptionalDouble().value();
                    UNIT_ASSERT_VALUES_EQUAL(actual, double(exact) / 3.0);
                    const auto ordinary = grouped ? values.ColumnParser("ordinary_avg").GetDouble() : values.ColumnParser("ordinary_avg").GetOptionalDouble().value();
                    UNIT_ASSERT_VALUES_EQUAL(ordinary, double(exact) / 2.0);
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("n").GetUint64(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("rows").GetUint64(), 4);
                } else if (!grouped) UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(set).Values, "[[#;#;0u;0u]]");
                const auto snapshot = SnapshotRows(set);
                if (strategy == 2) reference.push_back(snapshot);
                else AssertSame(snapshot, reference.at(index));
                ++index;
            }
        }
    }

    Y_UNIT_TEST_TWIN(SetOperationsMatchLegacyOnPopulatedTables, ColumnStore) {
        TVector<std::pair<TString, TString>> queries;
        for (const auto& [operation, values] : std::initializer_list<std::pair<const char*, const char*>>{
                {"UNION", "[[#];[[1]];[[2]];[[3]];[[4]]]"},
                {"UNION DISTINCT", "[[#];[[1]];[[2]];[[3]];[[4]]]"},
                {"UNION ALL", "[[#];[#];[#];[[1]];[[1]];[[1]];[[1]];[[1]];[[2]];[[3]];[[4]];[[4]];[[4]]]"},
                {"INTERSECT", "[[#];[[1]];[[4]]]"},
                {"INTERSECT ALL", "[[#];[[1]];[[1]];[[4]]]"},
                {"EXCEPT", "[[[2]]]"},
                {"EXCEPT ALL", "[[#];[[1]];[[2]]]"}}) {
            queries.emplace_back(TStringBuilder() << "SELECT v FROM `/Root/SetLeft` " << operation
                << " SELECT v FROM `/Root/SetRight` ORDER BY v", values);
            for (const bool emptyLeft : {false, true})
                queries.emplace_back(TStringBuilder() << "SELECT v FROM `/Root/SetLeft`"
                    << (emptyLeft ? " WHERE id < 0" : "") << " " << operation
                    << " SELECT v FROM `/Root/SetRight`" << (emptyLeft ? "" : " WHERE id < 0") << " ORDER BY v", "");
        }
        for (const auto* sql : {
                "SELECT 1 AS x UNION SELECT 2 AS x INTERSECT SELECT 2 AS x ORDER BY x",
                "SELECT 1 AS x UNION SELECT 2 AS x EXCEPT SELECT 1 AS x ORDER BY x",
                "SELECT v FROM `/Root/SetLeft` EXCEPT ALL SELECT v FROM `/Root/SetRight` EXCEPT ALL SELECT 1 AS v ORDER BY v",
                "SELECT 1 AS b, 2 AS a UNION ALL SELECT 3 AS a, 4 AS b ORDER BY a, b",
                "SELECT 1 AS z UNION ALL SELECT 2l AS a ORDER BY z, a",
                "SELECT 1 AS z UNION SELECT 2 AS a, 3 AS z ORDER BY z, a",
                "SELECT 1 AS z UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS z ORDER BY z, a",
                "SELECT 1 AS z, 2 AS a UNION ALL SELECT 3 AS z, 4 AS b ORDER BY z, a, b",
                "SELECT v AS z, v AS a FROM `/Root/SetLeft` INTERSECT SELECT v AS a, v AS z FROM `/Root/SetRight` ORDER BY a, z",
                "SELECT NULL AS v INTERSECT SELECT NULL AS v",
                "SELECT NULL AS v UNION SELECT NULL AS v",
                "SELECT NULL AS v EXCEPT ALL SELECT NULL AS v",
                "SELECT NULL AS v FROM `/Root/SetLeft` WHERE id < 0 UNION SELECT NULL AS v FROM `/Root/SetRight` WHERE id < 0",
                "SELECT CAST('nan' AS Double) AS v INTERSECT SELECT CAST('nan' AS Double) AS v",
                "SELECT COUNT(*) AS n FROM (SELECT 0.0 AS v UNION SELECT -0.0 AS v) AS q",
                "SELECT CAST(v AS Int64) AS v FROM `/Root/SetLeft` INTERSECT SELECT v FROM `/Root/SetRight` ORDER BY v",
                "SELECT CAST(v AS Decimal(12,2)) AS v FROM `/Root/SetLeft` EXCEPT ALL SELECT CAST(v AS Decimal(10,1)) AS v FROM `/Root/SetRight` ORDER BY v",
                "SELECT v FROM `/Root/SetLeft` UNION ALL SELECT v FROM `/Root/SetRight` ORDER BY v DESC LIMIT 4 OFFSET 2",
                "SELECT v FROM `/Root/SetLeft` UNION ALL (SELECT v FROM `/Root/SetRight` ORDER BY v DESC LIMIT 2)",
                "SELECT v FROM (SELECT v FROM `/Root/SetLeft` ORDER BY v DESC LIMIT 2) AS q UNION ALL SELECT v FROM `/Root/SetRight` ORDER BY v",
                "WITH q AS (SELECT v FROM `/Root/SetLeft`) SELECT v FROM q INTERSECT ALL SELECT v FROM q ORDER BY v",
                "SELECT COUNT(*) AS n FROM (SELECT v FROM `/Root/SetLeft` EXCEPT ALL SELECT v FROM `/Root/SetRight`) AS q",
                "SELECT (SELECT COUNT(*) FROM (SELECT v FROM `/Root/SetLeft` INTERSECT ALL SELECT v FROM `/Root/SetRight`) AS q) AS n",
                "SELECT EXISTS(SELECT v FROM `/Root/SetLeft` EXCEPT ALL SELECT v FROM `/Root/SetRight`) AS found",
                "SELECT id FROM `/Root/SetLeft` WHERE v IN (SELECT v FROM `/Root/SetLeft` INTERSECT ALL SELECT v FROM `/Root/SetRight`) ORDER BY id",
                "SELECT id FROM `/Root/SetLeft` AS a WHERE EXISTS(SELECT v FROM `/Root/SetRight` AS b WHERE b.v = a.v INTERSECT SELECT v FROM `/Root/SetRight`) ORDER BY id",
                "SELECT id FROM `/Root/SetLeft` AS a WHERE EXISTS(SELECT v FROM `/Root/SetRight` AS b WHERE b.v = a.v INTERSECT ALL SELECT v FROM `/Root/SetRight`) ORDER BY id",
                "SELECT * FROM (SELECT 1 AS z UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS z) AS q ORDER BY z, a",
                "SELECT * FROM (SELECT 1 AS z, 2 AS a UNION ALL SELECT 3 AS z, 4 AS b) AS q ORDER BY z, a, b",
                "SELECT NULL AS v INTERSECT SELECT NULL AS v ORDER BY v",
                "SELECT z, a FROM (SELECT 1 AS z, 2 AS a INTERSECT SELECT 2 AS a, 1 AS z "
                    "INTERSECT SELECT 1 AS z, 2 AS a) AS q"}) {
            // Sort outside a locally limited operand as well, so comparison
            // does not depend on execution order of the union's branches.
            TString query(sql);
            if (query.EndsWith("LIMIT 2)")) query = "SELECT v FROM (" + query + ") AS q ORDER BY v";
            queries.emplace_back(std::move(query), "");
        }
        // Independent value oracles where the old frontend cannot execute or
        // the older translator uses a different result-column order.
        queries[27].second = queries[48].second = "[[#;[2]];[[1];#];[[3];#]]";
        queries[28].second = queries[49].second = "[[1;[2];#];[3;#;[4]]]";
        queries[41].second = "[[#];[#];[[1]];[[1]];[[1]];[[2]];[[4]]]";
        TVector<TResultSnapshot> reference;
        TString failures;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            for (const bool right : {false, true}) {
                const TString path = right ? "/Root/SetRight" : "/Root/SetLeft";
                const auto created = session.ExecuteSchemeQuery("CREATE TABLE `" + path +
                    "` (id Int32 NOT NULL, v Int32, PRIMARY KEY(id))" + (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                const TVector<std::optional<i32>> data = right ? TVector<std::optional<i32>>{std::nullopt, 1, 1, 3, 4, 4} :
                    TVector<std::optional<i32>>{std::nullopt, std::nullopt, 1, 1, 1, 2, 4};
                TValueBuilder rows;
                rows.BeginList();
                i32 id = 0;
                for (const auto value : data)
                    rows.AddListItem().BeginStruct().AddMember("id").Int32(id++).AddMember("v").OptionalInt32(value).EndStruct();
                rows.EndList();
                const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            }
            for (size_t index = 0; index < queries.size(); ++index) {
                const auto& [sql, values] = queries[index];
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                const TString context = TStringBuilder() << "strategy=" << strategy << " query=" << index << " " << sql;
                if (strategy == 0 && sql.StartsWith("WITH ")) {
                    UNIT_ASSERT(!result.IsSuccess());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("WITH CTE is available only with YqlSelect") != std::string::npos, context);
                    continue;
                }
                if (strategy == 2 && sql.Contains("b.v = a.v INTERSECT ALL")) {
                    UNIT_ASSERT(!result.IsSuccess());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("correlated INTERSECT ALL/EXCEPT ALL") != std::string::npos, result.GetIssues().ToString());
                    reference.emplace_back();
                    continue;
                }
                // Pin failures in the existing compilers; native must still
                // execute every independent case and match the working path.
                TString oldError;
                if (strategy == 0 && sql.Contains("b.v = a.v")) oldError = "Member not found: a";

                if (!oldError.empty()) {
                    UNIT_ASSERT_C(!result.IsSuccess(), context);
                    UNIT_ASSERT_C(result.GetIssues().ToString().find(oldError.c_str()) != std::string::npos,
                        TStringBuilder() << context << ": " << result.GetIssues().ToString());
                    continue;
                }
                if (!result.IsSuccess()) {
                    failures += context + ": " + TString(result.GetIssues().ToString()) + "\n";
                    if (strategy == 2) reference.emplace_back();
                    continue;
                }
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, context);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                const auto actual = SnapshotRows(result.GetResultSet(0));
                if (strategy == 0 && (index == 27 || index == 28 || index == 48 || index == 49)) {
                    // YqlSelect preserves a common field prefix; the older
                    // translator loses that order and emits lexical fields.
                    auto columns = reference[index].Columns;
                    std::sort(columns.begin(), columns.end());
                    UNIT_ASSERT_VALUES_EQUAL_C(actual.Columns, columns, context);
                    UNIT_ASSERT_VALUES_EQUAL_C(actual.Values, (index == 27 || index == 48) ?
                        "[[[2];#];[#;[1]];[#;[3]]]" : "[[[2];#;1];[#;[4];3]]", context);
                    continue;
                }
                if (!values.empty()) UNIT_ASSERT_VALUES_EQUAL_C(actual.Values, values, context);
                if (strategy == 2 && (index == 27 || index == 28 || index == 48 || index == 49)) {
                    UNIT_ASSERT_VALUES_EQUAL(actual.Columns[0].first, "z");
                    UNIT_ASSERT_VALUES_EQUAL(actual.Columns[1].first, "a");
                    if (actual.Columns.size() == 3) UNIT_ASSERT_VALUES_EQUAL(actual.Columns[2].first, "b");
                }
                if (strategy == 2) reference.push_back(actual);
                else if (!reference.at(index).Values.empty() &&
                        (actual.Values != reference[index].Values || actual.Columns != reference[index].Columns)) {
                    failures += context + ": actual=" + actual.Values + " native=" + reference[index].Values + " columns=";
                    for (const auto& [name, type] : actual.Columns) failures += name + ":" + type + ",";
                    failures += "\n";
                }
            }
        }
        UNIT_ASSERT_C(failures.empty(), failures);
    }

    Y_UNIT_TEST(NativeOlapFiltersMatchComputeOnPopulatedTables) {
        TVector<TResultSnapshot> expected;
        for (const int strategy : {0, 2, 3, 1}) {
            const bool push = strategy == 1 || strategy == 3;
            auto settings = NativeSettings(strategy >= 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY :
                NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED);
            settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(strategy < 2);
            settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapScalarApply(false);
            settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
            NKikimrKqp::TKqpSetting setting;
            setting.SetName("OptEnableOlapPushdown"); setting.SetValue(push ? "true" : "false");
            settings.SetKqpSettings({setting});
            TKikimrRunner runner(settings);
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery(R"(CREATE TABLE `/Root/NativeOlap` (
                id Int32 NOT NULL, s Int64, u Uint64, x Int32, y Int32, f Double, t Utf8, b Bool, PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);)").GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            struct TRow { std::optional<i64> S; std::optional<ui64> U; std::optional<i32> X, Y; std::optional<double> F; const char* Text; };
            TValueBuilder rows;
            rows.BeginList();
            i32 id = 0;
            for (const auto& row : std::initializer_list<TRow>{
                    {-1, std::numeric_limits<ui64>::max(), -10, 2, 1.5, "a"},
                    {0, 0, 0, std::nullopt, 0., nullptr}, {1, 1, 10, 2, std::numeric_limits<double>::infinity(), "beta"},
                    {std::numeric_limits<i64>::max(), ui64{1} << 63, std::nullopt, 0, std::numeric_limits<double>::quiet_NaN(), "éclair"},
                    {std::numeric_limits<i64>::lowest(), 0, 20, -1, -std::numeric_limits<double>::infinity(), ""},
                    {1, std::numeric_limits<ui64>::max(), std::numeric_limits<i32>::max(), std::numeric_limits<i32>::lowest(), std::nullopt, "A"},
                    {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, nullptr},
                    {-1, 1, std::numeric_limits<i32>::lowest(), -1, -1.5, "alpha"}}) {
                rows.AddListItem().BeginStruct().AddMember("id").Int32(++id).AddMember("s").OptionalInt64(row.S)
                    .AddMember("u").OptionalUint64(row.U).AddMember("x").OptionalInt32(row.X).AddMember("y").OptionalInt32(row.Y)
                    .AddMember("b").OptionalBool(row.X ? std::optional<bool>{*row.X > 0} : std::nullopt)
                    .AddMember("f").OptionalDouble(row.F).AddMember("t").OptionalUtf8(row.Text ? std::optional<std::string>{row.Text} : std::nullopt).EndStruct();
            }
            rows.EndList();
            const auto loaded = client.BulkUpsert("/Root/NativeOlap", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            size_t index = 0;
            // Pin the pre-PR admission policy with scalar apply disabled.
            // Boolean members and scalar IF/COALESCE operands remain compute
            // predicates; storage execution capability alone does not admit them.
            for (const auto& [predicate, pushable] : std::initializer_list<std::pair<const char*, bool>>{
                    {"b", false}, {"NOT b", false}, {"b OR x < 0", false}, {"b AND y IS NULL", true},
                    {"id > 0", true}, {"x > 0", true}, {"x <= 0", true}, {"x = 10", true}, {"x != 10", true},
                    {"x IS NULL", true}, {"x IS NOT NULL", true}, {"NOT (x > 0)", true},
                    {"x > 0 OR y IS NULL", true}, {"x > 0 AND y < 0", true}, {"(x = 0 OR x = 10) AND (y < 3 OR y IS NULL)", true},
                    {"x + 1 > 0", true}, {"-x < 0", true}, {"Abs(x) > 1", true}, {"x / y > 2", true}, {"x % y > 0", true},
                    {"COALESCE(x, 0) > 1", false}, {"IF(x > 0, y, 0) >= 0", false}, {"s < u", true}, {"u <= s", true}, {"s = u", true},
                    {"f < 0.0", true}, {"f >= 1.5", true}, {"f != 1.5", true}, {"t = 'a'", true}, {"t < 'z'", true},
                    {"t IS NULL", true}, {"t IN ('a', 'beta', 'A')", true},
                    {"false OR x > 0", true}, {"x > 0 AND true", true}, {"x > $p", true}, {"StartsWith(t, 'a')", true}, {"EndsWith(t, 'a')", true},
                    {"StartsWith(t, 'a') AND x > 0", true}, {"StartsWith(t, 'a') OR x > 0", true},
                    {"NOT StartsWith(t, 'a')", true}, {"COALESCE(StartsWith(t, 'a'), true)", true},
                    {"StartsWith(t, 'a') OR NOT StartsWith(t, 'a')", true},
                    {"t LIKE '%a%'", true}, {"Length(t) > 1u", false}, {"x = x", false}}) {
                const TString sql = TStringBuilder() << "DECLARE $p AS Int32?; SELECT id FROM `/Root/NativeOlap` WHERE " << predicate << " ORDER BY id";
                const auto params = TParamsBuilder().AddParam("$p").OptionalInt32(1).Build().Build();
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(), params).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "strategy=" << strategy << " " << sql << ": " << result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                const auto actual = SnapshotRows(result.GetResultSet(0));
                if (strategy == 0) expected.push_back(actual);
                else { Cerr << "OLAP strategy " << strategy << ": " << predicate << Endl; AssertSame(actual, expected.at(index)); }
                const auto explain = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(), params,
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetAst());
                const auto ast = *explain.GetStats()->GetAst();
                if (strategy < 2 && TStringBuf(predicate) == "id > 0") {
                    // Range extraction consumes this entire key predicate,
                    // independently of the OLAP-filter setting. Require its
                    // exact storage restriction, not just an absent filter.
                    UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos, sql);
                    UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[1];[2];[3];[4];[5];[6];[7];[8]]");
                    UNIT_ASSERT(explain.GetStats()->GetPlan());
                    NJson::TJsonValue plan;
                    NJson::ReadJsonTree(*explain.GetStats()->GetPlan(), &plan, true);
                    size_t reads = 0;
                    const auto inspect = [&](auto&& self, const NJson::TJsonValue& node) -> void {
                        if (node.IsMap()) {
                            const auto& fields = node.GetMapSafe();
                            const auto name = fields.find("Name");
                            if (name != fields.end() && (name->second.GetStringSafe() == "TableRangeScan" || name->second.GetStringSafe() == "TableFullScan")) {
                                ++reads;
                                UNIT_ASSERT_VALUES_EQUAL(name->second.GetStringSafe(), "TableRangeScan");
                                UNIT_ASSERT_VALUES_EQUAL(fields.at("Storage").GetStringSafe(), "Column");
                                UNIT_ASSERT_VALUES_EQUAL(fields.at("ReadRangesExpectedSize").GetStringSafe(), "1");
                                const auto& keys = fields.at("ReadRangesKeys").GetArraySafe();
                                UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
                                UNIT_ASSERT_VALUES_EQUAL(keys[0].GetStringSafe(), "id");
                                const auto& lower = fields.at("ReadRangeLower").GetMapSafe();
                                const auto& upper = fields.at("ReadRangeUpper").GetMapSafe();
                                UNIT_ASSERT(!lower.at("Inclusive").GetBooleanSafe());
                                UNIT_ASSERT(upper.at("Inclusive").GetBooleanSafe());
                                const auto& values = lower.at("Values").GetArraySafe();
                                UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
                                UNIT_ASSERT_VALUES_EQUAL(values[0].GetStringSafe(), "0");
                                UNIT_ASSERT(upper.at("Values").GetArraySafe().empty());
                            }
                            for (const auto& [key, value] : fields) { Y_UNUSED(key); self(self, value); }
                        } else if (node.IsArray()) for (const auto& value : node.GetArraySafe()) self(self, value);
                    };
                    inspect(inspect, plan.GetMapSafe().at("SimplifiedPlan"));
                    UNIT_ASSERT_VALUES_EQUAL(reads, 1);
                } else if (strategy != 3) {
                    UNIT_ASSERT_VALUES_EQUAL_C(ast.find("KqpOlapFilter") != std::string::npos, push && pushable, sql);
                }
                ++index;
            }
            for (const auto& [sql, values] : std::initializer_list<std::pair<const char*, const char*>>{
                    {"SELECT id FROM `/Root/NativeOlap` WHERE x > 0 ORDER BY id LIMIT 2", "[[3];[5]]"},
                    {"SELECT id FROM `/Root/NativeOlap` WHERE x > 0 ORDER BY id LIMIT 0", "[]"},
                    {"SELECT id FROM `/Root/NativeOlap` WHERE x > 0 ORDER BY id DESC LIMIT 2", "[[6];[5]]"},
                    {"SELECT id FROM `/Root/NativeOlap` WHERE x > 0 AND Length(t) > 1u ORDER BY id", "[[3]]"},
                    {"SELECT COUNT(*) AS n FROM `/Root/NativeOlap` WHERE x > 0", nullptr}}) {
                const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << sql << ": " << result.GetIssues().ToString());
                const auto actual = SnapshotRows(result.GetResultSet(0));
                if (TStringBuf(sql).Contains("LIMIT 2")) {
                    const auto explain = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                    UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                    UNIT_ASSERT(explain.GetStats() && explain.GetStats()->GetAst());
                    UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStats()->GetAst()->find("ItemsLimit") != std::string::npos, push, sql);
                }
                if (values) UNIT_ASSERT_VALUES_EQUAL(actual.Values, values);
                else {
                    TResultSetParser parsed(result.GetResultSet(0)); UNIT_ASSERT(parsed.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(parsed.ColumnParser("n").GetUint64(), 3);
                }
                if (strategy == 0) expected.push_back(actual);
                else AssertSame(actual, expected.at(index));
                ++index;
            }
            for (const auto parameter : std::initializer_list<std::optional<i32>>{std::nullopt, std::numeric_limits<i32>::lowest(), std::numeric_limits<i32>::max()}) {
                const auto params = TParamsBuilder().AddParam("$p").OptionalInt32(parameter).Build().Build();
                const auto result = runner.GetQueryClient().ExecuteQuery(
                    "DECLARE $p AS Int32?; SELECT id FROM `/Root/NativeOlap` WHERE x > $p ORDER BY id", NQuery::TTxControl::NoTx(), params).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto actual = SnapshotRows(result.GetResultSet(0));
                UNIT_ASSERT_VALUES_EQUAL(actual.Values, parameter && *parameter == std::numeric_limits<i32>::lowest() ? "[[1];[2];[3];[5];[6]]" : "[]");
                if (strategy == 0) expected.push_back(actual);
                else AssertSame(actual, expected.at(index));
                ++index;
            }
        }
    }

    Y_UNIT_TEST_TWIN(MixedIntegerJoinsMatchLegacyOnPopulatedTables, ColumnStore) {
        TVector<TResultSnapshot> expected;
        for (const int strategy : {0, 2}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            for (const bool right : {false, true}) {
                const TString path = right ? "/Root/UnsignedKeys" : "/Root/SignedKeys";
                const auto created = session.ExecuteSchemeQuery("CREATE TABLE `" + path + "` (id Int32 NOT NULL, k " +
                    (right ? "Uint64" : "Int64") + ", PRIMARY KEY(id))" + (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                TValueBuilder rows;
                rows.BeginList();
                if (right) {
                    i32 id = 10;
                    for (const auto value : std::initializer_list<std::optional<ui64>>{0, 1, ui64(std::numeric_limits<i64>::max()),
                            ui64(std::numeric_limits<i64>::max()) + 1, std::numeric_limits<ui64>::max(), std::nullopt, 1})
                        rows.AddListItem().BeginStruct().AddMember("id").Int32(id++).AddMember("k").OptionalUint64(value).EndStruct();
                } else {
                    i32 id = 1;
                    for (const auto value : std::initializer_list<std::optional<i64>>{std::numeric_limits<i64>::lowest(), -1, 0, 1, std::numeric_limits<i64>::max(), std::nullopt})
                        rows.AddListItem().BeginStruct().AddMember("id").Int32(id++).AddMember("k").OptionalInt64(value).EndStruct();
                }
                rows.EndList();
                const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            }
            size_t index = 0;
            for (const auto& [kind, projection, values] : std::initializer_list<std::tuple<const char*, const char*, const char*>>{
                    {"INNER", "a.id AS l, b.id AS r", "[[3;10];[4;11];[4;16];[5;12]]"},
                    {"LEFT", "a.id AS l, b.id AS r", "[[1;#];[2;#];[3;[10]];[4;[11]];[4;[16]];[5;[12]];[6;#]]"},
                    {"RIGHT", "a.id AS l, b.id AS r", "[[#;13];[#;14];[#;15];[[3];10];[[4];11];[[4];16];[[5];12]]"},
                    {"FULL", "a.id AS l, b.id AS r", "[[#;[13]];[#;[14]];[#;[15]];[[1];#];[[2];#];[[3];[10]];[[4];[11]];[[4];[16]];[[5];[12]];[[6];#]]"},
                    {"LEFT SEMI", "a.id AS l", "[[3];[4];[5]]"}, {"LEFT ONLY", "a.id AS l", "[[1];[2];[6]]"},
                    {"RIGHT SEMI", "b.id AS r", "[[10];[11];[12];[16]]"}, {"RIGHT ONLY", "b.id AS r", "[[13];[14];[15]]"}}) {
                for (const bool reverse : {false, true}) {
                    const TString sql = TStringBuilder() << "SELECT " << projection << " FROM `/Root/SignedKeys` AS a " << kind
                        << " JOIN `/Root/UnsignedKeys` AS b ON " << (reverse ? "b.k = a.k" : "a.k = b.k") << " ORDER BY "
                        << (TString(kind).Contains("RIGHT ") ? "r" : TString(kind).Contains("LEFT ") ? "l" : "l,r");
                    const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();

                    UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "strategy=" << strategy << " " << sql << ": " << result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                    const auto actual = SnapshotRows(result.GetResultSet(0));
                    UNIT_ASSERT_VALUES_EQUAL_C(actual.Values, values, TStringBuilder() << "strategy=" << strategy << " " << sql);
                    if (strategy == 0) expected.push_back(actual);
                    else AssertSame(actual, expected.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST(RuntimeMixedNumericPromotionMatchesBothLegacyPaths) {
        TString sql = "DECLARE $s AS Int64?; DECLARE $u AS Uint64?; SELECT ";
        size_t column = 0;
        auto append = [&](const TString& expression) {
            if (column) sql += ",";
            sql += TStringBuilder() << expression << " AS c" << column++;
        };
        for (const auto* a : {"Int8", "Int16", "Int32", "Int64"}) for (const auto* b : {"Uint8", "Uint16", "Uint32", "Uint64"})
            for (const bool reverse : {false, true}) {
                const TString signedInput = TStringBuilder() << "CAST($s AS " << a << ")";
                const TString unsignedInput = TStringBuilder() << "CAST($u AS " << b << ")";
                const auto& left = reverse ? unsignedInput : signedInput;
                const auto& right = reverse ? signedInput : unsignedInput;
                for (const auto* op : {"+", "-", "*", "/", "%"}) append(TStringBuilder() << left << op << right);
                append(TStringBuilder() << "IF($s < 0," << left << "," << right << ")");
                append(TStringBuilder() << "COALESCE(" << left << "," << right << ")");
            }
        for (const auto* expression : {"CAST($s AS Float) + $u", "$s + CAST($u AS Float)",
                "CAST($s AS Float) + CAST($u AS Double)", "IF($s < 0, Float('inf'), $u)",
                "COALESCE(CAST($s AS Float), $u)", "CAST($s AS Float) < $u"}) append(expression);
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto session = runner.GetTableClient().CreateSession().GetValueSync().GetSession();
            size_t index = 0;
            for (const auto& [s, u] : std::initializer_list<std::pair<std::optional<i64>, std::optional<ui64>>>{
                    {-1, std::numeric_limits<ui64>::max()}, {std::numeric_limits<i64>::lowest(), ui64{1} << 63},
                    {std::numeric_limits<i64>::max(), ui64{1} << 63}, {-1, 0}, {0, 0}, {1, 1},
                    {-128, 255}, {-32768, 65535}, {-2147483648, 4294967295}, {127, 128}, {32767, 32768},
                    {2147483647, 2147483648}, {4611686293305294849LL, 9223372586610589697ULL},
                    {std::nullopt, 1}, {1, std::nullopt}, {std::nullopt, std::nullopt}}) {
                const auto params = TParamsBuilder().AddParam("$s").OptionalInt64(s).Build().AddParam("$u").OptionalUint64(u).Build().Build();
                const auto actual = Snapshot(session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync());
                if (strategy == 2) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                ++index;
            }
        }
    }

    Y_UNIT_TEST(MixedNumericRangeFilteringMatchesPredicateEvaluation) {
        TKikimrRunner runner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        auto client = runner.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        struct TKeyType { const char* Name; __int128 Minimum; __int128 Maximum; };
        const TKeyType keyTypes[]{
            {"Int32", std::numeric_limits<i32>::min(), std::numeric_limits<i32>::max()},
            {"Uint32", 0, std::numeric_limits<ui32>::max()},
            {"Int64", std::numeric_limits<i64>::min(), std::numeric_limits<i64>::max()},
            {"Uint64", 0, std::numeric_limits<ui64>::max()},
        };
        const TVector<std::pair<TString, TValue>> bounds{
            {"Int64?", TValueBuilder().OptionalInt64(-1).Build()},
            {"Int64?", TValueBuilder().OptionalInt64(std::numeric_limits<i64>::min()).Build()},
            {"Int64?", TValueBuilder().OptionalInt64(std::numeric_limits<i64>::max()).Build()},
            {"Uint64?", TValueBuilder().OptionalUint64(0).Build()},
            {"Uint64?", TValueBuilder().OptionalUint64(ui64{1} << 63).Build()},
            {"Uint64?", TValueBuilder().OptionalUint64(std::numeric_limits<ui64>::max()).Build()},
            {"Float?", TValueBuilder().OptionalFloat(16777216.f).Build()},
            {"Float?", TValueBuilder().OptionalFloat(std::numeric_limits<float>::quiet_NaN()).Build()},
            {"Double?", TValueBuilder().OptionalDouble(16777216.).Build()},
            {"Double?", TValueBuilder().OptionalDouble(9007199254740992.).Build()},
            {"Double?", TValueBuilder().OptionalDouble(2147483647.5).Build()},
            {"Double?", TValueBuilder().OptionalDouble(std::numeric_limits<double>::infinity()).Build()},
            {"Int64?", TValueBuilder().OptionalInt64(std::nullopt).Build()},
            {"Double?", TValueBuilder().OptionalDouble(std::nullopt).Build()},
        };
        for (const auto& keyType : keyTypes) {
            const TString path = TStringBuilder() << "/Root/Range" << keyType.Name;
            const auto created = session.ExecuteSchemeQuery(TStringBuilder() << "CREATE TABLE `" << path << "` (key "
                << keyType.Name << ", tag Uint32 NOT NULL, PRIMARY KEY(key));").GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            TVector<__int128> keys{keyType.Minimum, keyType.Minimum + 1, keyType.Maximum - 1, keyType.Maximum,
                -1, 0, 1, 16777215, 16777216, 16777217, 16777218,
                __int128{9007199254740991LL}, __int128{9007199254740992LL}, __int128{9007199254740993LL}};
            std::sort(keys.begin(), keys.end());
            keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
            TValueBuilder rows;
            rows.BeginList();
            ui32 tag = 0;
            const auto row = [&](std::optional<__int128> key) {
                rows.AddListItem().BeginStruct().AddMember("tag").Uint32(tag++).AddMember("key");
                const TString name = keyType.Name;
                if (name == "Int32") rows.OptionalInt32(key ? std::optional<i32>{static_cast<i32>(*key)} : std::nullopt);
                else if (name == "Uint32") rows.OptionalUint32(key ? std::optional<ui32>{static_cast<ui32>(*key)} : std::nullopt);
                else if (name == "Int64") rows.OptionalInt64(key ? std::optional<i64>{static_cast<i64>(*key)} : std::nullopt);
                else rows.OptionalUint64(key ? std::optional<ui64>{static_cast<ui64>(*key)} : std::nullopt);
                rows.EndStruct();
            };
            row(std::nullopt);
            for (const auto key : keys) if (key >= keyType.Minimum && key <= keyType.Maximum) row(key);
            rows.EndList();
            const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            for (const auto& [boundType, bound] : bounds)
            for (const auto* op : {"=", "<", "<=", ">", ">="})
            for (const bool computed : {false, true}) {
                const TString predicate = TStringBuilder() << "key " << op << (computed ? " IF($choose, $bound, $other)" : " $bound");
                const TString sql = TStringBuilder() << "PRAGMA YqlSelect='force'; DECLARE $bound AS " << boundType
                    << "; DECLARE $other AS " << boundType << "; DECLARE $choose AS Bool; SELECT tag FROM `" << path
                    << "` WHERE " << predicate << " ORDER BY tag; SELECT tag, COALESCE(" << predicate
                    << ", false) AS matched FROM `" << path << "` ORDER BY tag;";
                auto params = TParamsBuilder().AddParam("$bound", bound).AddParam("$other", bound)
                    .AddParam("$choose").Bool(!computed).Build().Build();
                const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, sql);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
                TVector<ui32> filtered, evaluated;
                TResultSetParser selected(result.GetResultSet(0)), projected(result.GetResultSet(1));
                while (selected.TryNextRow()) filtered.push_back(selected.ColumnParser("tag").GetUint32());
                while (projected.TryNextRow()) if (projected.ColumnParser("matched").GetBool())
                    evaluated.push_back(projected.ColumnParser("tag").GetUint32());
                UNIT_ASSERT_VALUES_EQUAL_C(filtered, evaluated, sql);
            }
        }
    }

    Y_UNIT_TEST(RuntimeMixedIntegerComparisonsMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        TString sql = "DECLARE $s AS Int64?; DECLARE $u AS Uint64?; SELECT ";
        size_t column = 0;
        for (const auto* a : {"Int8", "Int16", "Int32", "Int64"}) for (const auto* b : {"Uint8", "Uint16", "Uint32", "Uint64"})
            for (const auto* op : {"=", "!=", "<", "<=", ">", ">="}) for (const bool reverse : {false, true}) {
                const TString left = TStringBuilder() << "CAST($s AS " << a << ")", right = TStringBuilder() << "CAST($u AS " << b << ")";
                if (column) sql += ",";
                sql += TStringBuilder() << (reverse ? right : left) << op << (reverse ? left : right) << " AS c" << column++;
            }
        for (const auto& [signedValue, unsignedValue] : std::initializer_list<std::pair<std::optional<i64>, std::optional<ui64>>>{
                {-1, std::numeric_limits<ui64>::max()}, {std::numeric_limits<i64>::lowest(), ui64{1} << 63},
                {std::numeric_limits<i64>::max(), ui64{1} << 63}, {std::numeric_limits<i64>::max(), ui64(std::numeric_limits<i64>::max())},
                {-1, 0}, {0, 0}, {1, 1}, {127, 128}, {255, 255}, {32767, 32768}, {65535, 65535},
                {2147483647, 2147483648}, {4294967295, 4294967295}, {std::nullopt, 1}, {1, std::nullopt}, {std::nullopt, std::nullopt}}) {
            const auto params = TParamsBuilder().AddParam("$s").OptionalInt64(signedValue).Build().AddParam("$u").OptionalUint64(unsignedValue).Build().Build();
            AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
        }
    }

    Y_UNIT_TEST_TWIN(FullJoinResidualsAreRejectedOrDelegated, ColumnStore) {
        std::optional<TResultSnapshot> legacy;
        for (const int strategy : {0, 2, 3}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 3 ? NKikimrConfig::TTableServiceConfig::NATIVE_AUTO :
                strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableInlineJoinFiltersAfterCBO(false);
            service->SetAllowOlapDataQuery(true);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            for (const bool right : {false, true}) {
                const TString path = right ? "/Root/JoinB" : "/Root/JoinA";
                const auto created = session.ExecuteSchemeQuery("CREATE TABLE `" + path +
                    "` (id Int32 NOT NULL, k Int32 NOT NULL, v Int32, PRIMARY KEY(id))" +
                    (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                TValueBuilder rows;
                rows.BeginList();
                for (const auto& [id, key, value] : right ? TVector<std::tuple<i32, i32, i32>>{{3, 1, 15}, {4, 1, 5}, {5, 3, 30}} :
                        TVector<std::tuple<i32, i32, i32>>{{1, 1, 10}, {2, 2, 20}})
                    rows.AddListItem().BeginStruct().AddMember("id").Int32(id).AddMember("k").Int32(key)
                        .AddMember("v").OptionalInt32(value).EndStruct();
                rows.EndList();
                const auto loaded = client.BulkUpsert(path, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(loaded.IsSuccess(), loaded.GetIssues().ToString());
            }
            const auto result = runner.GetQueryClient().ExecuteQuery(
                "PRAGMA YqlSelect='force'; SELECT a.id AS l, b.id AS r FROM `/Root/JoinA` AS a FULL JOIN `/Root/JoinB` AS b "
                "ON a.k = b.k AND a.v > b.v ORDER BY l, r", NQuery::TTxControl::NoTx()).GetValueSync();
            const auto issues = result.GetIssues().ToString();
            if (strategy == 2) {
                UNIT_ASSERT(!result.IsSuccess());
                UNIT_ASSERT_C(issues.find("FULL JOIN requires only column equality keys") != std::string::npos, issues);
                UNIT_ASSERT_C(issues.find("[native_frontend_fallback]") == std::string::npos, issues);
                continue;
            }
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto actual = SnapshotRows(result.GetResultSet(0));

                UNIT_ASSERT_VALUES_EQUAL(actual.Values, "[[#;[3]];[#;[5]];[[1];[4]];[[2];#]]");
                if (strategy == 0) legacy = actual;
                else {
                    AssertSame(actual, *legacy);
                    UNIT_ASSERT_C(issues.find("[native_frontend_fallback]") != std::string::npos, issues);
                    UNIT_ASSERT_C(issues.find("feature=backend") != std::string::npos, issues);
                    const auto next = runner.GetQueryClient().ExecuteQuery(
                        "PRAGMA YqlSelect='force'; SELECT a.id AS l, b.id AS r FROM `/Root/JoinA` AS a FULL JOIN `/Root/JoinB` AS b "
                        "ON a.k = b.k ORDER BY l, r", NQuery::TTxControl::NoTx()).GetValueSync();
                    UNIT_ASSERT_C(next.IsSuccess(), next.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(SnapshotRows(next.GetResultSet(0)).Values, "[[#;[5]];[[1];[3]];[[1];[4]];[[2];#]]");
                    UNIT_ASSERT_C(next.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, next.GetIssues().ToString());
                }

        }
    }

    Y_UNIT_TEST_TWIN(WindowsMatchLegacyOnPopulatedTables, ColumnStore) {
        TVector<std::pair<TString, size_t>> queries;
        const TVector<TString> specifications{
            "", "PARTITION BY p", "PARTITION BY p + 1", "PARTITION BY p ORDER BY k",
            "PARTITION BY p ORDER BY k DESC", "PARTITION BY p ORDER BY k, id",
            "PARTITION BY p ORDER BY CAST(k AS String)",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING",
            "PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            "PARTITION BY p ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "PARTITION BY p ORDER BY k RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING",
            "PARTITION BY p ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING",
            "PARTITION BY p ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING",
            "PARTITION BY p ORDER BY k RANGE BETWEEN 5 FOLLOWING AND 15 FOLLOWING",
            "PARTITION BY p ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "PARTITION BY p ORDER BY k DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW",
            "PARTITION BY p ORDER BY CAST(k AS Double) RANGE BETWEEN 10 PRECEDING AND CURRENT ROW"};
        for (const auto& spec : specifications) {
            TString sql = "SELECT id";
            size_t index = 0;
            for (const auto* function : {"SUM(v)", "AVG(v)", "COUNT(v)", "COUNT(*)", "MIN(v)", "MAX(v)", "SUM(id)", "AVG(d)", "SUM(d)"})
                sql += TStringBuilder() << ", " << function << " OVER(" << spec << ") AS f" << index++;
            queries.emplace_back(sql + " FROM `/Root/WindowRows` ORDER BY id", 12);
        }
        queries.emplace_back("SELECT id, Rank() OVER(PARTITION BY p ORDER BY k) AS r, DenseRank() OVER(PARTITION BY p ORDER BY k) AS d, RowNumber() OVER(PARTITION BY p ORDER BY k, id) AS n FROM `/Root/WindowRows` ORDER BY id", 12);
        queries.emplace_back("SELECT p, SUM(v) AS s, SUM(SUM(v)) OVER() AS total, AVG(SUM(v)) OVER() AS mean, Rank() OVER(ORDER BY SUM(v) DESC) AS r FROM `/Root/WindowRows` GROUP BY p HAVING COUNT(*) > 0ul ORDER BY p", 5);
        queries.emplace_back("SELECT id, SUM(v) OVER() + SUM(v) OVER() AS doubled, SUM(v) OVER(PARTITION BY p) AS partial FROM `/Root/WindowRows` ORDER BY id", 12);
        queries.emplace_back("SELECT id, total FROM (SELECT id, SUM(v) OVER(PARTITION BY p) AS total FROM `/Root/WindowRows`) AS x WHERE total > 10 ORDER BY id", 6);
        queries.emplace_back("SELECT a.id, SUM(b.id) OVER(PARTITION BY a.p) AS total FROM `/Root/WindowRows` AS a LEFT JOIN `/Root/WindowRows` AS b ON a.id = b.id AND b.id < 2ul ORDER BY a.id", 12);
        queries.emplace_back("SELECT id, SUM(v) OVER() AS s FROM `/Root/WindowRows` WHERE id = 99ul ORDER BY id", 0);
        queries.emplace_back("$q = (SELECT k, SUM(1) OVER(ORDER BY k) AS n FROM `/Root/WindowRows`); PRAGMA DisableAnsiCurrentRow; SELECT k, n FROM $q ORDER BY k, n", 12);
        TVector<std::optional<TResultSnapshot>> reference(queries.size());
        std::optional<TResultSnapshot> apiReference[2];
        for (const bool windowV2 : {false, true}) for (const int strategy : {0, 2}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetAllowOlapDataQuery(true);
            service->SetEnableWindowFunctionsV2(windowV2);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(TString(
                "CREATE TABLE `/Root/WindowRows` (id Uint64 NOT NULL, p Int32, k Int32, v Int32, d Decimal(7,2), PRIMARY KEY(id))") +
                (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            using TCell = std::optional<i32>;
            const TCell n = std::nullopt;
            const TVector<std::tuple<TCell, TCell, TCell, const char*>> data{
                {1, 10, 5, "1.5"}, {1, 20, 3, "3.5"}, {1, 20, 7, "2.5"}, {1, 30, n, nullptr},
                {2, 10, n, nullptr}, {2, n, 4, "4.5"}, {n, 10, 1, "1"}, {n, 20, 2, "2"},
                {3, 10, n, nullptr}, {3, 20, n, nullptr}, {4, 10, 42, "42.5"}, {1, 30, -2, "-2.5"}};
            TValueBuilder rows;
            rows.BeginList();
            ui64 id = 0;
            for (const auto& [p, k, v, d] : data) {
                rows.AddListItem().BeginStruct().AddMember("id").Uint64(++id).AddMember("p").OptionalInt32(p)
                    .AddMember("k").OptionalInt32(k).AddMember("v").OptionalInt32(v).AddMember("d");
                if (d) rows.BeginOptional().Decimal(TDecimalValue(d, 7, 2)).EndOptional();
                else rows.EmptyOptional(TTypeBuilder().Decimal({7, 2}).Build());
                rows.EndStruct();
            }
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/WindowRows", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            for (size_t index = 0; index < queries.size(); ++index) {
                const auto& [sql, count] = queries[index];
                Cerr << "Window query " << index << ", strategy " << strategy << ", v2 " << windowV2 << ": " << sql << Endl;
                auto result = runner.GetQueryClient().ExecuteQuery("PRAGMA YqlSelect='force'; " + sql, NQuery::TTxControl::NoTx()).GetValueSync();
                // Existing legacy window annotation leaves SUM(required) required
                // even for a frame that can be empty; physical rewriting rejects it.
                if (strategy != 2 && (index == 10 || index == 11 || index == 13 || index == 17 || index == 19)) {
                    UNIT_ASSERT(!result.IsSuccess());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("Rewrite error, type should be") != std::string::npos, result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("'f6':Uint64?") != std::string::npos, result.GetIssues().ToString());
                    // Both old paths fail the original query. Widen only the
                    // reference argument's nullability, preserving every value;
                    // native must execute the original required-input query.
                    auto referenceSql = sql;
                    SubstGlobal(referenceSql, "SUM(id)", "SUM(CAST(id AS Uint64?))");
                    result = runner.GetQueryClient().ExecuteQuery("PRAGMA YqlSelect='force'; " + referenceSql,
                        NQuery::TTxControl::NoTx()).GetValueSync();
                }
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), count);
                const auto actual = SnapshotRows(result.GetResultSet(0));
                if (!reference[index]) reference[index] = actual;
                else AssertSame(actual, *reference[index]);
            }
            // Ordered default frames differ by API. Tied rows select the same
            // key and are sorted by the running count, so ROWS is deterministic.
            for (size_t api = 0; api < 3; ++api) for (size_t pragma = 0; pragma < 3; ++pragma) {
                const bool ansi = pragma == 1 || (pragma == 0 && api == 1);
                const TString sql = TString("PRAGMA YqlSelect='force'; ") +
                    (pragma == 1 ? "PRAGMA AnsiCurrentRow; " : pragma == 2 ? "PRAGMA DisableAnsiCurrentRow; " : "") +
                    "SELECT k, SUM(1) OVER(ORDER BY k) AS n FROM `/Root/WindowRows` ORDER BY k, n";
                if (api == 2) {
                    auto scan = client.StreamExecuteScanQuery(sql).GetValueSync();
                    UNIT_ASSERT_C(scan.IsSuccess(), scan.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(StreamResultToYson(scan), apiReference[ansi]->Values);
                    continue;
                }
                const auto resultSet = [&] {
                    if (api == 0) {
                        const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                        Snapshot(result);
                        return result.GetResultSet(0);
                    }
                    const auto result = runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    return result.GetResultSet(0);
                }();
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 12);
                TResultSetParser values(resultSet);
                for (i64 row = 0; row < 12; ++row) {
                    UNIT_ASSERT(values.TryNextRow());
                    const auto expected = ansi ? (row == 0 ? 1 : row < 6 ? 6 : row < 10 ? 10 : 12) : row + 1;
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("n").GetInt64(), expected);
                }
                const auto actual = SnapshotRows(resultSet);
                if (!apiReference[ansi]) apiReference[ansi] = actual;
                else AssertSame(actual, *apiReference[ansi]);
            }
            if (strategy == 2) {
                const auto result = runner.GetQueryClient().ExecuteQuery(
                    "SELECT id, SUM(v) OVER(PARTITION BY p) + 1 IN (SELECT 14L) AS matched FROM `/Root/WindowRows` ORDER BY id",
                    NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 12);
                TResultSetParser values(result.GetResultSet(0));
                for (const std::optional<bool> expected : std::initializer_list<std::optional<bool>>{true, true, true, true, false, false,
                        false, false, std::nullopt, std::nullopt, false, true}) {
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT(values.ColumnParser("matched").GetOptionalBool() == expected);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(InSubqueriesMatchLegacyOnPopulatedTables, EqualNullsJoinKeys) {
        using TBool = std::optional<bool>;
        const TBool n = std::nullopt;
        struct TCase {
            const char* Expression;
            TVector<TBool> Ansi;
            TVector<TBool> Legacy;
        };
        const TVector<TCase> cases{
            {"a.id IN (SELECT b.id FROM `/Root/InRows` AS b WHERE b.id < 3ul)", {true, true, false, false, false, false}, {}},
            {"a.value IN (SELECT b.value FROM `/Root/InRows` AS b WHERE b.id <= 2ul)", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"a.value NOT IN (SELECT b.value FROM `/Root/InRows` AS b WHERE b.id <= 2ul)", {false, n, n, false, n, n}, {false, n, true, false, n, true}},
            {"a.value IN (SELECT b.value FROM `/Root/InRows` AS b WHERE b.id = 99ul)", {false, false, false, false, false, false}, {false, n, false, false, n, false}},
            {"a.value IN (SELECT b.value FROM `/Root/InRows` AS b WHERE b.id < a.id)", {false, n, n, true, n, n}, {false, n, false, true, n, false}},
            {"CAST(a.id AS Int32) IN (SELECT b.value FROM `/Root/InRows` AS b)", {n, n, n, n, n, n}, {false, false, false, false, false, false}},
            {"a.value + 1 IN (SELECT b.value + 1 FROM `/Root/InRows` AS b WHERE b.id <= 2ul)", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"(a.value IN (SELECT b.value FROM `/Root/InRows` AS b WHERE b.id <= 2ul)) IN (SELECT true)", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"a.value IN (SELECT CAST(b.value AS Int64) FROM `/Root/InRows` AS b WHERE b.id <= 2ul)", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"CAST(a.value AS Decimal(8,2)) IN (SELECT CAST(b.value AS Decimal(10,3)) FROM `/Root/InRows` AS b WHERE b.id <= 2ul)", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"NULL IN (SELECT b.value FROM `/Root/InRows` AS b WHERE b.id = 99ul)", {false, false, false, false, false, false}, {n, n, n, n, n, n}},
            {"a.value IN (10, NULL)", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"CAST(a.id AS String) IN (SELECT CAST(b.id AS Utf8) FROM `/Root/InRows` AS b WHERE b.id < 3ul)", {true, true, false, false, false, false}, {}},
            {"a.value IN [10, NULL]", {true, n, n, true, n, n}, {true, n, false, true, n, false}},
            {"a.value NOT IN [10, NULL]", {false, n, n, false, n, n}, {false, n, true, false, n, true}},
            {"a.value IN []", {false, false, false, false, false, false}, {false, n, false, false, n, false}},
            {"a.value NOT IN []", {true, true, true, true, true, true}, {true, n, true, true, n, true}},
            {"a.value IN [NULL]", {n, n, n, n, n, n}, {false, n, false, false, n, false}},
            {"CAST(a.id AS String) IN ['1','3','1',]", {true, false, true, false, false, false}, {}},
        };
        TVector<std::optional<TResultSnapshot>> reference(cases.size() * 8);
        for (const int strategy : {0, 2}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableBlockHashJoinEqualNulls(EqualNullsJoinKeys);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/InRows` (id Uint64 NOT NULL, value Int32, PRIMARY KEY(id))").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            TValueBuilder rows;
            rows.BeginList();
            const std::optional<i32> values[]{10, std::nullopt, 30, 10, std::nullopt, 0};
            for (ui64 id = 1; id <= 6; ++id) rows.AddListItem().BeginStruct()
                .AddMember("id").Uint64(id).AddMember("value").OptionalInt32(values[id - 1]).EndStruct();
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/InRows", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            for (size_t api = 0; api < 4; ++api) {
                const bool generic = api % 2;
                const bool ansi = api == 1 || api == 2;
                for (size_t index = 0; index < cases.size(); ++index) for (const bool projection : {false, true}) {
                    const auto& test = cases[index];
                    const auto& expected = !ansi && !test.Legacy.empty() ? test.Legacy : test.Ansi;
                    const auto key = (api * cases.size() + index) * 2 + projection;
                    const TString context = TStringBuilder() << "IN strategy=" << strategy << ", api=" << api << ", case=" << index << ", projection=" << projection;
                    TString sql = "PRAGMA YqlSelect='force'; ";
                    if (api == 2) sql += "PRAGMA AnsiInForEmptyOrNullableItemsCollections; ";
                    if (api == 3) sql += "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections; ";
                    sql += projection ? TStringBuilder() << "SELECT a.id, " << test.Expression << " AS v FROM `/Root/InRows` AS a ORDER BY a.id" :
                        TStringBuilder() << "SELECT a.id FROM `/Root/InRows` AS a WHERE " << test.Expression << " ORDER BY a.id";
                    Cerr << context << ": " << sql << Endl;
                    const char* expectedError = nullptr;
                    // Preserve existing frontend/backend boundaries. Native cases
                    // always execute, with their values checked independently.
                    if (strategy == 0) {
                        if (index == 4) expectedError = "Member not found: _yql_join_sublink_0__alias_a.value";
                        if (index == 7) expectedError = "Expected persistable data, but got: Unit";
                        if (index == 10) expectedError = "Only SqlIn for any/all is supported on YqlSelect";
                    }
                    auto inspect = [&](const auto& result) {
                        if (expectedError) {
                            UNIT_ASSERT_C(!result.IsSuccess(), context);
                            UNIT_ASSERT_C(result.GetIssues().ToString().find(expectedError) != std::string::npos, context + ": " + result.GetIssues().ToString());
                            return;
                        }
                        UNIT_ASSERT_C(result.IsSuccess(), context + ": " + result.GetIssues().ToString());
                        UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, context);
                        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                        const auto& resultSet = result.GetResultSet(0);
                        const auto actual = SnapshotRows(resultSet);
                        TVector<ui64> ids;
                        for (size_t row = 0; row < expected.size(); ++row)
                            if (projection || expected[row].value_or(false)) ids.push_back(row + 1);
                        // Non-ANSI NOT IN on this nullable input is inconsistent
                        // in the old filter rewrites: the old optimizer keeps NULL
                        // lookups, and old RBO uses ANSI NULL-item semantics.
                        const bool legacyNotInBoundary = !projection && !ansi && index == 2;
                        if (legacyNotInBoundary && strategy == 0) ids = TVector<ui64>{2, 3, 5, 6};
                        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.RowsCount(), ids.size(), context);
                        {
                            TResultSetParser parser(resultSet);
                            for (const auto id : ids) {
                                UNIT_ASSERT(parser.TryNextRow());
                                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser("id").GetUint64(), id, context);
                                if (projection) {
                                    TTypeParser type(resultSet.GetColumnsMeta()[1].Type);
                                    const TBool value = type.GetKind() == TTypeParser::ETypeKind::Optional ? parser.ColumnParser("v").GetOptionalBool() : TBool{parser.ColumnParser("v").GetBool()};
                                    UNIT_ASSERT_C(value == expected[id - 1], context + ": " + actual.Values);
                                }
                            }
                        }
                        if (strategy == 0) reference[key] = actual;
                        else if (reference[key]) {
                            if (legacyNotInBoundary) UNIT_ASSERT(actual.Columns == reference[key]->Columns);
                            else AssertSame(actual, *reference[key]);
                        }
                    };
                    if (generic) inspect(runner.GetQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync());
                    else {
                        const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                        inspect(result);
                        if (!result.IsSuccess()) session = client.CreateSession().GetValueSync().GetSession();
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(ExistsSubqueriesMatchLegacyOnPopulatedTables, EqualNullsJoinKeys) {
        struct TCase {
            const char* Sql;
            TVector<ui64> Ids;
            TVector<bool> Values;
        };
        const TVector<TCase> cases{
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE EXISTS(SELECT b.value FROM `/Root/ExistsRows` AS b WHERE b.value = a.value) ORDER BY a.id", {1, 3, 4, 6}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE NOT EXISTS(SELECT b.value FROM `/Root/ExistsRows` AS b WHERE b.value = a.value) ORDER BY a.id", {2, 5}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE EXISTS(SELECT b.id FROM `/Root/ExistsRows` AS b WHERE b.id < a.id) ORDER BY a.id", {2, 3, 4, 5, 6}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE EXISTS(SELECT b.id FROM `/Root/ExistsRows` AS b WHERE b.id = a.id) AND NOT EXISTS(SELECT id FROM `/Root/ExistsRows` WHERE id = 99ul) ORDER BY a.id", {1, 2, 3, 4, 5, 6}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE a.id = 1ul OR EXISTS(SELECT b.id FROM `/Root/ExistsRows` AS b WHERE b.value < a.value) ORDER BY a.id", {1, 3, 4}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE EXISTS(SELECT b.id FROM `/Root/ExistsRows` AS b WHERE b.id < a.id AND EXISTS(SELECT c.id FROM `/Root/ExistsRows` AS c WHERE c.id = b.id)) ORDER BY a.id", {2, 3, 4, 5, 6}},
            {"SELECT a.id, EXISTS(SELECT b.id, b.value FROM `/Root/ExistsRows` AS b WHERE b.value = a.value) AS v FROM `/Root/ExistsRows` AS a ORDER BY a.id", {1, 2, 3, 4, 5, 6}, {true, false, true, true, false, true}},
            {"SELECT a.id, NOT EXISTS(SELECT id FROM `/Root/ExistsRows` WHERE id = 99ul) AS v FROM `/Root/ExistsRows` AS a ORDER BY a.id", {1, 2, 3, 4, 5, 6}, {true, true, true, true, true, true}},
            {"SELECT a.id, EXISTS(SELECT COUNT(*) FROM `/Root/ExistsRows` AS b WHERE b.id < a.id) AS v FROM `/Root/ExistsRows` AS a ORDER BY a.id", {1, 2, 3, 4, 5, 6}, {true, true, true, true, true, true}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE EXISTS(SELECT id FROM `/Root/ExistsRows` LIMIT 0) ORDER BY a.id", {}},
            {"SELECT a.id FROM `/Root/ExistsRows` AS a WHERE EXISTS(SELECT id FROM `/Root/ExistsRows` WHERE id = 99ul UNION ALL SELECT id FROM `/Root/ExistsRows` WHERE id = 1ul) ORDER BY a.id", {1, 2, 3, 4, 5, 6}},
        };
        TVector<std::optional<TResultSnapshot>> reference(cases.size());
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableBlockHashJoinEqualNulls(EqualNullsJoinKeys);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/ExistsRows` (id Uint64 NOT NULL, value Int32, PRIMARY KEY(id))").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            TValueBuilder rows;
            rows.BeginList();
            const std::optional<i32> values[]{10, std::nullopt, 30, 10, std::nullopt, 0};
            for (ui64 id = 1; id <= 6; ++id) rows.AddListItem().BeginStruct()
                .AddMember("id").Uint64(id).AddMember("value").OptionalInt32(values[id - 1]).EndStruct();
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/ExistsRows", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            for (size_t index = 0; index < cases.size(); ++index) {
                const auto& test = cases[index];
                Cerr << "EXISTS strategy=" << strategy << ", case=" << index << ": " << test.Sql << Endl;
                const auto result = session.ExecuteDataQuery(TString("PRAGMA YqlSelect='force'; ") + test.Sql,
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                // Original new RBO does not inline projected EXISTS calls. Preserve
                // these baseline failures; only the native Map path is enabled here.

                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), test.Ids.size());
                TResultSetParser parser(result.GetResultSet(0));
                for (size_t row = 0; row < test.Ids.size(); ++row) {
                    UNIT_ASSERT(parser.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetUint64(), test.Ids[row]);
                    if (!test.Values.empty()) {
                        // The old optimizer loses the single global-aggregate row
                        // for an empty correlated input. Native EXISTS is true.
                        const bool expected = strategy == 0 && index == 8 && row == 0 ? false : test.Values[row];
                        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("v").GetBool(), expected);
                    }
                }
                if (!reference[index]) reference[index] = actual;
                else if (strategy == 0 && index == 8) {
                    UNIT_ASSERT(actual.Columns == reference[index]->Columns);
                } else AssertSame(actual, *reference[index]);
            }
        }
    }

    Y_UNIT_TEST_TWIN(CorrelatedScalarSubqueriesMatchLegacyOnPopulatedTables, EqualNullsJoinKeys) {
        TVector<std::optional<TResultSnapshot>> reference;
        TString failures;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetEnableBlockHashJoinEqualNulls(EqualNullsJoinKeys);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/CorrelatedRows` (id Uint64 NOT NULL, value Int32, PRIMARY KEY(id))").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            const std::optional<i32> values[]{10, {}, 30, 10, {}, 0};
            TValueBuilder rows;
            rows.BeginList();
            for (ui64 i = 0; i < std::size(values); ++i) rows.AddListItem().BeginStruct()
                .AddMember("id").Uint64(i + 1).AddMember("value").OptionalInt32(values[i]).EndStruct();
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/CorrelatedRows", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            const std::pair<const char*, size_t> cases[]{
                {"SELECT a.id, (SELECT b.value FROM `/Root/CorrelatedRows` AS b WHERE b.id = a.id) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT MAX(b.value) FROM `/Root/CorrelatedRows` AS b WHERE b.value = a.value) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT MAX(b.value) FROM `/Root/CorrelatedRows` AS b WHERE b.id < a.id) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT COUNT(*) FROM `/Root/CorrelatedRows` AS b WHERE b.id < a.id) AS n FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT COUNT(*) FROM `/Root/CorrelatedRows` AS b WHERE b.value = a.value) AS n FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT (SELECT a.value)) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT MAX(b.value) + a.value FROM `/Root/CorrelatedRows` AS b) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT SUM(a.value) FROM `/Root/CorrelatedRows` AS b WHERE b.id < 3ul) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT MAX(b.value) FROM `/Root/CorrelatedRows` AS b HAVING MAX(b.value) > a.value) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.id, (SELECT b.value FROM `/Root/CorrelatedRows` AS b WHERE b.id < a.id ORDER BY b.id DESC LIMIT 1) AS v FROM `/Root/CorrelatedRows` AS a ORDER BY a.id", 6},
                {"SELECT a.value, (SELECT COUNT(*) FROM `/Root/CorrelatedRows` AS b WHERE b.value = a.value) AS n FROM `/Root/CorrelatedRows` AS a GROUP BY a.value ORDER BY a.value", 4},
                {"SELECT a.id FROM `/Root/CorrelatedRows` AS a WHERE a.value > (SELECT AVG(b.value) FROM `/Root/CorrelatedRows` AS b WHERE b.id <= a.id) ORDER BY a.id", 1},
            };
            size_t index = 0;
            for (const auto& [sql, rowCount] : cases) {
                Cerr << "Correlated scalar strategy=" << strategy << ", case=" << index << ": " << sql << Endl;
                const auto result = session.ExecuteDataQuery(TString("PRAGMA YqlSelect='force'; ") + sql,
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                if (strategy == 2) reference.emplace_back();
                // The existing SQL frontend cannot resolve this grandparent
                // reference through a SELECT without FROM. Its failure is not
                // a native execution reference; the result is checked below.
                if (index == 5 && strategy != 2) {
                    UNIT_ASSERT(!result.IsSuccess());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("No such column: value") != std::string::npos, result.GetIssues().ToString());
                    ++index;
                    continue;
                }
                if (index == 6 || index == 8) {
                    UNIT_ASSERT(!result.IsSuccess());
                    const auto* message = strategy == 2 ? "outer references after aggregation" : "must appear in GROUP BY";
                    UNIT_ASSERT_C(result.GetIssues().ToString().find(message) != std::string::npos, result.GetIssues().ToString());
                    ++index;
                    continue;
                }
                if (index == 9 && strategy == 0) {
                    UNIT_ASSERT(!result.IsSuccess());
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("Member not found: _alias_b.id") != std::string::npos, result.GetIssues().ToString());
                    ++index;
                    continue;
                }
                if (!result.IsSuccess()) {
                    failures += TStringBuilder() << "strategy=" << strategy << ", case=" << index << ": " << result.GetIssues().ToString() << '\n';
                    session = client.CreateSession().GetValueSync().GetSession();
                    ++index;
                    continue;
                }
                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), rowCount);
                if (index == 0 || index == 3 || index == 4 || index == 5 || index == 10) {
                    TResultSetParser data(result.GetResultSet(0));
                    const ui64 counts[]{2, 0, 1, 2, 0, 1};
                    const std::optional<i32> groups[]{std::nullopt, 0, 10, 30};
                    const ui64 groupCounts[]{0, 1, 2, 1};
                    for (ui64 i = 0; i < rowCount; ++i) {
                        UNIT_ASSERT(data.TryNextRow());
                        if (index == 0 || index == 5) UNIT_ASSERT(data.ColumnParser("v").GetOptionalInt32() == values[i]);
                        else {
                            if (index == 10) UNIT_ASSERT(data.ColumnParser("value").GetOptionalInt32() == groups[i]);
                            else UNIT_ASSERT_VALUES_EQUAL(data.ColumnParser("id").GetUint64(), i + 1);
                            const auto count = index == 3 ? i : index == 4 ? counts[i] : groupCounts[i];
                            // The legacy optimizer loses empty correlated COUNT groups.
                            // Existing new RBO restores zero, which native must preserve.
                            const auto expected = strategy == 0 && count == 0 ? std::optional<ui64>{} : std::optional<ui64>{count};
                            UNIT_ASSERT_C(data.ColumnParser("n").GetOptionalUint64() == expected,
                                TStringBuilder() << "strategy=" << strategy << ", case=" << index << ", row=" << i);
                        }
                    }
                }
                if (strategy == 2) reference[index] = actual;
                else if (reference.at(index)) {
                    if (strategy == 0 && (index == 3 || index == 4 || index == 10)) UNIT_ASSERT(actual.Columns == reference[index]->Columns);
                    else AssertSame(actual, *reference[index]);
                }
                ++index;
            }
            const auto invalid = session.ExecuteDataQuery(
                "PRAGMA YqlSelect='force'; SELECT (SELECT b.id FROM `/Root/CorrelatedRows` AS b WHERE b.value = a.value) AS invalid FROM `/Root/CorrelatedRows` AS a",
                TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!invalid.IsSuccess());
            if (strategy != 0) UNIT_ASSERT_C(invalid.GetIssues().ToString().find("more than one row") != std::string::npos, invalid.GetIssues().ToString());
            for (const auto* sql : {
                "SELECT (SELECT a.value FROM (SELECT 1 AS id) AS a) FROM `/Root/CorrelatedRows` AS a",
                "SELECT (SELECT a.value) FROM `/Root/CorrelatedRows` AS a GROUP BY a.id"}) {
                const auto invalidScope = session.ExecuteDataQuery(TString("PRAGMA YqlSelect='force'; ") + sql,
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(!invalidScope.IsSuccess(), sql);
            }
        }
        UNIT_ASSERT_C(failures.empty(), failures);
    }

    Y_UNIT_TEST(ScalarSubqueriesMatchLegacyOnPopulatedTables) {
        struct TCase {
            const char* Sql;
            size_t Rows = 1;
            // Expected pre-existing failures, or explicit native rejection.
            const char* Errors[3] = {};
            std::optional<i64> Total;
        };
        TVector<std::optional<TResultSnapshot>> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetEnableFallbackToYqlOptimizer(false);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED :
                NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetDefaultLangVer(NYql::GetMaxLangVersion());
            service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = runner.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto schema = session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/ScalarRows` (id Uint64 NOT NULL, value Int32, PRIMARY KEY(id))").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            TValueBuilder rows;
            rows.BeginList();
            for (ui64 id = 1; id <= 3; ++id) rows.AddListItem().BeginStruct()
                .AddMember("id").Uint64(id)
                .AddMember("value").OptionalInt32(id == 2 ? std::nullopt : std::optional<i32>(id * 10))
                .EndStruct();
            rows.EndList();
            const auto inserted = client.BulkUpsert("/Root/ScalarRows", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            size_t index = 0;
            for (const auto& test : std::initializer_list<TCase>{
                {"SELECT (SELECT id FROM `/Root/ScalarRows` WHERE id = 1ul) AS first_value, (SELECT id FROM `/Root/ScalarRows` WHERE id = 3ul) AS last_value", 1},
                {"SELECT (SELECT value FROM `/Root/ScalarRows` WHERE id = 99ul) AS empty, (SELECT value FROM `/Root/ScalarRows` WHERE id = 2ul) AS null_value", 1},
                {"SELECT id, value FROM `/Root/ScalarRows` WHERE value > (SELECT AVG(value) FROM `/Root/ScalarRows`) ORDER BY id", 1},
                {"SELECT id, (SELECT MAX(value) FROM `/Root/ScalarRows`) AS maximum FROM `/Root/ScalarRows` ORDER BY id", 3},
                {"SELECT (SELECT (SELECT 1) + 2) AS nested", 1},
                {"$q = SELECT MAX(value) AS v FROM `/Root/ScalarRows`; SELECT (SELECT v FROM $q) AS maximum", 1},
                {"SELECT (SELECT COUNT(*) FROM `/Root/ScalarRows` WHERE id > 99ul) AS zero", 1},
                {"SELECT SUM(value) AS total FROM `/Root/ScalarRows` HAVING SUM(value) > (SELECT MIN(value) FROM `/Root/ScalarRows`)", 1},
                {"SELECT SUM((SELECT MAX(value) FROM `/Root/ScalarRows`)) AS total FROM `/Root/ScalarRows`", 1, {"Unexpected node in results", nullptr, nullptr}, 90},
                {"SELECT id FROM `/Root/ScalarRows` ORDER BY (SELECT MAX(value) FROM `/Root/ScalarRows`), id", 3, {"Unexpected node in results", "typeIt!=typeItems.end()", "outside the native profile"}},
                {"SELECT k, COUNT(*) AS n FROM `/Root/ScalarRows` GROUP BY (SELECT MAX(value) FROM `/Root/ScalarRows`) AS k", 1,
                    {"Translation of the statement to YqlSelect was forced, but unsupported", "Translation of the statement to YqlSelect was forced, but unsupported", "outside the native profile"}},
            }) {
                const TString query = TString("PRAGMA YqlSelect='force'; ") + test.Sql;
                Cerr << "Scalar subquery strategy=" << strategy << ": " << test.Sql << Endl;
                const auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                if (strategy == 2) reference.emplace_back();
                if (const auto* error = test.Errors[strategy]) {
                    UNIT_ASSERT_C(!result.IsSuccess(), test.Sql);
                    UNIT_ASSERT_C(result.GetIssues().ToString().find(error) != std::string::npos, result.GetIssues().ToString());
                    // A pre-existing fatal backend error can close its session.
                    session = client.CreateSession().GetValueSync().GetSession();
                    ++index;
                    continue;
                }
                const auto actual = Snapshot(result);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), test.Rows);
                if (test.Total) {
                    TResultSetParser values(result.GetResultSet(0));
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("total").GetOptionalInt64().value(), *test.Total);
                }
                if (index == 0) {
                    TResultSetParser values(result.GetResultSet(0));
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("first_value").GetOptionalUint64().value(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser("last_value").GetOptionalUint64().value(), 3);
                }
                if (strategy == 2) reference[index] = actual;
                else {
                    UNIT_ASSERT(reference.at(index));
                    AssertSame(actual, *reference.at(index));
                }
                ++index;
            }
            const auto invalid = session.ExecuteDataQuery(
                "PRAGMA YqlSelect='force'; SELECT (SELECT id FROM `/Root/ScalarRows`) AS invalid",
                TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!invalid.IsSuccess());
            if (strategy != 0) UNIT_ASSERT_C(invalid.GetIssues().ToString().find("more than one row") != std::string::npos, invalid.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ParserDispatchRemainsNative) {
        TKikimrRunner kikimr(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const auto& [sql, parser] : {std::pair{"SELECT 1 AS answer", "handwritten"},
                                        {"SELECT 'привет' AS answer", "grammar"}}) {
            const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            Snapshot(result);
            const auto explained = session.ExplainDataQuery(sql).GetValueSync();
            UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
            UNIT_ASSERT_C(explained.GetAst().find(std::string("native parser: ") + parser) != std::string::npos,
                explained.GetAst());
        }
    }

    Y_UNIT_TEST(DynamicSignedCountsMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const bool nullable : {false, true}) for (const bool offset : {false, true}) {
            const TString sql = TString("DECLARE $count AS Int32") + (nullable ? "?" : "") +
                "; SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3 AS x) AS t ORDER BY x " +
                (offset ? "LIMIT 3 OFFSET $count" : "LIMIT $count");
            for (const auto count : {std::optional<i32>{}, std::optional<i32>{-1}, std::optional<i32>{0},
                    std::optional<i32>{1}, std::optional<i32>{4}}) {
                if (!nullable && !count) continue;
                TParamsBuilder builder;
                if (nullable) builder.AddParam("$count").OptionalInt32(count).Build();
                else builder.AddParam("$count").Int32(*count).Build();
                const auto parameters = builder.Build();
                const auto expected = legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), parameters).GetValueSync();
                const auto actual = native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), parameters).GetValueSync();
                AssertSame(Snapshot(actual), Snapshot(expected));
            }
        }
    }

    Y_UNIT_TEST(ScalarSemanticsMatchLegacyAcrossReusedSessions) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        TString failures;
        for (const auto* sql : {
            "SELECT 1",
            "SELECT Unwrap(1), Unwrap(1, 99), Unwrap(CAST(7 AS Int64?))",
            "SELECT CAST(NULL AS String), CAST(NULL AS Utf8)",
            "SELECT IF(false, Unwrap(CAST(NULL AS Int32), 'not reached'), 9)",
            "SELECT Unwrap(CAST(7 AS Int32?), Unwrap(CAST(NULL AS Utf8), 'unused message'))",
            "--!ansi_lexer\nSELECT 'a''b'",
            "SELECT a.x FROM (SELECT 1 AS x) AS a",
            "$a = 1; $f = ($x)->{RETURN $x + $a;}; $a = 10; SELECT $f(2), $a",
            "$f = ($x)->{RETURN $x + 1;}; $g = ($x)->{RETURN $f($x);}; $f = ($x)->{RETURN $x + 10;}; SELECT $g(2), $f(2)",
            "$q = SELECT 1 AS x; $saved = SELECT x FROM $q; $q = SELECT 2 AS x; SELECT x FROM $saved",
            "$x = 1; $x = $x + 1; SELECT $x",
            "SELECT 1 = NULL, NULL = NULL, NULL IN (1, NULL)",
            "SELECT 1 IN [1, 2L], 2 IN [NULL, 1], NULL IN [], 3 IN [1, 2L]",
            "SELECT x IN [x + 1, x], x IN [NULL, x + 1] FROM (SELECT 1 AS x UNION ALL SELECT CAST(NULL AS Int32) AS x) AS t ORDER BY x",
            "PRAGMA AnsiInForEmptyOrNullableItemsCollections; SELECT x IN [x + 1, x], x IN [NULL, x + 1] FROM (SELECT 1 AS x UNION ALL SELECT CAST(NULL AS Int32) AS x) AS t ORDER BY x",
            "$q = SELECT CAST(NULL AS Int32) AS x UNION ALL SELECT 1 AS x; SELECT 1 IN $q, 2 IN $q, NULL IN $q",
            "PRAGMA AnsiInForEmptyOrNullableItemsCollections; $q = SELECT CAST(NULL AS Int32) AS x UNION ALL SELECT 1 AS x; SELECT 1 IN $q, 2 IN $q, NULL IN $q",
            "$q = SELECT 1 AS x; $saved = $q; $q = SELECT 2 AS x; SELECT 1 IN $saved, 2 IN $saved, 2 IN $q",
            "$q = SELECT x FROM (SELECT 1 AS x) AS t WHERE false; SELECT 1 IN $q, NULL IN $q",
            "SELECT 1 IN ((SELECT 1 AS x)), 2 IN ((SELECT 1 AS x))",
            "SELECT 1 IN ((SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS t))",
            "SELECT 1 IN ((SELECT x FROM (SELECT 1 AS x) AS t WHERE false)), NULL IN ((SELECT x FROM (SELECT 1 AS x) AS t WHERE false))",
            "SELECT (1, 2) = (1, 2), (1, 2) != (1, 3), (1, 3) < (2, 0), (1, 3) >= (1, 3)",
            "SELECT (1, NULL) < (2, NULL), (1, NULL) = (2, NULL), (1, NULL) = (1, NULL), (NULL, 1) > (NULL, 0)",
            "SELECT SUM(x) AS total, COUNT(*) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS q",
            "SELECT a.x, b.x AS matched FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS a LEFT JOIN (SELECT 1 AS x) AS b ON a.x = b.x ORDER BY a.x",
            "SELECT DISTINCT x FROM (SELECT 1 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x) AS q ORDER BY x",
            "SELECT COUNT(DISTINCT x) AS n FROM (SELECT 1 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x) AS q",
            "SELECT k, COUNT(*) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS q GROUP BY x % 2 AS k HAVING COUNT(*) > 0ul ORDER BY k",
            "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS q ORDER BY x LIMIT 1 OFFSET 1",
            "SELECT Decimal('1.25',8,2), CAST(1 AS Decimal(8,2)), CAST('1.25' AS Decimal(8,2))",
            "SELECT Decimal('9.5',8,2) BETWEEN 1 AND 10, Decimal('1.25',8,2) = Decimal('1.250',6,3)",
            "SELECT IF(true, Decimal('1.25',8,2), 1L), IF(false, Decimal('1.25',8,2), Decimal('1.251',6,3))",
            "SELECT IF(false, Decimal('1.25',8,2), 18446744073709551615ul)",
            "SELECT Decimal('1.25',8,2) AS x UNION ALL SELECT 1L AS x",

            "SELECT Decimal('1',8,2) / Decimal('0',8,2), Decimal('0',8,2) / Decimal('0',8,2)",
            "SELECT -Decimal('nan',8,2), Abs(Decimal('-inf',8,2)), Decimal('inf',8,2) - Decimal('inf',8,2)",
            "SELECT CAST(Decimal('1.5',8,2) AS Int32), CAST(Decimal('9.9',2,1) AS Int32), CAST(Decimal('1.50',8,2) AS String)",
            "SELECT CAST(Decimal('1.25',8,2) AS Float), CAST(Decimal('1.25',8,2) AS Double), CAST(Decimal('99.9',3,1) AS Decimal(2,1))",
            "SELECT 1 + 2, Length('abc')",
            "SELECT Int8('-1') + Uint16('0'), Uint8('255') + Int8('0'), Uint64('18446744073709551615') + Int64('0')",
            "SELECT IF(true, Int8('-1'), Uint16('0')), IF(false, Int64('0'), Uint64('18446744073709551615'))",
            "SELECT CAST(Int8('-1') AS Uint16), CAST(Uint8('255') AS Int8), CAST(Uint64('18446744073709551615') AS Int64)",
            "SELECT Int8('-1') AS x UNION ALL SELECT Uint16('0') AS x ORDER BY x",
            "SELECT Uint64('18446744073709551615') AS x UNION SELECT Int64('-1') AS x",
            "SELECT Float('1') + 2, Float('1') + Double('2'), IF(false, 1, Float('nan'))",
            "SELECT Length(Utf8('hello')), StartsWith('abc', 'a'), EndsWith('abc', 'c'), String::AsciiToLower('ABC'), String::AsciiToUpper('abc')",
            "SELECT -128t, -2147483648, -9223372036854775808, -1u",
            "SELECT 7 / 2, 7 % 2, 1 / 0, 1 % 0",
            "SELECT 7.0 / 2.0, 1.0 / 0.0, 0.0 / 0.0, -1.0 / 0.0, 1.0 / Double('-0')",
            "SELECT -5.5 % 2.0, Float('-5.5') % Float('2'), 1.0 % 0.0, Double('inf') % 2.0, 1.0 % Double('inf')",
            "SELECT Float('1') / Float('0'), Float('0') / Float('0'), IF(false, 1.0) / 2.0",
            "SELECT CAST(42 AS Double), CAST(42 AS Float), CAST(1.5 AS Float), CAST(Float('1.5') AS Double)",
            "SELECT CAST(9223372586610589697ul AS Float), CAST(4611686293305294849L AS Float)",
            "SELECT CAST(Double('-4.9406564584124654e-324') AS Uint64), CAST(Float('-1e-40') AS Uint8)",
            "SELECT CAST(1.5 AS Int32), CAST(127.5 AS Int8), CAST(-0.5 AS Uint64), CAST(Double('nan') AS Int32)",
            "SELECT CAST(42 AS Bool), CAST(0 AS Bool), CAST(true AS Int32), CAST(false AS Double), CAST(Double('nan') AS Bool)",
            "SELECT CAST(42 AS String), CAST(true AS Utf8), CAST('abc' AS Utf8), CAST(Utf8('abc') AS String)",
            "SELECT CAST(' 1.5' AS Double), CAST('0x10' AS Float), CAST('1e9999' AS Double), CAST('-1e-9999' AS Double)",
            "SELECT CAST('TRUE' AS Bool), CAST('yes' AS Bool), CAST('1' AS Bool), CAST('nan' AS Double)",
            "SELECT Substring('abcdef', 2), Substring('abcdef', 2, 3), Substring('abcdef', 99), Substring('abcdef', 0, 0)",
            "SELECT Substring('abcdef', NULL), Substring('abcdef', NULL, 2), Substring('abcdef', 2, NULL), Substring(NULL, 2)",
            "SELECT Substring(IF(false, 'abcdef'), 2), Substring('abcdef', IF(false, 2u), IF(false, 3u))",
            "SELECT CAST(1 AS Int64), CAST(300 AS Uint8), CAST(-1 AS Uint32), CAST('123' AS Int32)",
            "SELECT Int32('-2147483648'), Uint64('18446744073709551615'), Utf8('hello'), Date('2024-02-29')",
            "SELECT Datetime('1970-01-01T01:30:00+01:30'), Timestamp('1970-01-01T00:00:01.23456789Z')",
            "SELECT IF(TRUE, 1, 2), IF(FALSE, 1), IF(NULL, 1, 2)",
            "SELECT CASE WHEN 1 = NULL THEN 1 ELSE 2 END, CASE WHEN FALSE THEN 1 ELSE NULL END",
            "SELECT COALESCE(NULL, 1), COALESCE(1 / 0, 2), COALESCE(IF(FALSE, 1), 3)",
            "SELECT 'abc' LIKE 'a%', 'abc' LIKE '%c', 'abc' LIKE '%b%', 'a' || 'b'",
            "SELECT IF(FALSE, 'abc') LIKE 'a%', IF(FALSE, 'a') || 'b'"
        }) {
            Cerr << "Native/legacy parity SQL: " << sql << Endl;
            try {
                const auto actual = native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(actual.IsSuccess(), TStringBuilder() << sql << '\n' << actual.GetIssues().ToString());
                const auto expected = legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(expected.IsSuccess(), TStringBuilder() << sql << '\n' << expected.GetIssues().ToString());
                AssertSame(Snapshot(actual), Snapshot(expected));
            } catch (const std::exception& error) {
                failures += TStringBuilder() << sql << '\n' << error.what() << '\n';
            }
        }
        UNIT_ASSERT_C(failures.empty(), failures);
    }

    Y_UNIT_TEST(RuntimeUnwrapMatchesLegacyIncludingLazyMessages) {
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto session = runner.GetTableClient().CreateSession().GetValueSync().GetSession();
            size_t index = 0;
            for (const auto value : std::initializer_list<std::optional<i32>>{7, std::nullopt, -8}) {
                const auto params = TParamsBuilder().AddParam("$v").OptionalInt32(value).Build()
                    .AddParam("$msg").Utf8("missing value").Build().Build();
                for (const auto* expression : {"Unwrap($v)", "Unwrap($v, $msg)", "IF($v IS NULL, 9, Unwrap($v, $msg))",
                        "Unwrap($v, Unwrap(CAST(NULL AS Utf8), 'missing message'))"}) {
                    const TString sql = TStringBuilder() << "DECLARE $v AS Int32?; DECLARE $msg AS Utf8; SELECT " << expression << " AS answer";
                    const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                    UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
                    if (!value && !TStringBuf(expression).StartsWith("IF")) {
                        UNIT_ASSERT(!result.IsSuccess());
                        UNIT_ASSERT_C(result.GetIssues().ToString().find("Failed to unwrap empty optional") != std::string::npos, result.GetIssues().ToString());
                        if (TStringBuf(expression).Contains("$msg"))
                            UNIT_ASSERT_C(result.GetIssues().ToString().find("missing value") != std::string::npos, result.GetIssues().ToString());
                        if (TStringBuf(expression).Contains("missing message"))
                            UNIT_ASSERT_C(result.GetIssues().ToString().find("missing message") != std::string::npos, result.GetIssues().ToString());
                        continue;
                    }
                    const auto actual = Snapshot(result);
                    UNIT_ASSERT_VALUES_EQUAL(actual.Values, TStringBuilder() << "[[" << value.value_or(9) << "]]");
                    if (strategy == 2) reference.push_back(actual);
                    else AssertSame(actual, reference.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST(RuntimeDateParsingAndIntervalsMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const bool utf8 : {false, true}) {
            const TString sql = TStringBuilder() << "DECLARE $s AS " << (utf8 ? "Utf8" : "String") << R"(?;
                DECLARE $amount AS Int32?;
                SELECT CAST($s AS Date), DateTime::IntervalFromDays($amount),
                    DateTime::IntervalFromHours($amount), DateTime2::IntervalFromMinutes($amount),
                    CAST($s AS Date) + DateTime::IntervalFromDays($amount)
            )";
            for (const auto& text : TVector<std::optional<std::string>>{std::nullopt, "", "1970-1-1", "1998-3-18", "2000-02-29",
                    "2100-02-29", "2105-12-31", "2106-1-1", "1998-003-18", " 1998-03-18", "1998-03-18T00:00:00Z"}) {
                for (const auto amount : std::initializer_list<std::optional<i32>>{std::nullopt, std::numeric_limits<i32>::lowest(),
                        -213503983, -213503982, -49673, -49672, -1, 0, 1, 49672, 49673, 213503982, 213503983, std::numeric_limits<i32>::max()}) {
                    auto builder = TParamsBuilder();
                    if (utf8) builder.AddParam("$s").OptionalUtf8(text).Build();
                    else builder.AddParam("$s").OptionalString(text).Build();
                    builder.AddParam("$amount").OptionalInt32(amount).Build();
                    const auto params = builder.Build();
                    AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                        Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
                }
            }
        }
    }

    Y_UNIT_TEST(RuntimeListAccessPreservesNestedOptionalPresence) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const TString sql = R"(
            DECLARE $items AS List<Int32?>;
            DECLARE $index AS Int64?;
            SELECT $items[$index] AS v, $items[$index] IS NULL AS missing
        )";
        for (const auto index : {std::optional<i64>{}, std::optional<i64>{-1}, std::optional<i64>{0},
                std::optional<i64>{1}, std::optional<i64>{2}, std::optional<i64>{Max<i64>()}}) {
            auto builder = TParamsBuilder();
            builder.AddParam("$items").BeginList().AddListItem().OptionalInt32(7)
                .AddListItem().OptionalInt32(std::nullopt).EndList().Build();
            builder.AddParam("$index").OptionalInt64(index).Build();
            const auto params = builder.Build();
            AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
        }
    }

    Y_UNIT_TEST(RuntimeUrlFunctionsPreserveLegacyNullAndCaseSemantics) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const TString sql = R"(
            DECLARE $url AS String?;
            SELECT Url::GetHost($url), Url::CutWWW($url), Url::CutWWW(Url::GetHost($url))
        )";
        for (const auto* url : {static_cast<const char*>(nullptr), "", "http:///", "www.",
                "https://www.example.org:443/path?query#fragment", "WWW.Example.org", "localhost:80/path",
                "mailto:name@example.org", "https://user:pass@example.org/a", "http://[::1]:80/a"}) {
            auto builder = TParamsBuilder();
            builder.AddParam("$url").OptionalString(url ? std::optional<std::string>{url} : std::nullopt).Build();
            const auto params = builder.Build();
            AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
        }
    }

    Y_UNIT_TEST(RuntimeCalendarOperationsMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const TString sql = R"(
            DECLARE $date AS Date?;
            DECLARE $time AS Datetime?;
            DECLARE $stamp AS Timestamp?;
            DECLARE $shift AS Int32;
            $calendar = DateTime::ShiftMonths($stamp, $shift);
            SELECT DateTime::GetMinute($stamp), DateTime::GetMinute($calendar),
                DateTime::ToSeconds($date), DateTime::ToSeconds($time), DateTime::ToSeconds($stamp),
                DateTime::GetYear($stamp), DateTime::GetYear($calendar),
                DateTime::MakeTimestamp($calendar), DateTime::MakeDate($calendar),
                DateTime::MakeDate(DateTime::ShiftYears($date, $shift)),
                DateTime::MakeDatetime(DateTime::ShiftMonths($time, $shift)),
                DateTime::MakeTimestamp(DateTime::ShiftYears($calendar, $shift)),
                DateTime::MakeDate(DateTime::Split($stamp)),
                CAST($date AS Timestamp), CAST($stamp AS Date), CAST($stamp AS Datetime)
        )";
        for (const auto* text : {static_cast<const char*>(nullptr), "1970-01-01T00:00:00Z", "2000-02-29T12:34:56.123456Z",
                "2020-01-31T23:59:59.999999Z", "2020-03-31T12:34:56.123456Z", "2105-12-31T23:59:59.999999Z"}) {
            for (const i32 shift : {-49152, -4096, -13, -1, 0, 1, 12, 100, 4096, 49152}) {
                const auto instant = text ? std::optional<TInstant>{TInstant::ParseIso8601(text)} : std::nullopt;
                auto builder = TParamsBuilder();
                builder.AddParam("$date").OptionalDate(instant).Build();
                builder.AddParam("$time").OptionalDatetime(instant).Build();
                builder.AddParam("$stamp").OptionalTimestamp(instant).Build();
                builder.AddParam("$shift").Int32(shift).Build();
                const auto params = builder.Build();
                AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                    Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
            }
        }
        for (const auto* query : {
                "SELECT DateTime::GetYear(Date('2020-02-29')), DateTime::MakeTimestamp(DateTime::Split(Date('2020-02-29')))",
                "SELECT DateTime::MakeDate(Date('2020-02-29')), DateTime::MakeDatetime(Timestamp('2020-02-29T12:34:56.123456Z'))",
                "SELECT CAST(Date('2020-01-01') AS Timestamp), CAST(Timestamp('2020-01-01T23:59:59.999999Z') AS Datetime), CAST(Timestamp('2020-01-01T23:59:59.999999Z') AS Date)",
                "SELECT CAST(1 AS Date), CAST(1 AS Datetime), CAST(1 AS Timestamp), CAST(-1 AS Date), CAST(49673 AS Date), CAST(4291747200ul AS Datetime), CAST(4291747200000000ul AS Timestamp)",
                "SELECT CAST(Uint8('1') AS Date), CAST(Uint16('1') AS Datetime), CAST(Uint32('1') AS Timestamp), CAST('2020-02-29' AS Date), CAST('2020-02-30' AS Date), CAST(NULL AS Timestamp)",
                "SELECT DateTime::GetYear(CAST(NULL AS Date)), DateTime::MakeDate(DateTime::ShiftMonths(CAST(NULL AS Timestamp), 1))"}) {
            AssertSame(Snapshot(native.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync()));
        }
        // Bare NULL has no temporal type for these polymorphic operations.
        for (const auto* query : {"SELECT DateTime::GetYear(NULL)", "SELECT DateTime::MakeDate(DateTime::ShiftMonths(NULL, 1))"}) {
            UNIT_ASSERT(!native.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
            UNIT_ASSERT(!legacy.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
        }
    }

    Y_UNIT_TEST(ValueByAggregatesPreserveLegacyRowsAndTypes) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        struct TRow { i32 Group; std::optional<i32> Key; std::optional<i32> Value; };
        const TRow rows[]{{1, 20, 200}, {1, 10, 100}, {1, std::nullopt, 300},
            {2, std::nullopt, 700}, {3, 4, std::nullopt}, {4, 10, 100}, {4, 10, 100}};
        for (const bool optionalKey : {false, true}) for (const bool optionalValue : {false, true}) {
            TParamsBuilder builder;
            auto& list = builder.AddParam("$rows").BeginList();
            for (const auto& row : rows) {
                if ((!optionalKey && !row.Key) || (!optionalValue && !row.Value)) continue;
                list.AddListItem().BeginStruct().AddMember("g").Int32(row.Group);
                if (optionalKey) list.AddMember("k").OptionalInt32(row.Key);
                else list.AddMember("k").Int32(*row.Key);
                if (optionalValue) list.AddMember("v").OptionalInt32(row.Value);
                else list.AddMember("v").Int32(*row.Value);
                list.EndStruct();
            }
            list.EndList().Build();
            const auto params = builder.Build();
            const TString declaration = TStringBuilder() << "DECLARE $rows AS List<Struct<g:Int32,k:Int32"
                << (optionalKey ? "?" : "") << ",v:Int32" << (optionalValue ? "?" : "") << ">>; ";
            for (const TStringBuf suffix : {"", " WHERE false", " WHERE k IS NULL", " WHERE g = 3"}) {
                const TString sql = declaration + "SELECT MIN_BY(v,k) AS lo, MAX_BY(v,k) AS hi FROM AS_TABLE($rows)" + TString(suffix);
                const auto expected = legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                const auto actual = native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                AssertSame(Snapshot(actual), Snapshot(expected));
            }
            const TString sql = declaration + "SELECT g, MIN_BY(v,k) AS lo, MAX_BY(v,k) AS hi FROM AS_TABLE($rows) GROUP BY g ORDER BY g";
            AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
        }
    }

    Y_UNIT_TEST_TWIN(RuntimeLikeEscapeAndUnderscoresMatchLegacy, CaseSensitive) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const char* patterns[]{"", "%", "_", "a_", "_b", "a_b", "a#_b", "a#%b", "a##b", "a%b%_", "%_b", "é_", "a%b%", "%a%b", "Σ_", "k%", "123%", "%Ж%", "😀%", "中"};
        for (const bool utf8 : {false, true}) for (const bool utf8Pattern : {false, true}) {
            TVector<std::optional<std::string>> inputs{std::nullopt, "", "ab", "a_b", "a%b", "a#b", "a\nb", "éж", "ÉЖ", "A_B", "AbBa", "ςσ", "Kelvin", "12345", "😀x", "中", std::string{"a\0b", 3}};
            if (!utf8) inputs.push_back(std::string{"a\xff" "b", 3});
            TString sql = utf8 ? "DECLARE $s AS Utf8?; " : "DECLARE $s AS String?; ";
            for (std::size_t i = 0; i < std::size(patterns); ++i)
                sql += TStringBuilder() << "DECLARE $p" << i << " AS String; ";
            sql += "SELECT ";
            for (std::size_t i = 0; i < std::size(patterns); ++i) {
                if (i) sql += ", ";
                sql += TStringBuilder() << "$s " << (CaseSensitive ? "LIKE" : "ILIKE") << " '" << patterns[i]
                    << (utf8Pattern ? "'u" : "'") << " ESCAPE '#' AS lit" << i
                    << ", $s " << (CaseSensitive ? "LIKE" : "ILIKE") << " $p" << i << " ESCAPE '#' AS dyn" << i;
            }
            for (const auto& input : inputs) {
                TParamsBuilder builder;
                auto& value = builder.AddParam("$s");
                if (utf8) value.OptionalUtf8(input).Build();
                else value.OptionalString(input).Build();
                for (std::size_t i = 0; i < std::size(patterns); ++i)
                    builder.AddParam(TStringBuilder() << "$p" << i).String(patterns[i]).Build();
                const auto params = builder.Build();
                const auto expected = legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                const auto actual = native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                AssertSame(Snapshot(actual), Snapshot(expected));
            }
        }
    }


    Y_UNIT_TEST(StaticStringCallableFormsMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const char* functions[]{"Contains", "StartsWith", "EndsWith", "_yql_AsciiEqualsIgnoreCase",
            "_yql_AsciiContainsIgnoreCase", "_yql_AsciiStartsWithIgnoreCase", "_yql_AsciiEndsWithIgnoreCase"};
        for (const bool utf8 : {false, true}) {
            TString sql = utf8 ? "DECLARE $s AS Utf8?; " : "DECLARE $s AS String?; ";
            sql += "DECLARE $p AS String; ";
            for (std::size_t i = 0; i < std::size(functions); ++i)
                sql += TStringBuilder() << "$f" << i << " = Udf(String::" << functions[i] << "); $g" << i << " = $f" << i << "; ";
            sql += "SELECT ";
            for (std::size_t i = 0; i < std::size(functions); ++i) {
                if (i) sql += ", ";
                sql += TStringBuilder() << "String::" << functions[i] << "($s, $p) AS direct" << i
                    << ", Udf(String::" << functions[i] << ")($s, $p) AS invoke" << i
                    << ", $g" << i << "($s, $p) AS bound" << i;
            }
            TVector<std::optional<std::string>> inputs{std::nullopt, "", "abc", "ABC", "aBc", "Éé", "Kk", std::string{"a\0b", 3}};
            if (!utf8) inputs.push_back(std::string{"a\xff" "b", 3});
            for (const auto& input : inputs) for (const auto* pattern : {"", "a", "A", "bc", "ABC", "é", "k"}) {
                TParamsBuilder builder;
                if (utf8) builder.AddParam("$s").OptionalUtf8(input).Build();
                else builder.AddParam("$s").OptionalString(input).Build();
                builder.AddParam("$p").String(pattern).Build();
                const auto params = builder.Build();
                const auto expected = legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                const auto actual = native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                AssertSame(Snapshot(actual), Snapshot(expected));
            }
        }
    }

    Y_UNIT_TEST(RuntimePercentLikePatternsMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const char* patterns[]{"", "%", "%%", "a%b", "%a%b%", "a%b%", "%a%b", "ab%b", "%ab%ba%", "a%%b%a", "%Customer%Complaints%", "%é%ж%"};
        for (const bool utf8 : {false, true}) {
            TVector<std::optional<std::string>> inputs{std::nullopt, "", "a", "ab", "abb", "abc", "aba", "abba", "aaabbb", "a\nb", std::string{"a\0b", 3}, "Customer Complaints", "Complaints Customer", "é_ж"};
            if (!utf8) {
                inputs.push_back(std::string{"a\xff" "b", 3});
                inputs.push_back(std::string{"\xc0\xaf" "ab", 4});
            }
            TString sql = utf8 ? "DECLARE $s AS Utf8?; SELECT " : "DECLARE $s AS String?; SELECT ";
            for (std::size_t i = 0; i < std::size(patterns); ++i) {
                if (i) sql += ", ";
                sql += TStringBuilder() << "$s LIKE '" << patterns[i] << "' AS v" << i;
            }
            for (const auto& input : inputs) {
                auto builder = TParamsBuilder();
                auto& value = builder.AddParam("$s");
                if (utf8) value.OptionalUtf8(input).Build();
                else value.OptionalString(input).Build();
                const auto params = builder.Build();
                const auto expected = legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                UNIT_ASSERT_C(expected.IsSuccess(), expected.GetIssues().ToString());
                const auto actual = native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                UNIT_ASSERT_C(actual.IsSuccess(), actual.GetIssues().ToString());
                UNIT_ASSERT_C(actual.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, actual.GetIssues().ToString());
                AssertSame(Snapshot(actual), Snapshot(expected));
            }
        }
    }

    Y_UNIT_TEST(RuntimeFloatingCastsAndSubstringMatchLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const TString sql = R"(
            DECLARE $x AS Double;
            DECLARE $y AS Double?;
            DECLARE $text AS String?;
            DECLARE $start AS Uint32?;
            DECLARE $count AS Uint32?;
            SELECT $x / $y AS ratio, CAST($x AS Int32) AS integral,
                CAST($x AS Float) AS rounded, Substring($text, $start, $count) AS part
        )";
        for (const double x : {7.5, -0.5, 2147483648.0}) for (const unsigned scenario : {0, 1, 2, 3}) {
            const auto params = TParamsBuilder()
                .AddParam("$x").Double(x).Build()
                .AddParam("$y").OptionalDouble(scenario == 0 ? std::nullopt : std::optional<double>{scenario == 1 ? 0.0 : 2.0}).Build()
                .AddParam("$text").OptionalString(scenario == 3 ? std::nullopt : std::optional<TString>{"abcdef"}).Build()
                .AddParam("$start").OptionalUint32(scenario == 1 ? std::nullopt : std::optional<ui32>{2}).Build()
                .AddParam("$count").OptionalUint32(scenario == 2 ? std::nullopt : std::optional<ui32>{3}).Build()
                .Build();
            AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
        }
    }

    Y_UNIT_TEST(RuntimeUnicodeCaseMatchesBothLegacyPaths) {
        const TString sql = R"(
            DECLARE $x AS Utf8;
            DECLARE $optional AS Utf8?;
            DECLARE $bytes AS String?;
            SELECT Unicode::ToUpper($x), Unicode::ToLower($x),
                Unicode::ToUpper($optional), Unicode::ToLower($optional), Unicode::ToUpper(CAST($bytes AS Utf8)),
                Unicode::ToUpper('école'), Unicode::ToLower(Utf8('Σ')), Unicode::ToUpper(NULL)
        )";
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto session = runner.GetTableClient().CreateSession().GetValueSync().GetSession();
            size_t index = 0;
            for (const auto& [input, upper, lower] : std::initializer_list<std::tuple<std::string, std::string, std::string>>{
                    {"", "", ""}, {"AbC", "ABC", "abc"}, {"École", "ÉCOLE", "école"},
                    {"Привет", "ПРИВЕТ", "привет"}, {"σςΣ", "ΣΣΣ", "σςσ"}, {"ıIİi", "IIİI", "ıiii"},
                    {"ſS", "SS", "ſs"}, {"ȿⱾ", "ⱾⱾ", "ȿȿ"}, {"e\u0301", "E\u0301", "e\u0301"},
                    {"\U00010428\U00010400😀", "\U00010400\U00010400😀", "\U00010428\U00010428😀"},
                    {std::string("a\0B", 3), std::string("A\0B", 3), std::string("a\0b", 3)}}) {
                for (const bool present : {false, true}) {
                    auto builder = TParamsBuilder();
                    builder.AddParam("$x").Utf8(input).Build();
                    builder.AddParam("$optional").OptionalUtf8(present ? std::optional<std::string>{input} : std::nullopt).Build();
                    builder.AddParam("$bytes").OptionalString(present ? input : std::string("\xff", 1)).Build();
                    const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), builder.Build()).GetValueSync();
                    const auto actual = Snapshot(result);
                    TResultSetParser values(result.GetResultSet(0));
                    UNIT_ASSERT(values.TryNextRow());
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(0).GetUtf8(), upper);
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(1).GetUtf8(), lower);
                    UNIT_ASSERT(values.ColumnParser(2).GetOptionalUtf8() == (present ? std::optional<std::string>{upper} : std::nullopt));
                    UNIT_ASSERT(values.ColumnParser(3).GetOptionalUtf8() == (present ? std::optional<std::string>{lower} : std::nullopt));
                    UNIT_ASSERT(values.ColumnParser(4).GetOptionalUtf8() == (present ? std::optional<std::string>{upper} : std::nullopt));
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(5).GetUtf8(), "ÉCOLE");
                    UNIT_ASSERT_VALUES_EQUAL(values.ColumnParser(6).GetUtf8(), "σ");
                    UNIT_ASSERT(!values.ColumnParser(7).GetOptionalUtf8());
                    if (strategy == 2) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST(RuntimeDecimalIntegerOperationsMatchBothLegacyPaths) {
        const TString sql = R"(
            DECLARE $a AS Decimal(3,2)?;
            DECLARE $i AS bigint?;
            DECLARE $u AS Uint64?;
            SELECT $a * CAST($i AS Int8), $a * CAST($i AS Int16), $a * CAST($i AS Int32), $a * $i, $i * $a,
                $a * CAST($u AS Uint8), $a * CAST($u AS Uint16), $a * CAST($u AS Uint32), $a * $u, $u * $a,
                $a / CAST($i AS tinyint), $a / CAST($i AS SMALLINT), $a / CAST($i AS integer), $a / $i,
                $a / CAST($u AS Uint8), $a / CAST($u AS Uint16), $a / CAST($u AS Uint32), $a / $u,
                CAST($a / 50 AS int), CAST($i AS varchar), tinyint('12'), SMALLINT('12'),
                int('12'), Integer('12'), BIGINT('12'), varchar('abc')
        )";
        TVector<TResultSnapshot> reference;
        for (const int strategy : {2, 0}) {
            NKikimrConfig::TAppConfig config;
            auto* service = config.MutableTableServiceConfig();
            service->SetEnableNewRBO(strategy != 0);
            service->SetNativeFrontendMode(strategy == 2 ? NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED : NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
            service->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
            auto session = runner.GetTableClient().CreateSession().GetValueSync().GetSession();
            size_t index = 0;
            for (const auto* decimal : {static_cast<const char*>(nullptr), "0", "0.01", "-0.01", "0.02", "-0.02", "1.25", "-1.25", "9.99", "-9.99", "inf", "-inf", "nan"}) {
                for (const auto& [signedValue, unsignedValue] : std::initializer_list<std::pair<std::optional<i64>, std::optional<ui64>>>{
                        {std::nullopt, std::nullopt}, {0, 0}, {1, 1}, {-1, 1}, {-2, 2}, {3, 3}, {-3, 3}, {127, 255}, {-128, 256},
                        {32767, 65535}, {-32768, 65536}, {2147483647, 4294967295ULL},
                        {std::numeric_limits<i64>::lowest(), std::numeric_limits<ui64>::max()}}) {
                    auto builder = TParamsBuilder();
                    auto& value = builder.AddParam("$a");
                    if (decimal) value.BeginOptional().Decimal(TDecimalValue(decimal, 3, 2)).EndOptional().Build();
                    else value.EmptyOptional(TTypeBuilder().Decimal({3, 2}).Build()).Build();
                    builder.AddParam("$i").OptionalInt64(signedValue).Build();
                    builder.AddParam("$u").OptionalUint64(unsignedValue).Build();
                    const auto params = builder.Build();
                    const auto actual = Snapshot(session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync());
                    if (strategy == 2) reference.push_back(actual); else AssertSame(actual, reference.at(index));
                    ++index;
                }
            }
        }
    }

    Y_UNIT_TEST(RuntimeDecimalMatchesLegacy) {
        TKikimrRunner nativeRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        TKikimrRunner legacyRunner(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
        auto native = nativeRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto legacy = legacyRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
        const TString sql = R"(
            DECLARE $a AS Decimal(8,2);
            DECLARE $b AS Decimal(8,2)?;
            DECLARE $i AS Int64;
            SELECT $a > $i, $a = $i, IF($a > $i, $a, $i), COALESCE($b, CAST($i AS Decimal(8,2))),
                $a + $b, $a - $b, $a * $b, $a / $b, $a % $b,
                $a = $b, $a != $b, $a < $b, $a <= $b, -$a, Abs($a),
                CAST($a AS Int32), CAST($a AS Float), CAST($a AS Double),
                CAST($a AS String), CAST($a AS Decimal(2,1))
        )";
        for (const auto [a, b] : {std::pair{"1.25", "2.00"}, {"999999.99", "999999.99"},
                {"-1.25", "0"}, {"0", "0"}, {"nan", "inf"}, {"inf", "inf"}, {"-inf", "inf"},
                {"1.25", static_cast<const char*>(nullptr)}}) {
            auto builder = TParamsBuilder();
            builder.AddParam("$a").Decimal(TDecimalValue(a, 8, 2)).Build();
            builder.AddParam("$i").Int64(-9007199254740993LL).Build();
            auto& optional = builder.AddParam("$b");
            if (b) optional.BeginOptional().Decimal(TDecimalValue(b, 8, 2)).EndOptional().Build();
            else optional.EmptyOptional(TTypeBuilder().Decimal({8, 2}).Build()).Build();
            const auto params = builder.Build();
            AssertSame(Snapshot(native.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()),
                Snapshot(legacy.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx(), params).GetValueSync()));
        }
    }

    Y_UNIT_TEST(TpcdsReadQueriesMatchLegacyOnPopulatedColumnTables) {
        const auto legacy = TpcdsReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY, false);
        const auto automatic = TpcdsReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY, true);
        const auto native = TpcdsReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED, true);
        UNIT_ASSERT_VALUES_EQUAL(automatic.size(), legacy.size());
        UNIT_ASSERT_VALUES_EQUAL(native.size(), legacy.size());
        for (std::size_t i = 0; i < legacy.size(); ++i) {
            AssertSame(native[i], automatic[i]);
            // The native and legacy frontends can order star columns differently.
            // Compare every value and type by name for those projections.
            if (i != 0 && (i < 9 || i >= 13) && i != 29 && i != 30 && i != 35 && i != 39) AssertSame(native[i], legacy[i]);
            else {
                AssertSameByName(native[i], legacy[i]);
            }
        }
    }

    Y_UNIT_TEST(RemainingTpcdsReadQueriesMatchBothCompilers) {
        // Execute every remaining original benchmark query, including the
        // deliberately contradictory sale_type predicates in Q4. Empty and
        // NULL-only outputs here still need stronger data fixtures; they are
        // not counted as the populated-result parity proof above.
        const auto native = TpcdsReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED, true, true);
        const auto automatic = TpcdsReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY, true, true);
        const auto legacy = TpcdsReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY, false, true);
        UNIT_ASSERT_VALUES_EQUAL(native.size(), automatic.size());
        UNIT_ASSERT_VALUES_EQUAL(native.size(), legacy.size());
        for (size_t i = 0; i < native.size(); ++i) {
            AssertSame(native[i], automatic[i]);
            AssertSameByName(native[i], legacy[i]);
        }
    }

    Y_UNIT_TEST(TpchReadQueriesMatchLegacyOnPopulatedColumnTables) {
        const auto legacy = TpchReadQueries(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY, false);
        for (const auto mode : {NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY,
                               NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED}) {
            const auto actual = TpchReadQueries(mode, true);
            UNIT_ASSERT_VALUES_EQUAL(actual.size(), legacy.size());
            for (std::size_t i = 0; i < legacy.size(); ++i) AssertSame(actual[i], legacy[i]);
        }
    }

    Y_UNIT_TEST(ConstantResultsMatchLegacy) {
        AssertSame(ConstantQuery(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED),
            ConstantQuery(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
    }

    Y_UNIT_TEST(GenericMultipleResultsAndScanMatchLegacy) {
        TVector<TResultSnapshot> previous;
        TString previousScan;
        for (const auto mode : {NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY,
                               NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED}) {
            TKikimrRunner kikimr(NativeSettings(mode));
            const auto result = kikimr.GetQueryClient().ExecuteQuery(
                "SELECT 1 AS first; SELECT IF(FALSE, 2) AS second;",
                NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
            for (size_t i = 0; i < 2; ++i) {
                auto snapshot = SnapshotRows(result.GetResultSet(i));
                if (mode == NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY) previous.push_back(std::move(snapshot));
                else AssertSame(snapshot, previous[i]);
            }
            auto scan = kikimr.GetTableClient().StreamExecuteScanQuery(
                "SELECT 1 AS value UNION ALL SELECT 2 AS value").GetValueSync();
            UNIT_ASSERT_C(scan.IsSuccess(), scan.GetIssues().ToString());
            auto values = StreamResultToYson(scan);
            if (mode == NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY) previousScan = std::move(values);
            else UNIT_ASSERT_VALUES_EQUAL(values, previousScan);
        }
    }

    Y_UNIT_TEST(PreparedParameterTypesAndRepeatedExecutionMatchLegacy) {
        const auto native = ParameterQueries(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED);
        const auto legacy = ParameterQueries(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
        UNIT_ASSERT_VALUES_EQUAL(native.size(), legacy.size());
        for (size_t i = 0; i < native.size(); ++i) {
            AssertSame(native[i], legacy[i]);
        }
    }

    Y_UNIT_TEST(TableResultNamesTypesAndNullsMatchLegacy) {
        AssertSame(TableQuery(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED),
            TableQuery(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY));
    }

    Y_UNIT_TEST(CatalogSnapshotsRefreshResultMetadataAfterAlter) {
        auto settings = NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        TKikimrRunner kikimr(settings);
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = tableSession.ExecuteSchemeQuery(
            "CREATE TABLE `/Root/catalog_snapshot` (id Int64 NOT NULL, value String, PRIMARY KEY(id));").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto rows = TValueBuilder().BeginList().AddListItem().BeginStruct()
            .AddMember("id").Int64(1).AddMember("value").OptionalString("one").EndStruct().EndList().Build();
        const auto written = kikimr.GetTableClient().BulkUpsert("/Root/catalog_snapshot", std::move(rows)).GetValueSync();
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        // Keep the reference PR's repeated SELECT * / ALTER / same SELECT *
        // sequence. A shared snapshot must not retain a previous catalog version.
        for (const size_t count : {2, 2, 3}) {
            if (count == 3) {
                const auto altered = tableSession.ExecuteSchemeQuery(
                    "ALTER TABLE `/Root/catalog_snapshot` ADD COLUMN extra String;").GetValueSync();
                UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());
            }
            const auto result = session.ExecuteQuery("SELECT * FROM `/Root/catalog_snapshot` ORDER BY id;",
                NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
            const auto& set = result.GetResultSet(0);
            UNIT_ASSERT_VALUES_EQUAL(set.GetColumnsMeta().size(), count);
            auto expected = SnapshotRows(set);
            UNIT_ASSERT_VALUES_EQUAL(expected.NamedValues, count == 2 ? "[[1;[\"one\"]]]" : "[[#;1;[\"one\"]]]");
            if (count == 3) {
                const auto& columns = set.GetColumnsMeta();
                const auto extra = std::find_if(columns.begin(), columns.end(), [](const auto& column) { return column.Name == "extra"; });
                UNIT_ASSERT(extra != columns.end());
                UNIT_ASSERT_VALUES_EQUAL(extra->Type.ToString(), "String?");
            }
        }
    }

    Y_UNIT_TEST_TWIN(AutoModeReportsUnsupportedFallback, DefaultMode) {
        auto settings = NativeSettings(DefaultMode ? NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY : NKikimrConfig::TTableServiceConfig::NATIVE_AUTO);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
        TKikimrRunner kikimr(settings);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto result = session.ExecuteDataQuery("SELECT ListLength(ListReverse([1, 2])) AS n",
            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") != std::string::npos, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        const auto next = session.ExecuteDataQuery("SELECT 3 AS n",
            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(Snapshot(next).Values, "[[3]]");
    }

    Y_UNIT_TEST(DefaultNewRboEntryBypassesTheYqlAstCache) {
        auto settings = NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        TKikimrRunner kikimr(settings);
        for (int round = 0; round < 2; ++round) {
            const auto result = kikimr.GetQueryClient().ExecuteQuery("SELECT q.* FROM (SELECT 1 AS value) AS q",
                NQuery::TTxControl::NoTx(), NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats() && result.GetStats()->GetAst());
            UNIT_ASSERT_C(result.GetStats()->GetAst()->find("native frontend: native-read-v1") != std::string::npos,
                *result.GetStats()->GetAst());
            UNIT_ASSERT(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos);
        }
    }

    Y_UNIT_TEST(RequiredModeRejectsUnsupportedWithoutFallback) {
        TKikimrRunner kikimr(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const auto* sql : {"SELECT ListLength(ListReverse([1, 2])) AS n", "SELECT * FROM 'table'"}) {
            const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), sql);
            UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(RequiredModeAcceptsQuotedLiteralAndResultNameWithoutFallback) {
        TKikimrRunner kikimr(NativeSettings(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const auto& [sql, name, values] : {
                std::tuple{"SELECT 'a''b'", "b", "[[\"a\"]]"},
                std::tuple{"SELECT 1 AS 'answer'", "answer", "[[1]]"}}) {
            const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos,
                result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).GetColumnsMeta().at(0).Name, name);
            UNIT_ASSERT_VALUES_EQUAL(Snapshot(result).Values, values);
        }
    }

    Y_UNIT_TEST_TWIN(AutoModeDoesNotFallbackForInvalidQueries, DefaultMode) {
        auto settings = NativeSettings(DefaultMode ? NKikimrConfig::TTableServiceConfig::NATIVE_LEGACY : NKikimrConfig::TTableServiceConfig::NATIVE_AUTO);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
        TKikimrRunner kikimr(settings);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const auto* sql : {"SELECT missing_column", "SELECT FROM", "SELECT NOT NOT true", "SELECT * AS x FROM t",
                               "$unused = *; SELECT 1"}) {
            const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), sql);
            UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos, result.GetIssues().ToString());
        }
    }
}

} // namespace NKikimr::NKqp
