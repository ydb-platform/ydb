#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;


namespace {

void CreateSimpleDataTypes(TKikimrRunner& kikimr) {
    TTableClient tableClient{kikimr.GetDriver()};
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/SimpleDataTypes` (
            col_bool Bool,
            col_uint64 Uint64,
            col_int32 Int32,
            col_double Double,
            col_float Float,
            col_string String,
            col_utf8 Utf8,
            col_date Date,
            col_datetime Datetime,
            col_timestamp Timestamp,
            col_interval Interval,
            col_decimal Decimal(22, 9),
            col_decimal_35 Decimal(35, 10),
            PRIMARY KEY (col_uint64)
        ) WITH (
            PARTITION_AT_KEYS = (100, 200, 300)
        );)").GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    result = session.ExecuteDataQuery(R"(
        --!syntax_v1
        REPLACE INTO `/Root/SimpleDataTypes` (col_bool, col_uint64, col_int32,
            col_double, col_float, col_string, col_utf8, col_date, col_datetime,
            col_timestamp, col_interval, col_decimal, col_decimal_35) VALUES
            (NULL, NULL, -1, 1.0, 1.0f, "Value-001", "値-001",
            Date("2021-01-01"), Datetime("2021-01-01T01:01:01Z"), Timestamp("2021-01-01T01:01:01.111111Z"),
            Interval("P1DT1H1M1.111111S"), Decimal("1.11", 22, 9), Decimal("155555555555555", 35, 10)),
            (false, 2u, NULL, NULL, 2.0f, "Value-002", "値-002",
            Date("2021-02-02"), Datetime("2021-07-31T02:02:02Z"), Timestamp("2021-02-02T02:02:02.222222Z"),
            Interval("P2DT2H2M2.222S"), Decimal("2.22", 22, 9), Decimal("255555555555555", 35, 10)),
            (false, 101u, -101, 101.101, NULL, NULL, "値-101",
            Date("2021-02-02"), Datetime("2021-05-31T10:10:10Z"), Timestamp("2021-10-10T10:10:10.101101Z"),
            Interval("P101DT10H10M10.101101S"), Decimal("101.101", 22, 9), Decimal("355555555555555", 35, 10)),
            (true, 102u, -102, 102.102, 102.0f, "Value-102", NULL,
            Date("2021-12-12"), Datetime("2021-12-12T10:10:10Z"), Timestamp("2021-12-12T10:10:10.102102Z"),
            Interval("P102DT10H10M10.102102S"), Decimal("102.102", 22, 9), Decimal("455555555555555", 35, 10)),
            (false, 201u, -201, 201.201, 201.201f, "Value-201", "値-201",
            NULL, NULL, Timestamp("2021-12-21T10:10:10.201201Z"),
            Interval("P201DT10H10M10.201201S"), Decimal("201.201", 22, 9), Decimal("555555555555555", 35, 10)),
            (true, 202u, -202, 202.202, 202.202f, "Value-202", "値-202",
            Date("2021-12-22"), Datetime("2021-12-22T10:10:10Z"), NULL,
            NULL, Decimal("202.202", 22, 9), Decimal("655555555555555", 35, 10)),
            (true, 301u, -301, 301.301, 301.301f, "Value-301", "値-301",
            Date("2021-05-31"), Datetime("2021-10-10T10:31:31Z"), Timestamp("2021-05-31T10:31:31.301301Z"),
            Interval("P301DT10H10M10.301301S"), NULL, NULL),
            (false, 302u, -302, 302.302, 302.302f, "Value-302", "値-302",
            Date("2021-06-30"), Datetime("2021-05-31T10:32:32Z"), Timestamp("2021-06-30T10:32:32.302302Z"),
            Interval("P302DT10H10M10.302302S"), Decimal("302.302", 22, 9), Decimal("755555555555555", 35, 10))
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    session.Close();
}

void CheckPlanForMergeCn(const TMaybe<TString>& planJson, bool hasChildSort, const TString& sortOp) {
    UNIT_ASSERT(planJson);
    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*planJson, &plan, /* throwOnError */ true);

    auto mergeCn = FindPlanNodeByKv(plan, "Node Type", "Merge");
    UNIT_ASSERT(mergeCn.IsDefined());

    auto childSort = FindPlanNodeByKv(mergeCn, "Name", sortOp);
    if (hasChildSort) {
        UNIT_ASSERT(!childSort.IsDefined());
    } else {
        UNIT_ASSERT(childSort.IsDefined());
    }

    // Check that TopSort has no Merge Connection in children
    auto topSort = FindPlanNodeByKv(plan, "Name", sortOp);
    if (topSort.IsDefined()) {
        mergeCn = FindPlanNodeByKv(topSort, "Node Type", "Merge");
        UNIT_ASSERT(!mergeCn.IsDefined());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpMergeCn) {
    Y_UNIT_TEST(TopSortBy_PK_Uint64_Limit3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_uint64 FROM `/Root/SimpleDataTypes` ORDER BY col_uint64 LIMIT 3
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, true, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [[2u]];
            [[101u]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Int32_Limit3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_int32 FROM `/Root/SimpleDataTypes` ORDER BY col_int32 LIMIT 3
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [[-302]];
            [[-301]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortByDesc_Double_Limit3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_double FROM `/Root/SimpleDataTypes` ORDER BY col_double DESC LIMIT 3
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [[302.302]];
            [[301.301]];
            [[202.202]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Float_Limit4) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_float FROM `/Root/SimpleDataTypes` ORDER BY col_float LIMIT 4
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [[1.0]];
            [[2.0]];
            [[102.0]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_String_Limit3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_string FROM `/Root/SimpleDataTypes` ORDER BY col_string LIMIT 3
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [["Value-001"]];
            [["Value-002"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Utf8_Limit2) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_utf8 FROM `/Root/SimpleDataTypes` ORDER BY col_utf8 LIMIT 2
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [["値-001"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Date_Limit4) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_date, CAST(col_date AS String) FROM `/Root/SimpleDataTypes` ORDER BY col_date LIMIT 4
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#;#];
            [[18628u];["2021-01-01"]];
            [[18660u];["2021-02-02"]];
            [[18660u];["2021-02-02"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortByDesc_Datetime_Limit3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_datetime, CAST(col_datetime AS String) FROM `/Root/SimpleDataTypes` ORDER BY col_datetime DESC LIMIT 3
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [[1640167810u];["2021-12-22T10:10:10Z"]];
            [[1639303810u];["2021-12-12T10:10:10Z"]];
            [[1633861891u];["2021-10-10T10:31:31Z"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Timestamp_Limit2) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_timestamp, CAST(col_timestamp AS String) FROM `/Root/SimpleDataTypes` ORDER BY col_timestamp LIMIT 2
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#;#];
            [[1609462861111111u];["2021-01-01T01:01:01.111111Z"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Interval_Limit3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_interval, CAST(col_interval AS String) FROM `/Root/SimpleDataTypes` ORDER BY col_interval LIMIT 3
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#;#];
            [[90061111111];["P1DT1H1M1.111111S"]];
            [[180122222000];["P2DT2H2M2.222S"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Decimal_Limit5) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_decimal FROM `/Root/SimpleDataTypes` ORDER BY col_decimal LIMIT 5
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [["1.11"]];
            [["2.22"]];
            [["101.101"]];
            [["102.102"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortByDesc_Bool_And_PKUint64_Limit4) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_bool, col_uint64 FROM `/Root/SimpleDataTypes` ORDER BY col_bool DESC, col_uint64 LIMIT 4
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [[%true];[102u]];
            [[%true];[202u]];
            [[%true];[301u]];
            [[%false];[2u]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(TopSortBy_Date_And_Datetime_Limit4) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT CAST(col_date AS String) AS col_date, CAST(col_datetime AS String) AS col_datetime
            FROM `/Root/SimpleDataTypes` ORDER BY col_date, col_datetime LIMIT 4
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "TopSort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#;#];
            [["2021-01-01"];["2021-01-01T01:01:01Z"]];
            [["2021-02-02"];["2021-05-31T10:10:10Z"]];
            [["2021-02-02"];["2021-07-31T02:02:02Z"]]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(SortBy_PK_Uint64_Desc) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(false);
        TKikimrSettings ksettings;
        ksettings.SetAppConfig(app);

        TKikimrRunner kikimr{ksettings};
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_uint64 FROM `/Root/SimpleDataTypes` ORDER BY col_uint64 DESC
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "Sort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [[302u]];
            [[301u]];
            [[202u]];
            [[201u]];
            [[102u]];
            [[101u]];
            [[2u]];
            [#]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(SortBy_Int32) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        CreateSimpleDataTypes(kikimr);

        TString query = R"(
            SELECT col_int32 FROM `/Root/SimpleDataTypes` ORDER BY col_int32
        )";

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto res = CollectStreamResult(result);
        CheckPlanForMergeCn(res.PlanJson, false, "Sort");

        result = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [#];
            [[-302]];
            [[-301]];
            [[-202]];
            [[-201]];
            [[-102]];
            [[-101]];
            [[-1]]])", StreamResultToYson(result));
    }


}

} // namespace NKqp
} // namespace NKikimr

