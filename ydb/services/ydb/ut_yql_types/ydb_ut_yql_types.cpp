#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ymath.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

#ifndef _win_


void ExecSchemeYql(std::shared_ptr<grpc::Channel> channel, const TString &sessionId, const TString &yql)
{
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;

    Ydb::Table::ExecuteSchemeQueryRequest request;
    request.set_session_id(sessionId);
    request.set_yql_text(yql);

    Ydb::Table::ExecuteSchemeQueryResponse response;
    auto status = stub->ExecuteSchemeQuery(&context, request, &response);
    UNIT_ASSERT(status.ok());
        auto deferred = response.operation();

    UNIT_ASSERT(deferred.ready() == true);
    UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
}


void CheckYqlDecimalValues(std::shared_ptr<grpc::Channel> channel, const TString &sessionId, const TString &yql,
                           TVector<std::pair<i64, ui64>> vals, ui64 scale)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    TVector<std::pair<ui64, ui64>> halves;
    for (auto &pr : vals) {
        NYql::NDecimal::TInt128 val = pr.first;
        val *= Power(10ull, scale);
        if (val >= 0)
            val += pr.second;
        else
            val -= pr.second;
        halves.push_back(std::make_pair(reinterpret_cast<ui64*>(&val)[0], reinterpret_cast<ui64*>(&val)[1]));
    }

    auto &result_set = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(result_set.rows_size(), halves.size());
    for (size_t i = 0; i < halves.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(result_set.rows(i).items(0).low_128(), halves[i].first);
        UNIT_ASSERT_VALUES_EQUAL(result_set.rows(i).items(0).high_128(), halves[i].second);
    }
}

void CreateTable(std::shared_ptr<grpc::Channel> channel,
                 const Ydb::Table::CreateTableRequest &request)
{
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    Ydb::Table::CreateTableResponse response;
    auto status = stub->CreateTable(&context, request, &response);
    auto deferred = response.operation();
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(deferred.ready());
    UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
}

void CreateTableDecimal(std::shared_ptr<grpc::Channel> channel)
{
    Ydb::Table::CreateTableRequest request;
    request.set_path("/Root/table-1");
    auto &col = *request.add_columns();
    col.set_name("key");
    col.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT32);
    auto &col1 = *request.add_columns();
    col1.set_name("value1");
    auto &decimalType1 = *col1.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type();
    decimalType1.set_precision(1);
    decimalType1.set_scale(0);
    auto &col35 = *request.add_columns();
    col35.set_name("value35");
    auto &decimalType35 = *col35.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type();
    decimalType35.set_precision(35);
    decimalType35.set_scale(10);
    request.add_primary_key("key");

    CreateTable(channel, request);
}

Y_UNIT_TEST_SUITE(TYqlDecimalTests) {
    Y_UNIT_TEST(SimpleUpsertSelect) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        CreateTableDecimal(channel);

        ExecYql(channel, sessionId,
                "UPSERT INTO `/Root/table-1` (key, value1, value35) VALUES "
                "(1, CAST(\"1\" as DECIMAL(1,0)), CAST(\"1234567890.1234567891\" as DECIMAL(35,10))),"
                "(2, CAST(\"2\" as DECIMAL(1,0)), CAST(\"2234567890.1234567891\" as DECIMAL(35,10))),"
                "(3, CAST(\"3\" as DECIMAL(1,0)), CAST(\"3234567890.1234567891\" as DECIMAL(35,10)));");

        CheckYqlDecimalValues(channel, sessionId, "SELECT value1 FROM `/Root/table-1` WHERE key=1;",
                              {{1, 0}}, 0);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value1 FROM `/Root/table-1` WHERE key >= 1 AND key <= 3;",
                              {{1, 0}, {2, 0}, {3, 0}}, 0);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value35 FROM `/Root/table-1` WHERE key=1;",
                              {{1234567890, 1234567891}}, 10);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value35 FROM `/Root/table-1` WHERE key >= 1 AND key <= 3;",
                              {{1234567890, 1234567891}, {2234567890, 1234567891}, {3234567890, 1234567891}}, 10);
    }

    Y_UNIT_TEST(NegativeValues) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        CreateTableDecimal(channel);

        ExecYql(channel, sessionId,
                "UPSERT INTO `/Root/table-1` (key, value1, value35) VALUES "
                "(1, CAST(\"-1\" as DECIMAL(1,0)), CAST(\"-1234567890.1234567891\" as DECIMAL(35,10))),"
                "(2, CAST(\"-2\" as DECIMAL(1,0)), CAST(\"-2234567890.1234567891\" as DECIMAL(35,10))),"
                "(3, CAST(\"-3\" as DECIMAL(1,0)), CAST(\"-3234567890.1234567891\" as DECIMAL(35,10)));");

        CheckYqlDecimalValues(channel, sessionId, "SELECT value1 FROM `/Root/table-1` WHERE key=1;",
                              {{-1, 0}}, 0);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value1 FROM `/Root/table-1` WHERE key >= 1 AND key <= 3;",
                              {{-1, 0}, {-2, 0}, {-3, 0}}, 0);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value35 FROM `/Root/table-1` WHERE key=1;",
                              {{-1234567890, 1234567891}} ,10);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value35 FROM `/Root/table-1` WHERE key >= 1 AND key <= 3;",
                              {{-1234567890, 1234567891}, {-2234567890, 1234567891}, {-3234567890, 1234567891}}, 10);

    }

    Y_UNIT_TEST(DecimalKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        request.set_path("/Root/table-1");
        auto &col1 = *request.add_columns();
        col1.set_name("key");
        auto &decimalType = *col1.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type();
        decimalType.set_precision(22);
        decimalType.set_scale(9);
        auto &col2 = *request.add_columns();
        col2.set_name("value");
        col2.mutable_type()->CopyFrom(col1.type());
        request.add_primary_key("key");

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES
                (CAST("1" as DECIMAL(22,9)), CAST("1" as DECIMAL(22,9))),
                (CAST("22.22" as DECIMAL(22,9)), CAST("22.22" as DECIMAL(22,9))),
                (CAST("9999999999999.999999999" as DECIMAL(22,9)), CAST("9999999999999.999999999" as DECIMAL(22,9))),
                (CAST("-1" as DECIMAL(22,9)), CAST("-1" as DECIMAL(22,9))),
                (CAST("-22.22" as DECIMAL(22,9)), CAST("-22.22" as DECIMAL(22,9))),
                (CAST("-9999999999999.999999999" as DECIMAL(22,9)), CAST("-9999999999999.999999999" as DECIMAL(22,9)));
        )");

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"1\" as DECIMAL(22,9));",
                              {{1, 0}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"22.22\" as DECIMAL(22,9));",
                              {{22, 220000000}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"9999999999999.999999999\" as DECIMAL(22,9));",
                              {{9999999999999, 999999999}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"-1\" as DECIMAL(22,9));",
                              {{-1, 0}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"-22.22\" as DECIMAL(22,9));",
                              {{-22, 220000000}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"-9999999999999.999999999\" as DECIMAL(22,9));",
                              {{-9999999999999, 999999999}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key >= CAST(\"-22.22\" as DECIMAL(22,9))",
                              {{-22, 220000000}, {-1, 0},
                               {1, 0}, {22, 220000000}, {9999999999999, 999999999}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key < CAST(\"-22.22\" as DECIMAL(22,9))",
                              {{-9999999999999, 999999999}}, 9);

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key > CAST(\"-22.222\" as DECIMAL(22,9)) AND key < CAST(\"22.222\" as DECIMAL(22,9))",
                              {{-22, 220000000}, {-1, 0},
                               {1, 0}, {22, 220000000}}, 9);
    }
}

void CheckDateValues(std::shared_ptr<grpc::Channel> channel,
                     const TString &sessionId,
                     const TString &yql,
                     TVector<TInstant> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).uint32_value(), vals[i].Days());
    }
}

void CheckDatetimeValues(std::shared_ptr<grpc::Channel> channel,
                         const TString &sessionId,
                         const TString &yql,
                         TVector<TInstant> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).uint32_value(), vals[i].Seconds());
    }
}

void CheckTimestampValues(std::shared_ptr<grpc::Channel> channel,
                          const TString &sessionId,
                          const TString &yql,
                          TVector<TInstant> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).uint64_value(), vals[i].MicroSeconds());
    }
}

void CheckIntervalValues(std::shared_ptr<grpc::Channel> channel,
                          const TString &sessionId,
                          const TString &yql,
                          TVector<i64> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).int64_value(), vals[i]);
    }
}

Y_UNIT_TEST_SUITE(TYqlDateTimeTests) {
    Y_UNIT_TEST(SimpleUpsertSelect) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: UINT64 } } } }
            columns { name: "val1" type: { optional_type { item { type_id: DATE } } } }
            columns { name: "val2" type: { optional_type { item { type_id: DATETIME } } } }
            columns { name: "val3" type: { optional_type { item { type_id: TIMESTAMP } } } }
            columns { name: "val4" type: { optional_type { item { type_id: INTERVAL } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key, val1, val2, val3, val4) VALUES
                (1, CAST(0 as DATE),
                    CAST(0 as DATETIME),
                    CAST(0 as TIMESTAMP),
                    CAST(0 as INTERVAL)),
                (2, CAST(1000 as DATE),
                    CAST(1000 as DATETIME),
                    CAST(1000 as TIMESTAMP),
                    CAST(1000 as INTERVAL)),
                (3, CAST('2050-01-01' as DATE),
                    CAST('2050-01-01T00:00:00Z' as DATETIME),
                    CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP),
                    CAST(-1000 as INTERVAL));
        )___");

        CheckDateValues(channel, sessionId, "SELECT val1 FROM `/Root/table-1`;",
                        {TInstant::Zero(), TInstant::Days(1000), TInstant::ParseIso8601("2050-01-01T00:00:00Z")});

        CheckDatetimeValues(channel, sessionId, "SELECT val2 FROM `/Root/table-1`;",
                            {TInstant::Zero(), TInstant::Seconds(1000), TInstant::ParseIso8601("2050-01-01T00:00:00Z")});

        CheckTimestampValues(channel, sessionId, "SELECT val3 FROM `/Root/table-1`;",
                            {TInstant::Zero(), TInstant::MicroSeconds(1000), TInstant::ParseIso8601("2050-01-01T00:00:00Z")});

        CheckIntervalValues(channel, sessionId, "SELECT val4 FROM `/Root/table-1`;",
                            {0, 1000, -1000});
    }

    Y_UNIT_TEST(DateKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: DATE } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST('2000-01-01' as DATE)),
                (CAST('2020-01-01' as DATE)),
                (CAST('2050-01-01' as DATE));
        )___");

        CheckDateValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST('2050-01-01' as DATE);",
                        {TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDateValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key > CAST('2000-01-01' as DATE);",
                        {TInstant::ParseIso8601("2020-01-01T00:00:00Z"),
                         TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDateValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST('2050-01-01' as DATE);",
                        {TInstant::ParseIso8601("2000-01-01T00:00:00Z"),
                         TInstant::ParseIso8601("2020-01-01T00:00:00Z")});
    }

    Y_UNIT_TEST(DatetimeKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: DATETIME } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST('2000-01-01T00:00:00Z' as DATETIME)),
                (CAST('2020-01-01T00:00:00Z' as DATETIME)),
                (CAST('2050-01-01T00:00:00Z' as DATETIME));
        )___");

        CheckDatetimeValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST('2050-01-01T00:00:00Z' as DATETIME);",
                            {TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDatetimeValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key > CAST('2000-01-01T00:00:00Z' as DATETIME);",
                            {TInstant::ParseIso8601("2020-01-01T00:00:00Z"),
                             TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDatetimeValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST('2050-01-01T00:00:00Z' as DATETIME);",
                            {TInstant::ParseIso8601("2000-01-01T00:00:00Z"),
                             TInstant::ParseIso8601("2020-01-01T00:00:00Z")});
    }

    Y_UNIT_TEST(TimestampKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: TIMESTAMP } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST('2000-01-01T00:00:00.000000Z' as TIMESTAMP)),
                (CAST('2020-01-01T00:00:00.000000Z' as TIMESTAMP)),
                (CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP));
        )___");

        CheckTimestampValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP);",
                             {TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckTimestampValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key > CAST('2000-01-01T00:00:00.000000Z' as TIMESTAMP);",
                             {TInstant::ParseIso8601("2020-01-01T00:00:00Z"),
                              TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckTimestampValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP);",
                             {TInstant::ParseIso8601("2000-01-01T00:00:00Z"),
                              TInstant::ParseIso8601("2020-01-01T00:00:00Z")});
    }

    Y_UNIT_TEST(IntervalKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: INTERVAL } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST(0 as INTERVAL)),
                (CAST(1000 as INTERVAL)),
                (CAST(-1000 as INTERVAL));
        )___");

        CheckIntervalValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST(1000 as INTERVAL);",
                            {1000});
        CheckIntervalValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key >= CAST(0 as INTERVAL);",
                            {0, 1000});
        CheckIntervalValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST(1000 as INTERVAL);",
                            {-1000, 0});
    }

    Y_UNIT_TEST(SimpleOperations) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        {
            Ydb::Table::CreateTableRequest request;
            TString scheme(R"___(
                path: "/Root/table-1"
                columns { name: "key" type: { optional_type { item { type_id: UINT32 } } } }
                columns { name: "val" type: { optional_type { item { type_id: TIMESTAMP } } } }
                primary_key: ["key"]
            )___");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            CreateTable(channel, request);
        }
        {
            Ydb::Table::CreateTableRequest request;
            TString scheme(R"___(
                path: "/Root/table-2"
                columns { name: "key" type: { optional_type { item { type_id: UINT32 } } } }
                columns { name: "val" type: { optional_type { item { type_id: INTERVAL } } } }
                primary_key: ["key"]
            )___");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            CreateTable(channel, request);
        }

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key, val) VALUES
                (1, CAST('2000-01-01T00:00:00.000000Z' as TIMESTAMP)),
                (2, CAST('2020-01-01T00:00:00.000000Z' as TIMESTAMP));
        )___");
        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-2` (key, val) VALUES
                (1, CAST(3600000000 as INTERVAL)),
                (2, CAST(143123456 as INTERVAL));
        )___");

        ExecYql(channel, sessionId, R"___(
                $t1 = (SELECT val FROM `/Root/table-1` WHERE key = 1);
                $t2 = (SELECT val FROM `/Root/table-1` WHERE key = 2);
                $i1 = (SELECT val FROM `/Root/table-2` WHERE key = 1);
                $i2 = (SELECT val FROM `/Root/table-2` WHERE key = 2);
                UPSERT INTO `/Root/table-1` (key, val) VALUES
                (3, $t1 + $i1),
                (4, $t2 + $i2);
        )___");

        ExecYql(channel, sessionId, R"___(
                $t1 = (SELECT val FROM `/Root/table-1` WHERE key = 1);
                $t2 = (SELECT val FROM `/Root/table-1` WHERE key = 2);
                $i1 = (SELECT val FROM `/Root/table-2` WHERE key = 1);
                $i2 = (SELECT val FROM `/Root/table-2` WHERE key = 2);
                UPSERT INTO `/Root/table-2` (key, val) VALUES
                (3, $i1 + $i2),
                (4, $t2 - $t1),
                (5, $t1 - $t2);
        )___");

        CheckTimestampValues(channel, sessionId, "SELECT val FROM `/Root/table-1` WHERE key = 3;",
                             {TInstant::ParseIso8601("2000-01-01T01:00:00Z")});

        CheckTimestampValues(channel, sessionId, "SELECT val FROM `/Root/table-1` WHERE key = 4;",
                             {TInstant::ParseIso8601("2020-01-01T00:00:00Z") + TDuration::MicroSeconds(143123456)});

        CheckIntervalValues(channel, sessionId, "SELECT val FROM `/Root/table-2` WHERE key = 3;",
                            {3743123456});

        auto diff = TInstant::ParseIso8601("2020-01-01T00:00:00Z") - TInstant::ParseIso8601("2000-01-01T00:00:00Z");
        CheckIntervalValues(channel, sessionId, "SELECT val FROM `/Root/table-2` WHERE key = 4;",
                            {static_cast<i64>(diff.MicroSeconds())});

        CheckIntervalValues(channel, sessionId, "SELECT val FROM `/Root/table-2` WHERE key = 5;",
                            {- static_cast<i64>(diff.MicroSeconds())});

        CheckTimestampValues(channel, sessionId, R"___(
                             $v1 = (SELECT val FROM `/Root/table-1` WHERE key = 1);
                             $v2 = (SELECT val FROM `/Root/table-2` WHERE key = 1);
                             SELECT ($v1 + $v2);)___",
                             {TInstant::ParseIso8601("2000-01-01T01:00:00Z")});
    }
}

#endif

} // namespace NKikimr
