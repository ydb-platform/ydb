#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/ranges.h>
#include <ydb/public/sdk/cpp/src/client/result_ranges/ranges_stream_drain.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <google/protobuf/text_format.h>

#include <deque>
#include <vector>

using namespace NYdb;
using namespace NYdb::NStatusHelpers;

namespace {

TStatus OkStatus() {
    return TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues{});
}

TStatus EosStatus() {
    return TStatus(EStatus::CLIENT_OUT_OF_RANGE, NYdb::NIssue::TIssues{});
}

TResultSet MakeSingleInt32ResultSet(int32_t value) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INT32 }\n"
        "}\n"
        "rows {\n"
        "  items { int32_value: " +
        std::to_string(value) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeEmptyInt32SchemaResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INT32 }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

int32_t SumColumnV(const TResultSet& rs) {
    int32_t sum = 0;
    TResultSetParser parser(rs);
    while (parser.TryNextRow()) {
        sum += static_cast<int32_t>(parser.ColumnParser("v").GetInt32());
    }
    return sum;
}

template <typename Part>
struct TMockStreamIterator {
    std::deque<NThreading::TFuture<Part>> Queue;

    NThreading::TFuture<Part> ReadNext() {
        UNIT_ASSERT(!Queue.empty());
        NThreading::TFuture<Part> f = std::move(Queue.front());
        Queue.pop_front();
        return f;
    }

    void Push(Part&& part) {
        Queue.push_back(NThreading::MakeFuture(std::move(part)));
    }
};

NTable::TDataQueryResult MakeDataQueryResult(std::vector<TResultSet>&& sets) {
    return NTable::TDataQueryResult(
        TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues{}),
        std::move(sets),
        std::nullopt,
        std::nullopt,
        false,
        std::nullopt);
}

} // namespace

Y_UNIT_TEST_SUITE(TResultSetRangeTest) {
    Y_UNIT_TEST(EmptyDataQueryResultYieldsNoRows) {
        auto data = MakeDataQueryResult({});
        TResultSetRange range(std::move(data));
        size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            (void)it;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 0u);
    }

    Y_UNIT_TEST(SingleResultSetCtorIteratesRows) {
        auto rs = MakeSingleInt32ResultSet(7);
        TResultSetRange range(std::move(rs));
        int sum = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            sum += static_cast<int>(it->ColumnParser("v").GetInt32());
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 7);
    }

    Y_UNIT_TEST(MultiSetDataQueryResultPreservesOrder) {
        std::vector<TResultSet> sets;
        sets.push_back(MakeSingleInt32ResultSet(1));
        sets.push_back(MakeSingleInt32ResultSet(2));
        sets.push_back(MakeSingleInt32ResultSet(3));
        auto data = MakeDataQueryResult(std::move(sets));
        TResultSetRange range(std::move(data));
        int sum = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            sum += static_cast<int>(it->ColumnParser("v").GetInt32());
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 6);
    }

    Y_UNIT_TEST(FailedDataQueryResultCtorThrows) {
        auto data = NTable::TDataQueryResult(
            TStatus(EStatus::UNAVAILABLE, NYdb::NIssue::TIssues{}),
            {},
            std::nullopt,
            std::nullopt,
            false,
            std::nullopt);
        UNIT_ASSERT_EXCEPTION(
            TResultSetRange(std::move(data)),
            TYdbErrorException);
    }
}

Y_UNIT_TEST_SUITE(TResultSetRangeStreamDrainTest) {
    Y_UNIT_TEST(DrainExecuteQueryIteratorSkipsStatsOnlyParts) {
        TMockStreamIterator<NQuery::TExecuteQueryPart> it;
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), std::nullopt, std::nullopt));
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), MakeSingleInt32ResultSet(11), 0, std::nullopt, std::nullopt));
        it.Push(NQuery::TExecuteQueryPart(EosStatus(), std::nullopt, std::nullopt));

        auto rs = NYdb::NResultRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(rs.has_value());
        UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*rs), 11);

        auto after = NYdb::NResultRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(!after.has_value());
    }

    Y_UNIT_TEST(DrainScanQueryIteratorSkipsStatsOnlyParts) {
        TMockStreamIterator<NTable::TScanQueryPart> it;
        it.Push(NTable::TScanQueryPart(OkStatus()));
        it.Push(NTable::TScanQueryPart(OkStatus(), MakeSingleInt32ResultSet(22), std::nullopt, std::nullopt, std::nullopt));
        it.Push(NTable::TScanQueryPart(EosStatus()));

        auto rs = NYdb::NResultRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(rs.has_value());
        UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*rs), 22);

        UNIT_ASSERT(!NYdb::NResultRangesDetail::DrainStreamIterator(it).has_value());
    }

    Y_UNIT_TEST(DrainReadTableIteratorReturnsPartThenEos) {
        TMockStreamIterator<NTable::TReadTableResultPart> it;
        it.Push(NTable::TReadTableResultPart(MakeSingleInt32ResultSet(33), OkStatus()));
        it.Push(NTable::TReadTableResultPart(MakeEmptyInt32SchemaResultSet(), EosStatus()));

        auto rs = NYdb::NResultRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(rs.has_value());
        UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*rs), 33);

        UNIT_ASSERT(!NYdb::NResultRangesDetail::DrainStreamIterator(it).has_value());
    }
}

namespace {

TResultSet MakeOptionalInt32ResultSet(const std::vector<std::optional<int32_t>>& values) {
    std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { optional_type { item { type_id: INT32 } } }\n"
        "}\n";
    for (const auto& v : values) {
        text += "rows {\n";
        if (v) {
            text += "  items { int32_value: " + std::to_string(*v) + " }\n";
        } else {
            text += "  items { null_flag_value: NULL_VALUE }\n";
        }
        text += "}\n";
    }
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeIdNameResultSet(const std::vector<std::pair<int32_t, std::string>>& rows) {
    std::string text =
        "columns {\n"
        "  name: \"id\"\n"
        "  type { type_id: INT32 }\n"
        "}\n"
        "columns {\n"
        "  name: \"name\"\n"
        "  type { type_id: UTF8 }\n"
        "}\n";
    for (const auto& [id, name] : rows) {
        text += "rows {\n";
        text += "  items { int32_value: " + std::to_string(id) + " }\n";
        text += "  items { text_value: \"" + name + "\" }\n";
        text += "}\n";
    }
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleDateResultSet(uint32_t daysSinceEpoch) {
    const std::string text =
        "columns {\n"
        "  name: \"d\"\n"
        "  type { type_id: DATE }\n"
        "}\n"
        "rows {\n"
        "  items { uint32_value: " +
        std::to_string(daysSinceEpoch) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleDatetimeResultSet(uint32_t secondsSinceEpoch) {
    const std::string text =
        "columns {\n"
        "  name: \"d\"\n"
        "  type { type_id: DATETIME }\n"
        "}\n"
        "rows {\n"
        "  items { uint32_value: " +
        std::to_string(secondsSinceEpoch) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleTimestampResultSet(uint64_t microsSinceEpoch) {
    const std::string text =
        "columns {\n"
        "  name: \"d\"\n"
        "  type { type_id: TIMESTAMP }\n"
        "}\n"
        "rows {\n"
        "  items { uint64_value: " +
        std::to_string(microsSinceEpoch) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeOptionalDateResultSet(const std::vector<std::optional<uint32_t>>& values) {
    std::string text =
        "columns {\n"
        "  name: \"d\"\n"
        "  type { optional_type { item { type_id: DATE } } }\n"
        "}\n";
    for (const auto& v : values) {
        text += "rows {\n";
        if (v) {
            text += "  items { uint32_value: " + std::to_string(*v) + " }\n";
        } else {
            text += "  items { null_flag_value: NULL_VALUE }\n";
        }
        text += "}\n";
    }
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

} // namespace

Y_UNIT_TEST_SUITE(TRangeColumnsTest) {
    Y_UNIT_TEST(GetSingleInt32) {
        TResultSetRange range(MakeSingleInt32ResultSet(7));
        int32_t sum = 0;
        for (auto [v] : range.Get<int32_t>({"v"})) {
            sum += v;
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 7);
    }

    Y_UNIT_TEST(GetTwoColumnsTuple) {
        auto rs = MakeIdNameResultSet({{1, "foo"}, {2, "bar"}, {3, "baz"}});
        TResultSetRange range(std::move(rs));
        std::vector<std::tuple<int32_t, std::string>> got;
        for (auto row : range.Get<int32_t, std::string>({"id", "name"})) {
            got.push_back(std::move(row));
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got[0]), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got[0]), "foo");
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got[1]), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got[1]), "bar");
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got[2]), 3);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got[2]), "baz");
    }

    Y_UNIT_TEST(GetOptional) {
        auto rs = MakeOptionalInt32ResultSet({std::nullopt, 42, std::nullopt, 7});
        TResultSetRange range(std::move(rs));
        std::vector<std::optional<int32_t>> got;
        for (auto [v] : range.Get<std::optional<int32_t>>({"v"})) {
            got.push_back(v);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 4u);
        UNIT_ASSERT(!got[0].has_value());
        UNIT_ASSERT_VALUES_EQUAL(*got[1], 42);
        UNIT_ASSERT(!got[2].has_value());
        UNIT_ASSERT_VALUES_EQUAL(*got[3], 7);
    }

    Y_UNIT_TEST(GetAcceptsVectorOfNames) {
        auto rs = MakeIdNameResultSet({{10, "x"}, {20, "y"}});
        TResultSetRange range(std::move(rs));
        std::vector<std::string> names{"id", "name"};
        int32_t idSum = 0;
        std::string concat;
        for (auto [id, n] : range.Get<int32_t, std::string>(names)) {
            idSum += id;
            concat += n;
        }
        UNIT_ASSERT_VALUES_EQUAL(idSum, 30);
        UNIT_ASSERT_VALUES_EQUAL(concat, "xy");
    }

    Y_UNIT_TEST(GetTooFewNamesThrows) {
        TResultSetRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION(
            (range.Get<int32_t, int32_t>({"v"})),
            std::invalid_argument);
    }

    Y_UNIT_TEST(GetTooManyNamesThrows) {
        TResultSetRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION(
            (range.Get<int32_t>({"v", "extra"})),
            std::invalid_argument);
    }

    Y_UNIT_TEST(GetIteratesAcrossMultipleResultSets) {
        std::vector<TResultSet> sets;
        sets.push_back(MakeIdNameResultSet({{1, "a"}}));
        sets.push_back(MakeIdNameResultSet({{2, "b"}, {3, "c"}}));
        sets.push_back(MakeIdNameResultSet({{4, "d"}}));
        auto data = MakeDataQueryResult(std::move(sets));
        TResultSetRange range(std::move(data));
        int32_t idSum = 0;
        std::string concat;
        for (auto [id, n] : range.Get<int32_t, std::string>({"id", "name"})) {
            idSum += id;
            concat += n;
        }
        UNIT_ASSERT_VALUES_EQUAL(idSum, 1 + 2 + 3 + 4);
        UNIT_ASSERT_VALUES_EQUAL(concat, "abcd");
    }

    Y_UNIT_TEST(GetTInstantOnDateColumn) {
        // YDB Date is days since epoch; pick 2022-01-01 = 18993 days.
        constexpr uint32_t days = 18993;
        TResultSetRange range(MakeSingleDateResultSet(days));
        std::optional<TInstant> got;
        for (auto [d] : range.Get<TInstant>({"d"})) {
            got = d;
        }
        UNIT_ASSERT(got.has_value());
        UNIT_ASSERT_VALUES_EQUAL(got->Days(), days);
    }

    Y_UNIT_TEST(GetTInstantOnDatetimeColumn) {
        // YDB Datetime is seconds since epoch; pick 2022-01-01 00:00:00 UTC.
        constexpr uint32_t secs = 1640995200u;
        TResultSetRange range(MakeSingleDatetimeResultSet(secs));
        std::optional<TInstant> got;
        for (auto [d] : range.Get<TInstant>({"d"})) {
            got = d;
        }
        UNIT_ASSERT(got.has_value());
        UNIT_ASSERT_VALUES_EQUAL(got->Seconds(), secs);
    }

    Y_UNIT_TEST(GetTInstantOnTimestampColumn) {
        // YDB Timestamp is microseconds since epoch.
        constexpr uint64_t micros = 1640995200000123ull;
        TResultSetRange range(MakeSingleTimestampResultSet(micros));
        std::optional<TInstant> got;
        for (auto [d] : range.Get<TInstant>({"d"})) {
            got = d;
        }
        UNIT_ASSERT(got.has_value());
        UNIT_ASSERT_VALUES_EQUAL(got->MicroSeconds(), micros);
    }

    Y_UNIT_TEST(GetOptionalTInstantNullAndValue) {
        constexpr uint32_t days = 18993;
        auto rs = MakeOptionalDateResultSet({std::nullopt, days, std::nullopt});
        TResultSetRange range(std::move(rs));
        std::vector<std::optional<TInstant>> got;
        for (auto [d] : range.Get<std::optional<TInstant>>({"d"})) {
            got.push_back(d);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT(!got[0].has_value());
        UNIT_ASSERT(got[1].has_value());
        UNIT_ASSERT_VALUES_EQUAL(got[1]->Days(), days);
        UNIT_ASSERT(!got[2].has_value());
    }

    Y_UNIT_TEST(GetTInstantOnWrongTypeThrows) {
        TResultSetRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION(
            ([&]{
                for (auto [d] : range.Get<TInstant>({"v"})) {
                    (void)d;
                }
            }()),
            std::runtime_error);
    }
}
