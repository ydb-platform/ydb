#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/rows.h>
#include <ydb/public/sdk/cpp/src/client/row_ranges/rows_stream_drain.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include "row_ranges_fixtures.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/yexception.h>

#include <google/protobuf/text_format.h>

#include <concepts>
#include <deque>
#include <iterator>
#include <list>
#include <ranges>
#include <type_traits>
#include <unordered_map>
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

Y_UNIT_TEST_SUITE(TRowRangeTest) {
    Y_UNIT_TEST(EmptyDataQueryResultYieldsNoRows) {
        auto data = MakeDataQueryResult({});
        TRowRange range(std::move(data));
        size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            (void)it;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 0u);
    }

    Y_UNIT_TEST(SingleResultSetCtorIteratesRows) {
        auto rs = MakeSingleInt32ResultSet(7);
        TRowRange range(std::move(rs));
        int sum = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            sum += static_cast<int>(it->ColumnParser("v").GetInt32());
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 7);
    }

    Y_UNIT_TEST(MultiSetDataQueryResultThrows) {
        std::vector<TResultSet> sets;
        sets.push_back(MakeSingleInt32ResultSet(1));
        sets.push_back(MakeSingleInt32ResultSet(2));
        sets.push_back(MakeSingleInt32ResultSet(3));
        auto data = MakeDataQueryResult(std::move(sets));
        UNIT_ASSERT_EXCEPTION(
            TRowRange(std::move(data)),
            yexception);
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
            TRowRange(std::move(data)),
            TYdbRangeErrorException);
    }
}

Y_UNIT_TEST_SUITE(TRowRangeStreamDrainTest) {
    Y_UNIT_TEST(DrainExecuteQueryIteratorSkipsStatsOnlyParts) {
        TMockStreamIterator<NQuery::TExecuteQueryPart> it;
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), std::nullopt, std::nullopt));
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), MakeSingleInt32ResultSet(11), 0, std::nullopt, std::nullopt));
        it.Push(NQuery::TExecuteQueryPart(EosStatus(), std::nullopt, std::nullopt));

        auto rs = NYdb::NRowRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(rs.has_value());
        UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*rs), 11);

        auto after = NYdb::NRowRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(!after.has_value());
    }

    Y_UNIT_TEST(DrainScanQueryIteratorSkipsStatsOnlyParts) {
        TMockStreamIterator<NTable::TScanQueryPart> it;
        it.Push(NTable::TScanQueryPart(OkStatus()));
        it.Push(NTable::TScanQueryPart(OkStatus(), MakeSingleInt32ResultSet(22), std::nullopt, std::nullopt, std::nullopt));
        it.Push(NTable::TScanQueryPart(EosStatus()));

        auto rs = NYdb::NRowRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(rs.has_value());
        UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*rs), 22);

        UNIT_ASSERT(!NYdb::NRowRangesDetail::DrainStreamIterator(it).has_value());
    }

    Y_UNIT_TEST(DrainReadTableIteratorReturnsPartThenEos) {
        TMockStreamIterator<NTable::TReadTableResultPart> it;
        it.Push(NTable::TReadTableResultPart(MakeSingleInt32ResultSet(33), OkStatus()));
        it.Push(NTable::TReadTableResultPart(MakeEmptyInt32SchemaResultSet(), EosStatus()));

        auto rs = NYdb::NRowRangesDetail::DrainStreamIterator(it);
        UNIT_ASSERT(rs.has_value());
        UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*rs), 33);

        UNIT_ASSERT(!NYdb::NRowRangesDetail::DrainStreamIterator(it).has_value());
    }

    Y_UNIT_TEST(ExecuteQueryStreamSameIndexPartsOk) {
        TMockStreamIterator<NQuery::TExecuteQueryPart> it;
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), MakeSingleInt32ResultSet(1), 0, std::nullopt, std::nullopt));
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), MakeSingleInt32ResultSet(2), 0, std::nullopt, std::nullopt));
        it.Push(NQuery::TExecuteQueryPart(EosStatus(), std::nullopt, std::nullopt));

        int32_t sum = 0;
        while (auto rs = NYdb::NRowRangesDetail::DrainStreamIterator(it)) {
            sum += SumColumnV(*rs);
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 3);
    }

    Y_UNIT_TEST(ExecuteQueryStreamNonZeroIndexThrows) {
        TMockStreamIterator<NQuery::TExecuteQueryPart> it;
        it.Push(NQuery::TExecuteQueryPart(OkStatus(), MakeSingleInt32ResultSet(1), 1, std::nullopt, std::nullopt));

        UNIT_ASSERT_EXCEPTION(
            NYdb::NRowRangesDetail::DrainStreamIterator(it),
            yexception);
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

TResultSet MakeSingleColumnResultSet(const std::string& columnName, const TValue& value) {
    Ydb::ResultSet proto;
    auto* column = proto.add_columns();
    column->set_name(columnName);
    column->mutable_type()->CopyFrom(value.GetType().GetProto());
    auto* row = proto.add_rows();
    row->add_items()->CopyFrom(value.GetProto());
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleIntervalResultSet(int64_t micros) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INTERVAL }\n"
        "}\n"
        "rows {\n"
        "  items { int64_value: " +
        std::to_string(micros) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleJsonResultSet(const std::string& json) {
    return MakeSingleColumnResultSet("v", TValueBuilder().Json(json).Build());
}

TResultSet MakeSingleBytesResultSet(const std::string& bytesLiteral) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: STRING }\n"
        "}\n"
        "rows {\n"
        "  items { bytes_value: \"" + bytesLiteral + "\" }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleTimestamp64ResultSet(int64_t micros) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: TIMESTAMP64 }\n"
        "}\n"
        "rows {\n"
        "  items { int64_value: " +
        std::to_string(micros) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeSingleInterval64ResultSet(int64_t micros) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INTERVAL64 }\n"
        "}\n"
        "rows {\n"
        "  items { int64_value: " +
        std::to_string(micros) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeListInt32ResultSet(const std::vector<int32_t>& values) {
    std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { list_type { item { type_id: INT32 } } }\n"
        "}\n"
        "rows {\n"
        "  items {\n";
    for (int32_t v : values) {
        text += "    items { int32_value: " + std::to_string(v) + " }\n";
    }
    text += "  }\n}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeTupleColumnResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"t\"\n"
        "  type {\n"
        "    tuple_type {\n"
        "      elements { optional_type { item { type_id: UTF8 } } }\n"
        "      elements { type_id: INT8 }\n"
        "    }\n"
        "  }\n"
        "}\n"
        "rows {\n"
        "  items {\n"
        "    items { text_value: \"hello\" }\n"
        "    items { int32_value: -5 }\n"
        "  }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeStructColumnResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"s\"\n"
        "  type {\n"
        "    struct_type {\n"
        "      members { name: \"id\" type { type_id: INT32 } }\n"
        "      members { name: \"name\" type { type_id: UTF8 } }\n"
        "    }\n"
        "  }\n"
        "}\n"
        "rows {\n"
        "  items {\n"
        "    items { int32_value: 42 }\n"
        "    items { text_value: \"struct\" }\n"
        "  }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeDictColumnResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"d\"\n"
        "  type {\n"
        "    dict_type {\n"
        "      key { type_id: UINT32 }\n"
        "      payload { type_id: UTF8 }\n"
        "    }\n"
        "  }\n"
        "}\n"
        "rows {\n"
        "  items {\n"
        "    pairs {\n"
        "      key { uint32_value: 1 }\n"
        "      payload { text_value: \"one\" }\n"
        "    }\n"
        "    pairs {\n"
        "      key { uint32_value: 2 }\n"
        "      payload { text_value: \"two\" }\n"
        "    }\n"
        "  }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeOptionalListInt32ResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { optional_type { item { list_type { item { type_id: INT32 } } } } }\n"
        "}\n"
        "rows {\n"
        "  items { null_flag_value: NULL_VALUE }\n"
        "}\n"
        "rows {\n"
        "  items {\n"
        "    items { int32_value: 5 }\n"
        "    items { int32_value: 6 }\n"
        "  }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeTaggedColumnResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"t\"\n"
        "  type {\n"
        "    tagged_type {\n"
        "      tag: \"my_tag\"\n"
        "      type { type_id: UTF8 }\n"
        "    }\n"
        "  }\n"
        "}\n"
        "rows {\n"
        "  items { text_value: \"tagged_value\" }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

TResultSet MakeVariantColumnResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type {\n"
        "    variant_type {\n"
        "      tuple_items {\n"
        "        elements { type_id: INT32 }\n"
        "        elements { type_id: UTF8 }\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "}\n"
        "rows {\n"
        "  items {\n"
        "    variant_index: 1\n"
        "    nested_value { text_value: \"variant_utf8\" }\n"
        "  }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

} // namespace

Y_UNIT_TEST_SUITE(TRowColumnsTest) {
    Y_UNIT_TEST(GetSingleInt32) {
        TRowRange range(MakeSingleInt32ResultSet(7));
        int32_t sum = 0;
        for (auto [v] : range.Get<int32_t>({"v"})) {
            sum += v;
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 7);
    }

    Y_UNIT_TEST(GetTwoColumnsTuple) {
        auto rs = MakeIdNameResultSet({{1, "foo"}, {2, "bar"}, {3, "baz"}});
        TRowRange range(std::move(rs));
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
        TRowRange range(std::move(rs));
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
        TRowRange range(std::move(rs));
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
        TRowRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION(
            (range.Get<int32_t, int32_t>({"v"})),
            std::invalid_argument);
    }

    Y_UNIT_TEST(GetTooManyNamesThrows) {
        TRowRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION(
            (range.Get<int32_t>({"v", "extra"})),
            std::invalid_argument);
    }

    Y_UNIT_TEST(GetMultipleResultSetsThrows) {
        std::vector<TResultSet> sets;
        sets.push_back(MakeIdNameResultSet({{1, "a"}}));
        sets.push_back(MakeIdNameResultSet({{2, "b"}, {3, "c"}}));
        sets.push_back(MakeIdNameResultSet({{4, "d"}}));
        auto data = MakeDataQueryResult(std::move(sets));
        UNIT_ASSERT_EXCEPTION(
            TRowRange(std::move(data)),
            yexception);
    }

    Y_UNIT_TEST(GetTInstantOnDateColumn) {
        // YDB Date is days since epoch; pick 2022-01-01 = 18993 days.
        constexpr uint32_t days = 18993;
        TRowRange range(MakeSingleDateResultSet(days));
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
        TRowRange range(MakeSingleDatetimeResultSet(secs));
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
        TRowRange range(MakeSingleTimestampResultSet(micros));
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
        TRowRange range(std::move(rs));
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
        TRowRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION(
            ([&]{
                for (auto [d] : range.Get<TInstant>({"v"})) {
                    (void)d;
                }
            }()),
            std::runtime_error);
    }

    Y_UNIT_TEST(GetInt64OnIntervalColumn) {
        constexpr int64_t micros = 123456789;
        TRowRange range(MakeSingleIntervalResultSet(micros));
        int64_t got = 0;
        for (auto [v] : range.Get<int64_t>({"v"})) {
            got = v;
        }
        UNIT_ASSERT_VALUES_EQUAL(got, micros);
    }

    Y_UNIT_TEST(GetStringOnJsonColumn) {
        TRowRange range(MakeSingleJsonResultSet("[1,2,3]"));
        std::string got;
        for (auto [v] : range.Get<std::string>({"v"})) {
            got = v;
        }
        UNIT_ASSERT_VALUES_EQUAL(got, "[1,2,3]");
    }

    Y_UNIT_TEST(GetStringOnBytesColumn) {
        TRowRange range(MakeSingleBytesResultSet("raw_bytes"));
        std::string got;
        for (auto [v] : range.Get<std::string>({"v"})) {
            got = v;
        }
        UNIT_ASSERT_VALUES_EQUAL(got, "raw_bytes");
    }

    Y_UNIT_TEST(GetDecimalValue) {
        auto cell = TValueBuilder()
            .Decimal(TDecimalValue("42.50", 10, 2))
            .Build();
        TRowRange range(MakeSingleColumnResultSet("d", cell));
        TDecimalValue got("", 0, 0);
        for (auto [v] : range.Get<TDecimalValue>({"d"})) {
            got = v;
        }
        UNIT_ASSERT_VALUES_EQUAL(got.ToString(), "42.5");
    }

    Y_UNIT_TEST(GetPgValue) {
        auto cell = TValueBuilder()
            .Pg(TPgValue(TPgValue::VK_TEXT, "pg_text", TPgType("text")))
            .Build();
        TRowRange range(MakeSingleColumnResultSet("p", cell));
        TPgValue got(TPgValue::VK_NULL, "", TPgType("text"));
        for (auto [v] : range.Get<TPgValue>({"p"})) {
            got = v;
        }
        UNIT_ASSERT(got.IsText());
        UNIT_ASSERT_VALUES_EQUAL(got.Content_, "pg_text");
    }

    Y_UNIT_TEST(GetTimestamp64) {
        constexpr int64_t micros = 1640995200000123LL;
        TRowRange range(MakeSingleTimestamp64ResultSet(micros));
        std::chrono::sys_time<TWideMicroseconds> got{};
        for (auto [v] : range.Get<std::chrono::sys_time<TWideMicroseconds>>({"v"})) {
            got = v;
        }
        UNIT_ASSERT_VALUES_EQUAL(
            got.time_since_epoch().count(),
            micros);
    }

    Y_UNIT_TEST(GetInterval64) {
        constexpr int64_t micros = 9876543210LL;
        TRowRange range(MakeSingleInterval64ResultSet(micros));
        TWideMicroseconds got{};
        for (auto [v] : range.Get<TWideMicroseconds>({"v"})) {
            got = v;
        }
        UNIT_ASSERT_VALUES_EQUAL(got.count(), micros);
    }

    Y_UNIT_TEST(GetVectorInt32) {
        TRowRange range(MakeListInt32ResultSet({1, 2, 3}));
        std::vector<int32_t> got;
        for (auto [v] : range.Get<std::vector<int32_t>>({"v"})) {
            got = std::move(v);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(got[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(got[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(got[2], 3);
    }

    Y_UNIT_TEST(GetListInt32) {
        TRowRange range(MakeListInt32ResultSet({4, 5, 6}));
        std::list<int32_t> got;
        for (auto [v] : range.Get<std::list<int32_t>>({"v"})) {
            got = std::move(v);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(got.front(), 4);
        UNIT_ASSERT_VALUES_EQUAL(got.back(), 6);
    }

    Y_UNIT_TEST(GetDequeInt32) {
        TRowRange range(MakeListInt32ResultSet({7, 8}));
        std::deque<int32_t> got;
        for (auto [v] : range.Get<std::deque<int32_t>>({"v"})) {
            got = std::move(v);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(got[0], 7);
        UNIT_ASSERT_VALUES_EQUAL(got[1], 8);
    }

    Y_UNIT_TEST(GetTupleColumn) {
        TRowRange range(MakeTupleColumnResultSet());
        std::tuple<std::optional<std::string>, int8_t> got;
        for (auto [t] : range.Get<std::tuple<std::optional<std::string>, int8_t>>({"t"})) {
            got = std::move(t);
        }
        UNIT_ASSERT(std::get<0>(got).has_value());
        UNIT_ASSERT_VALUES_EQUAL(*std::get<0>(got), "hello");
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got), static_cast<int8_t>(-5));
    }

    Y_UNIT_TEST(GetStructAsTuple) {
        TRowRange range(MakeStructColumnResultSet());
        std::tuple<int32_t, std::string> got;
        for (auto [s] : range.Get<std::tuple<int32_t, std::string>>({"s"})) {
            got = std::move(s);
        }
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got), 42);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got), "struct");
    }

    Y_UNIT_TEST(GetTupleTooFewElementsThrows) {
        TRowRange range(MakeTupleColumnResultSet());
        UNIT_ASSERT_EXCEPTION(
            ([&]{
                for (auto [t] : range.Get<std::tuple<std::optional<std::string>, int8_t, int32_t>>({"t"})) {
                    (void)t;
                }
            }()),
            std::runtime_error);
    }

    Y_UNIT_TEST(GetTupleTooManyElementsThrows) {
        TRowRange range(MakeTupleColumnResultSet());
        UNIT_ASSERT_EXCEPTION(
            ([&]{
                for (auto [t] : range.Get<std::tuple<std::optional<std::string>>>({"t"})) {
                    (void)t;
                }
            }()),
            std::runtime_error);
    }

    Y_UNIT_TEST(GetStructTooFewElementsThrows) {
        TRowRange range(MakeStructColumnResultSet());
        UNIT_ASSERT_EXCEPTION(
            ([&]{
                for (auto [s] : range.Get<std::tuple<int32_t, std::string, bool>>({"s"})) {
                    (void)s;
                }
            }()),
            std::runtime_error);
    }

    Y_UNIT_TEST(GetStructTooManyElementsThrows) {
        TRowRange range(MakeStructColumnResultSet());
        UNIT_ASSERT_EXCEPTION(
            ([&]{
                for (auto [s] : range.Get<std::tuple<int32_t>>({"s"})) {
                    (void)s;
                }
            }()),
            std::runtime_error);
    }

    Y_UNIT_TEST(GetDictColumn) {
        TRowRange range(MakeDictColumnResultSet());
        std::map<uint32_t, std::string> got;
        for (auto [d] : range.Get<std::map<uint32_t, std::string>>({"d"})) {
            got = std::move(d);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(got.at(1), "one");
        UNIT_ASSERT_VALUES_EQUAL(got.at(2), "two");
    }

    Y_UNIT_TEST(GetUnorderedDictColumn) {
        TRowRange range(MakeDictColumnResultSet());
        std::unordered_map<uint32_t, std::string> got;
        for (auto [d] : range.Get<std::unordered_map<uint32_t, std::string>>({"d"})) {
            got = std::move(d);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(got.at(1), "one");
        UNIT_ASSERT_VALUES_EQUAL(got.at(2), "two");
    }

    Y_UNIT_TEST(GetOptionalVectorInt32) {
        TRowRange range(MakeOptionalListInt32ResultSet());
        std::vector<std::optional<std::vector<int32_t>>> got;
        for (auto [v] : range.Get<std::optional<std::vector<int32_t>>>({"v"})) {
            got.push_back(v);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 2u);
        UNIT_ASSERT(!got[0].has_value());
        UNIT_ASSERT(got[1].has_value());
        UNIT_ASSERT_VALUES_EQUAL(got[1]->size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL((*got[1])[0], 5);
        UNIT_ASSERT_VALUES_EQUAL((*got[1])[1], 6);
    }

    Y_UNIT_TEST(GetTaggedColumn) {
        TRowRange range(MakeTaggedColumnResultSet());
        std::pair<std::string, std::string> got;
        for (auto [t] : range.Get<std::pair<std::string, std::string>>({"t"})) {
            got = std::move(t);
        }
        UNIT_ASSERT_VALUES_EQUAL(got.first, "my_tag");
        UNIT_ASSERT_VALUES_EQUAL(got.second, "tagged_value");
    }

    Y_UNIT_TEST(GetVariantColumn) {
        TRowRange range(MakeVariantColumnResultSet());
        std::variant<int32_t, std::string> got;
        for (auto [v] : range.Get<std::variant<int32_t, std::string>>({"v"})) {
            got = std::move(v);
        }
        UNIT_ASSERT(std::holds_alternative<std::string>(got));
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(got), "variant_utf8");
    }

    Y_UNIT_TEST(GetTValueColumn) {
        auto cell = TValueBuilder().Int32(99).Build();
        TRowRange range(MakeSingleColumnResultSet("v", cell));
        int32_t got = 0;
        for (auto [v] : range.Get<TValue>({"v"})) {
            got = TValueParser(v).GetInt32();
        }
        UNIT_ASSERT_VALUES_EQUAL(got, 99);
    }
}

namespace {

static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::iterator_category,
    std::input_iterator_tag>);
static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::value_type,
    TRowParser>);
static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::difference_type,
    std::ptrdiff_t>);
static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::pointer,
    TRowParser*>);
static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::reference,
    TRowParser&>);
static_assert(!std::copy_constructible<TRowIterator>);
static_assert(!std::assignable_from<TRowIterator&, const TRowIterator&>);
static_assert(std::movable<TRowIterator>);
static_assert(std::sentinel_for<TRowIterEnd, TRowIterator>);

TResultSet MakeThreeInt32RowsResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INT32 }\n"
        "}\n"
        "rows {\n"
        "  items { int32_value: 1 }\n"
        "}\n"
        "rows {\n"
        "  items { int32_value: 2 }\n"
        "}\n"
        "rows {\n"
        "  items { int32_value: 3 }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

} // namespace

Y_UNIT_TEST_SUITE(TRowIteratorTest) {
    Y_UNIT_TEST(StdDistanceMatchesRowCount) {
        TRowRange range(MakeThreeInt32RowsResultSet());
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(range.begin(), range.end()), 3);
    }

    Y_UNIT_TEST(PrefixAndPostfixIncrement) {
        TRowRange range(MakeThreeInt32RowsResultSet());
        auto it = range.begin();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(it->ColumnParser("v").GetInt32()), 1);
        ++it;
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(it->ColumnParser("v").GetInt32()), 2);
        it++;
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(it->ColumnParser("v").GetInt32()), 3);
        ++it;
        UNIT_ASSERT(it == range.end());
    }

    Y_UNIT_TEST(SurvivesRangeMove) {
        TRowRange range(MakeThreeInt32RowsResultSet());
        auto it = range.begin();
        TRowRange moved = std::move(range);
        (void)moved;
        int32_t sum = 0;
        for (; it != TRowIterEnd{}; ++it) {
            sum += static_cast<int32_t>(it->ColumnParser("v").GetInt32());
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 6);
    }

    Y_UNIT_TEST(TypedViewSurvivesRangeMove) {
        TRowRange range(MakeThreeInt32RowsResultSet());
        auto cols = range.Get<int32_t>({"v"});
        TRowRange moved = std::move(range);
        (void)moved;
        int32_t sum = 0;
        for (auto [v] : cols) {
            sum += v;
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 6);
    }
}
