#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/rows.h>
#include <ydb/public/sdk/cpp/src/client/row_ranges/rows_stream_drain.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include "row_ranges_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/yexception.h>

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
using namespace NRowRangesTest;

namespace {

TStatus OkStatus() {
    return TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues{});
}

TStatus EosStatus() {
    return TStatus(EStatus::CLIENT_OUT_OF_RANGE, NYdb::NIssue::TIssues{});
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

static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::iterator_category,
    std::input_iterator_tag>);
static_assert(!std::copy_constructible<TRowIterator>);
static_assert(!std::assignable_from<TRowIterator&, const TRowIterator&>);
static_assert(std::movable<TRowIterator>);
static_assert(std::sentinel_for<TRowIterEnd, TRowIterator>);

using ListInt32Columns = decltype(std::declval<TRowRange&>().Get<std::list<int32_t>>({"v"}));
using DequeInt32Columns = decltype(std::declval<TRowRange&>().Get<std::deque<int32_t>>({"v"}));
static_assert(std::same_as<
    typename ListInt32Columns::Iterator::value_type,
    std::tuple<std::list<int32_t>>>);
static_assert(std::same_as<
    typename DequeInt32Columns::Iterator::value_type,
    std::tuple<std::deque<int32_t>>>);

} // namespace

Y_UNIT_TEST_SUITE(TRowRangeTest) {
    Y_UNIT_TEST(EmptyDataQueryResultYieldsNoRows) {
        TRowRange range(MakeDataQueryResult({}));
        size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            (void)it;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 0u);
    }

    Y_UNIT_TEST(SingleResultSetCtorIteratesRows) {
        TRowRange range(MakeSingleInt32ResultSet(7));
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
        UNIT_ASSERT_EXCEPTION(TRowRange(MakeDataQueryResult(std::move(sets))), yexception);
    }

    Y_UNIT_TEST(FailedDataQueryResultCtorThrows) {
        auto data = NTable::TDataQueryResult(
            TStatus(EStatus::UNAVAILABLE, NYdb::NIssue::TIssues{}),
            {},
            std::nullopt,
            std::nullopt,
            false,
            std::nullopt);
        UNIT_ASSERT_EXCEPTION(TRowRange(std::move(data)), TYdbRangeErrorException);
    }
}

Y_UNIT_TEST_SUITE(TRowRangeStreamDrainTest) {
    Y_UNIT_TEST(DrainSkipsNonResultParts) {
        {
            TMockStreamIterator<NQuery::TExecuteQueryPart> it;
            it.Push(NQuery::TExecuteQueryPart(OkStatus(), std::nullopt, std::nullopt));
            it.Push(NQuery::TExecuteQueryPart(OkStatus(), MakeSingleInt32ResultSet(11), 0, std::nullopt, std::nullopt));
            it.Push(NQuery::TExecuteQueryPart(EosStatus(), std::nullopt, std::nullopt));
            UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*NYdb::NRowRangesDetail::DrainStreamIterator(it)), 11);
            UNIT_ASSERT(!NYdb::NRowRangesDetail::DrainStreamIterator(it).has_value());
        }
        {
            TMockStreamIterator<NTable::TScanQueryPart> it;
            it.Push(NTable::TScanQueryPart(OkStatus()));
            it.Push(NTable::TScanQueryPart(OkStatus(), MakeSingleInt32ResultSet(22), std::nullopt, std::nullopt, std::nullopt));
            it.Push(NTable::TScanQueryPart(EosStatus()));
            UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*NYdb::NRowRangesDetail::DrainStreamIterator(it)), 22);
            UNIT_ASSERT(!NYdb::NRowRangesDetail::DrainStreamIterator(it).has_value());
        }
        {
            TMockStreamIterator<NTable::TReadTableResultPart> it;
            it.Push(NTable::TReadTableResultPart(MakeSingleInt32ResultSet(33), OkStatus()));
            it.Push(NTable::TReadTableResultPart(MakeEmptyInt32SchemaResultSet(), EosStatus()));
            UNIT_ASSERT_VALUES_EQUAL(SumColumnV(*NYdb::NRowRangesDetail::DrainStreamIterator(it)), 33);
            UNIT_ASSERT(!NYdb::NRowRangesDetail::DrainStreamIterator(it).has_value());
        }
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
        UNIT_ASSERT_EXCEPTION(NYdb::NRowRangesDetail::DrainStreamIterator(it), yexception);
    }
}

Y_UNIT_TEST_SUITE(TRowColumnsTest) {
    Y_UNIT_TEST(GetSingleInt32) {
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<int32_t>(MakeSingleInt32ResultSet(7), "v"), 7);
    }

    Y_UNIT_TEST(GetTwoColumnsTuple) {
        auto got = ReadAllRows<int32_t, std::string>(
            MakeIdNameResultSet({{1, "foo"}, {2, "bar"}, {3, "baz"}}),
            {"id", "name"});
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got[0]), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got[0]), "foo");
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got[2]), 3);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got[2]), "baz");
    }

    Y_UNIT_TEST(GetOptional) {
        auto got = ReadAll<std::optional<int32_t>>(
            MakeOptionalInt32ResultSet({std::nullopt, 42, std::nullopt, 7}),
            "v");
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 4u);
        UNIT_ASSERT(!got[0].has_value());
        UNIT_ASSERT_VALUES_EQUAL(*got[1], 42);
        UNIT_ASSERT(!got[2].has_value());
        UNIT_ASSERT_VALUES_EQUAL(*got[3], 7);
    }

    Y_UNIT_TEST(GetAcceptsVectorOfNames) {
        TRowRange range(MakeIdNameResultSet({{10, "x"}, {20, "y"}}));
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

    Y_UNIT_TEST(GetColumnNameCountMismatchThrows) {
        TRowRange range(MakeSingleInt32ResultSet(1));
        UNIT_ASSERT_EXCEPTION((range.Get<int32_t, int32_t>({"v"})), std::invalid_argument);
        UNIT_ASSERT_EXCEPTION((range.Get<int32_t>({"v", "extra"})), std::invalid_argument);
    }

    Y_UNIT_TEST(GetMultipleResultSetsThrows) {
        std::vector<TResultSet> sets;
        sets.push_back(MakeIdNameResultSet({{1, "a"}}));
        sets.push_back(MakeIdNameResultSet({{2, "b"}, {3, "c"}}));
        sets.push_back(MakeIdNameResultSet({{4, "d"}}));
        UNIT_ASSERT_EXCEPTION(TRowRange(MakeDataQueryResult(std::move(sets))), yexception);
    }

    Y_UNIT_TEST(GetTInstantDispatchesPrimitive) {
        constexpr uint32_t days = 18993;
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<TInstant>(MakeSingleDateResultSet(days), "d").Days(), days);

        constexpr uint32_t secs = 1640995200u;
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<TInstant>(MakeSingleDatetimeResultSet(secs), "d").Seconds(), secs);

        constexpr uint64_t micros = 1640995200000123ull;
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<TInstant>(MakeSingleTimestampResultSet(micros), "d").MicroSeconds(), micros);
    }

    Y_UNIT_TEST(GetOptionalTInstant) {
        constexpr uint32_t days = 18993;
        auto got = ReadAll<std::optional<TInstant>>(
            MakeOptionalDateResultSet({std::nullopt, days, std::nullopt}),
            "d");
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT(!got[0].has_value());
        UNIT_ASSERT(got[1].has_value());
        UNIT_ASSERT_VALUES_EQUAL(got[1]->Days(), days);
        UNIT_ASSERT(!got[2].has_value());
    }

    Y_UNIT_TEST(GetTInstantWrongTypeThrows) {
        AssertGetThrows<TInstant, std::runtime_error>(MakeSingleInt32ResultSet(1), {"v"});
    }

    Y_UNIT_TEST(GetStringLikePrimitives) {
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<std::string>(MakeSingleJsonResultSet("[1,2,3]"), "v"), "[1,2,3]");
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<std::string>(MakeSingleBytesResultSet("raw_bytes"), "v"), "raw_bytes");
    }

    Y_UNIT_TEST(GetWideTimeScalars) {
        constexpr int64_t intervalMicros = 123456789;
        UNIT_ASSERT_VALUES_EQUAL(ReadSingle<int64_t>(MakeSingleIntervalResultSet(intervalMicros), "v"), intervalMicros);

        constexpr int64_t timestamp64Micros = 1640995200000123LL;
        const auto timestamp64 = ReadSingle<std::chrono::sys_time<TWideMicroseconds>>(
            MakeSingleTimestamp64ResultSet(timestamp64Micros),
            "v");
        UNIT_ASSERT_VALUES_EQUAL(timestamp64.time_since_epoch().count(), timestamp64Micros);

        constexpr int64_t interval64Micros = 9876543210LL;
        UNIT_ASSERT_VALUES_EQUAL(
            ReadSingle<TWideMicroseconds>(MakeSingleInterval64ResultSet(interval64Micros), "v").count(),
            interval64Micros);
    }

    Y_UNIT_TEST(GetDecimalAndPg) {
        auto decimalCell = TValueBuilder().Decimal(TDecimalValue("42.50", 10, 2)).Build();
        UNIT_ASSERT_VALUES_EQUAL(
            ReadSingle<TDecimalValue>(MakeSingleColumnResultSet("d", decimalCell), "d").ToString(),
            "42.5");

        auto pgCell = TValueBuilder()
            .Pg(TPgValue(TPgValue::VK_TEXT, "pg_text", TPgType("text")))
            .Build();
        const auto pg = ReadSingle<TPgValue>(MakeSingleColumnResultSet("p", pgCell), "p");
        UNIT_ASSERT(pg.IsText());
        UNIT_ASSERT_VALUES_EQUAL(pg.Content_, "pg_text");
    }

    Y_UNIT_TEST(GetTValueColumn) {
        auto cell = TValueBuilder().Int32(99).Build();
        const auto value = ReadSingle<TValue>(MakeSingleColumnResultSet("v", cell), "v");
        UNIT_ASSERT_VALUES_EQUAL(TValueParser(value).GetInt32(), 99);
    }

    Y_UNIT_TEST(GetSequenceContainers) {
        const auto got = ReadSingle<std::vector<int32_t>>(MakeListInt32ResultSet({1, 2, 3}), "v");
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(got[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(got[2], 3);
    }

    Y_UNIT_TEST(GetAssociativeContainers) {
        const auto check = [](auto map) {
            UNIT_ASSERT_VALUES_EQUAL(map.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(map.at(1), "one");
            UNIT_ASSERT_VALUES_EQUAL(map.at(2), "two");
        };
        check(ReadSingle<std::map<uint32_t, std::string>>(MakeDictColumnResultSet(), "d"));
        check(ReadSingle<std::unordered_map<uint32_t, std::string>>(MakeDictColumnResultSet(), "d"));
    }

    Y_UNIT_TEST(GetTupleColumn) {
        const auto got = ReadSingle<std::tuple<std::optional<std::string>, int8_t>>(MakeTupleColumnResultSet(), "t");
        UNIT_ASSERT(std::get<0>(got).has_value());
        UNIT_ASSERT_VALUES_EQUAL(*std::get<0>(got), "hello");
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got), static_cast<int8_t>(-5));
    }

    Y_UNIT_TEST(GetStructAsTuple) {
        const auto got = ReadSingle<std::tuple<int32_t, std::string>>(MakeStructColumnResultSet(), "s");
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(got), 42);
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(got), "struct");
    }

    Y_UNIT_TEST(GetTupleArityMismatchThrows) {
        AssertGetThrows<
            std::tuple<std::optional<std::string>, int8_t, int32_t>,
            std::runtime_error>(MakeTupleColumnResultSet(), {"t"});
        AssertGetThrows<std::tuple<std::optional<std::string>>, std::runtime_error>(MakeTupleColumnResultSet(), {"t"});
    }

    Y_UNIT_TEST(GetStructArityMismatchThrows) {
        AssertGetThrows<std::tuple<int32_t, std::string, bool>, std::runtime_error>(MakeStructColumnResultSet(), {"s"});
        AssertGetThrows<std::tuple<int32_t>, std::runtime_error>(MakeStructColumnResultSet(), {"s"});
    }

    Y_UNIT_TEST(GetOptionalVectorInt32) {
        const auto got = ReadAll<std::optional<std::vector<int32_t>>>(MakeOptionalListInt32ResultSet(), "v");
        UNIT_ASSERT_VALUES_EQUAL(got.size(), 2u);
        UNIT_ASSERT(!got[0].has_value());
        UNIT_ASSERT(got[1].has_value());
        UNIT_ASSERT_VALUES_EQUAL(got[1]->size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL((*got[1])[0], 5);
        UNIT_ASSERT_VALUES_EQUAL((*got[1])[1], 6);
    }

    Y_UNIT_TEST(GetTaggedColumn) {
        const auto got = ReadSingle<std::pair<std::string, std::string>>(MakeTaggedColumnResultSet(), "t");
        UNIT_ASSERT_VALUES_EQUAL(got.first, "my_tag");
        UNIT_ASSERT_VALUES_EQUAL(got.second, "tagged_value");
    }

    Y_UNIT_TEST(GetVariantColumn) {
        const auto got = ReadSingle<std::variant<int32_t, std::string>>(MakeVariantColumnResultSet(), "v");
        UNIT_ASSERT(std::holds_alternative<std::string>(got));
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(got), "variant_utf8");
    }
}

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
