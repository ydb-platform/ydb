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
