#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/library/yql/providers/generic/connector/libcpp/yt_client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/yson/node/node.h>

#include <arrow/api.h>

#include <util/generic/string.h>
#include <util/string/cast.h>

using namespace NYql;
using namespace NYql::NConnector;

namespace {

    // TYtClient talks to a real YT cluster in production via three narrow data-access
    // seams: GetYtClient (create/cache a client), GetNode (fetch a Cypress node such as
    // "<table>/@schema" or "<table>/@row_count"), and ReadRows (read the rows selected by
    // a rich path). For unit tests we override those seams with in-memory NYT::TNode
    // fixtures so the whole schema-mapping / splitting / predicate / limit / Arrow logic
    // can be exercised offline, without a live (or recipe-launched local) YT cluster.
    class TTestableYtClient: public TYtClient {
    public:
        // A static fixture table with `RowCount` rows:
        //   id: uint64 (required), payload: string (optional).
        explicit TTestableYtClient(const TGenericGatewayConfig& config, ui64 rowCount = 5)
            : TYtClient(config)
            , RowCount_(rowCount)
        {
            for (ui64 i = 0; i < RowCount_; ++i) {
                Rows_.push_back(NYT::TNode()("id", i)("payload", TString("row") + ToString(i)));
            }
        }

    protected:
        NYT::IClientPtr GetYtClient(const NYql::TGenericDataSourceInstance&) override {
            // Never dereferenced: all data access goes through GetNode / ReadRows below.
            return nullptr;
        }

        NYT::TNode GetNode(const NYT::IClientPtr&, const TString& path) override {
            if (path.EndsWith("/@schema")) {
                return NYT::TNode::CreateList()
                    .Add(NYT::TNode()("name", "id")("type", "uint64")("required", true))
                    .Add(NYT::TNode()("name", "payload")("type", "string")("required", false));
            }
            if (path.EndsWith("/@row_count")) {
                return NYT::TNode(static_cast<i64>(RowCount_));
            }
            ythrow yexception() << "unexpected GetNode path: " << path;
        }

        TVector<NYT::TNode> ReadRows(const NYT::IClientPtr&, const NYT::TRichYPath& path) override {
            i64 lower = 0;
            i64 upper = static_cast<i64>(Rows_.size());

            // Honor the row-range carried by the rich path (as ListSplits/ReadSplits set it).
            const auto ranges = path.GetRangesView();
            if (!ranges.empty()) {
                const auto& range = ranges.front();
                if (range.LowerLimit_.RowIndex_.Defined()) {
                    lower = *range.LowerLimit_.RowIndex_;
                }
                if (range.UpperLimit_.RowIndex_.Defined()) {
                    upper = *range.UpperLimit_.RowIndex_;
                }
            }

            lower = Max<i64>(lower, 0);
            upper = Min<i64>(upper, static_cast<i64>(Rows_.size()));

            TVector<NYT::TNode> rows;
            for (i64 i = lower; i < upper; ++i) {
                rows.push_back(Rows_[i]);
            }
            return rows;
        }

    private:
        const ui64 RowCount_;
        TVector<NYT::TNode> Rows_;
    };

    NYql::TGenericDataSourceInstance MakeDataSourceInstance(const TString& cluster) {
        NYql::TGenericDataSourceInstance dsi;
        dsi.set_kind(NYql::EGenericDataSourceKind::YT);
        dsi.mutable_yt_options()->set_cluster(cluster);
        return dsi;
    }

    // Fills TSelect.what with (id: UINT64, payload: Optional<STRING>).
    void FillWhat(NApi::TSelect& select) {
        {
            auto* item = select.mutable_what()->add_items();
            auto* column = item->mutable_column();
            column->set_name("id");
            column->mutable_type()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto* item = select.mutable_what()->add_items();
            auto* column = item->mutable_column();
            column->set_name("payload");
            column->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
    }

    // Reads all splits for a single split covering the given select and returns the
    // resulting arrow record batches.
    TVector<std::shared_ptr<arrow::RecordBatch>> RunReadSplits(
        IClient& client,
        const NApi::TSelect& select,
        NApi::TReadSplitsRequest::EFiltering filtering) {
        NApi::TReadSplitsRequest request;
        auto* split = request.add_splits();
        *split->mutable_select() = select;
        request.set_format(NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
        request.set_filtering(filtering);

        auto iteratorResult = client.ReadSplits(request).GetValueSync();
        Y_ENSURE(iteratorResult.Status.Ok(), iteratorResult.Status.ToDebugString());

        auto drainer = MakeReadSplitsStreamIteratorDrainer(std::move(iteratorResult.Iterator));
        auto buffer = drainer->Run().GetValueSync();
        Y_ENSURE(buffer.Issues.Empty(), buffer.Issues.ToString());

        TVector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (const auto& response : buffer.Responses) {
            batches.push_back(ReadSplitsResponseToArrowRecordBatch(response));
        }
        return batches;
    }

    class TYtClientFixture: public ::testing::Test {
    protected:
        TGenericGatewayConfig Config_;
        static constexpr const char* TablePath_ = "//tmp/lookup_test";
    };

} // namespace

TEST_F(TYtClientFixture, DescribeTableMapsSchema) {
    TTestableYtClient client(Config_);

    NApi::TDescribeTableRequest request;
    *request.mutable_data_source_instance() = MakeDataSourceInstance("test_cluster");
    request.set_table(TablePath_);

    auto result = client.DescribeTable(request).GetValueSync();
    ASSERT_TRUE(result.Status.Ok()) << result.Status.ToDebugString();
    ASSERT_TRUE(result.Response.has_value());

    const auto& schema = result.Response->schema();
    ASSERT_EQ(schema.columns_size(), 2);

    ASSERT_EQ(schema.columns(0).name(), "id");
    ASSERT_EQ(schema.columns(0).type().type_id(), Ydb::Type::UINT64);

    ASSERT_EQ(schema.columns(1).name(), "payload");
    ASSERT_TRUE(schema.columns(1).type().has_optional_type());
    ASSERT_EQ(schema.columns(1).type().optional_type().item().type_id(), Ydb::Type::STRING);
}

TEST_F(TYtClientFixture, ListSplitsSingleSplitForLookup) {
    TTestableYtClient client(Config_);

    NApi::TListSplitsRequest request;
    auto* select = request.add_selects();
    *select->mutable_data_source_instance() = MakeDataSourceInstance("test_cluster");
    select->mutable_from()->set_table(TablePath_);
    request.set_max_split_count(1);

    auto iteratorResult = client.ListSplits(request).GetValueSync();
    ASSERT_TRUE(iteratorResult.Status.Ok()) << iteratorResult.Status.ToDebugString();

    auto drainer = MakeListSplitsStreamIteratorDrainer(std::move(iteratorResult.Iterator));
    auto buffer = drainer->Run().GetValueSync();
    ASSERT_TRUE(buffer.Issues.Empty()) << buffer.Issues.ToString();

    size_t totalSplits = 0;
    for (const auto& response : buffer.Responses) {
        totalSplits += response.splits_size();
        for (const auto& split : response.splits()) {
            // The lookup path (max_split_count == 1) leaves the range description empty.
            ASSERT_TRUE(split.description().empty());
        }
    }
    ASSERT_EQ(totalSplits, 1u);
}

TEST_F(TYtClientFixture, ReadSplitsFullScan) {
    TTestableYtClient client(Config_);

    NApi::TSelect select;
    *select.mutable_data_source_instance() = MakeDataSourceInstance("test_cluster");
    select.mutable_from()->set_table(TablePath_);
    FillWhat(select);

    auto batches = RunReadSplits(client, select, NApi::TReadSplitsRequest::FILTERING_OPTIONAL);
    ASSERT_EQ(batches.size(), 1u);

    auto batch = batches[0];
    ASSERT_EQ(batch->num_columns(), 2);
    ASSERT_EQ(batch->num_rows(), 5);

    auto idArray = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
    for (i64 i = 0; i < idArray->length(); ++i) {
        ASSERT_EQ(idArray->Value(i), static_cast<ui64>(i));
    }
}

TEST_F(TYtClientFixture, ReadSplitsMandatoryFilteringPointLookup) {
    TTestableYtClient client(Config_);

    NApi::TSelect select;
    *select.mutable_data_source_instance() = MakeDataSourceInstance("test_cluster");
    select.mutable_from()->set_table(TablePath_);
    FillWhat(select);

    // WHERE id == 2 (disjunction-of-conjunctions shape produced by the lookup actor).
    auto* disjunction = select.mutable_where()->mutable_filter_typed()->mutable_disjunction();
    auto* operand = disjunction->add_operands();
    auto* comparison = operand->mutable_comparison();
    comparison->set_operation(NApi::TPredicate::TComparison::EQ);
    comparison->mutable_left_value()->set_column("id");
    auto* typedValue = comparison->mutable_right_value()->mutable_typed_value();
    typedValue->mutable_type()->set_type_id(Ydb::Type::UINT64);
    typedValue->mutable_value()->set_uint64_value(2);

    auto batches = RunReadSplits(client, select, NApi::TReadSplitsRequest::FILTERING_MANDATORY);
    ASSERT_EQ(batches.size(), 1u);

    auto batch = batches[0];
    ASSERT_EQ(batch->num_rows(), 1);

    auto idArray = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
    ASSERT_EQ(idArray->Value(0), 2u);
}

TEST_F(TYtClientFixture, ReadSplitsRespectsLimit) {
    TTestableYtClient client(Config_);

    NApi::TSelect select;
    *select.mutable_data_source_instance() = MakeDataSourceInstance("test_cluster");
    select.mutable_from()->set_table(TablePath_);
    FillWhat(select);
    select.mutable_limit()->set_limit(2);

    auto batches = RunReadSplits(client, select, NApi::TReadSplitsRequest::FILTERING_OPTIONAL);
    ASSERT_EQ(batches.size(), 1u);

    auto batch = batches[0];
    ASSERT_EQ(batch->num_rows(), 2);
}
