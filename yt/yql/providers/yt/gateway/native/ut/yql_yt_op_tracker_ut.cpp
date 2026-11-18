#include <yt/yql/providers/yt/gateway/native/yql_yt_op_tracker.h>

#include <yt/cpp/mapreduce/interface/job_statistics.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node.h>

namespace NYql::NNative {
namespace {

NYT::TNode Sample(TStringBuf state, i64 value, TStringBuf task = "map")
{
    return NYT::TNode()
        ("tags", NYT::TNode()("job_state", state)("job_type", task))
        ("summary", NYT::TNode()("sum", value)("count", 1)("min", value)("max", value));
}

THashMap<TString, i64> GetCounters(const NYT::TJobStatistics& statistics,
    NYT::EOperationType type = NYT::EOperationType::Map, bool final = false, i64 outputCount = 1)
{
    NYT::TOperationAttributes attributes;
    attributes.Type = type;
    // Like the server, return only the first path and put the full count in attributes.
    auto outputTables = NYT::TNode::CreateList().Add("//tmp/output");
    outputTables.Attributes()["count"] = outputCount;
    attributes.BriefSpec = NYT::TNode()("output_table_paths", outputTables);
    return GetProgressDataStatistics(statistics, attributes, final);
}

void AddTask(NYT::TNode& node, TStringBuf task, i64 input, i64 output, TStringBuf state = "completed")
{
    for (TStringBuf metric : {"uncompressed_data_size", "row_count"}) {
        auto& inputNode = node["data"]["input"][metric];
        if (inputNode.IsUndefined()) {
            inputNode = NYT::TNode::CreateList();
        }
        inputNode.Add(Sample(state, input, task));
        auto& outputNode = node["data"]["output"]["0"][metric];
        if (outputNode.IsUndefined()) {
            outputNode = NYT::TNode::CreateList();
        }
        outputNode.Add(Sample(state, output, task));
    }
}

void AssertCounters(const THashMap<TString, i64>& counters, i64 input, i64 output)
{
    UNIT_ASSERT_VALUES_EQUAL(counters.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(counters.at("input_bytes"), input);
    UNIT_ASSERT_VALUES_EQUAL(counters.at("input_rows"), input);
    UNIT_ASSERT_VALUES_EQUAL(counters.at("output_bytes"), output);
    UNIT_ASSERT_VALUES_EQUAL(counters.at("output_rows"), output);
}

} // namespace

Y_UNIT_TEST_SUITE(TOperationProgressDataStatistics) {
    Y_UNIT_TEST(SumsAllOutputsAndRespectsJobState) {
        NYT::TNode node;
        node["data"]["input"]["uncompressed_data_size"] = NYT::TNode::CreateList()
            .Add(Sample("completed", 100)).Add(Sample("running", 200)).Add(Sample("failed", 1000));
        node["data"]["input"]["row_count"] = NYT::TNode::CreateList().Add(Sample("running", 3));
        node["data"]["output"]["0"]["uncompressed_data_size"] = NYT::TNode::CreateList()
            .Add(Sample("completed", 3000000000LL)).Add(Sample("running", 4000000000LL));
        node["data"]["output"]["2"]["uncompressed_data_size"] = NYT::TNode::CreateList()
            .Add(Sample("running", 5000000000LL)).Add(Sample("failed", 999));
        node["data"]["output"]["0"]["row_count"] = NYT::TNode::CreateList().Add(Sample("completed", 10));
        node["data"]["output"]["2"]["row_count"] = NYT::TNode::CreateList().Add(Sample("running", 20));

        const NYT::TJobStatistics statistics(node);
        const auto progress = GetCounters(statistics.JobState({NYT::EJobState::Running, NYT::EJobState::Completed}),
            NYT::EOperationType::Map, false, 3);
        UNIT_ASSERT_VALUES_EQUAL(progress.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("input_bytes"), 300);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("input_rows"), 3);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("output_bytes"), 12000000000LL);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("output_rows"), 30);

        const auto completed = GetCounters(statistics, NYT::EOperationType::Map, true, 3);
        UNIT_ASSERT_VALUES_EQUAL(completed.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(completed.at("input_bytes"), 100);
        UNIT_ASSERT_VALUES_EQUAL(completed.at("output_bytes"), 3000000000LL);
        UNIT_ASSERT_VALUES_EQUAL(completed.at("output_rows"), 10);
    }

    Y_UNIT_TEST(DoesNotReadOutputsBeyondTableCount) {
        NYT::TNode node;
        AddTask(node, "map", 100, 5);
        for (TStringBuf metric : {"uncompressed_data_size", "row_count"}) {
            node["data"]["output"]["1"][metric] = NYT::TNode::CreateList().Add(Sample("completed", 1000));
        }
        AssertCounters(GetCounters(NYT::TJobStatistics(node)), 100, 5);
    }

    Y_UNIT_TEST(MissingOutputCountPreservesInputCounters) {
        NYT::TNode node;
        AddTask(node, "map", 100, 5);
        const NYT::TJobStatistics statistics(node);
        NYT::TOperationAttributes attributes;
        attributes.Type = NYT::EOperationType::Map;
        const THashMap<TString, i64> expected = {{"input_bytes", 100}, {"input_rows", 100}};
        UNIT_ASSERT(GetProgressDataStatistics(statistics, attributes, false) == expected);
        attributes.BriefSpec = NYT::TNode::CreateMap();
        UNIT_ASSERT(GetProgressDataStatistics(statistics, attributes, false) == expected);
        (*attributes.BriefSpec)["output_table_paths"] = NYT::TNode::CreateList().Add("//tmp/output");
        UNIT_ASSERT(GetProgressDataStatistics(statistics, attributes, false) == expected);
    }

    Y_UNIT_TEST(MapReduceCountsLastDirectMapperOutput) {
        NYT::TNode node;
        AddTask(node, "partition_map", 100, 80);
        AddTask(node, "sorted_reduce", 80, 5);
        for (TStringBuf metric : {"uncompressed_data_size", "row_count"}) {
            node["data"]["output"]["1"][metric] = NYT::TNode::CreateList().Add(Sample("completed", 7, "partition_map"));
            node["data"]["output"]["2"][metric] = NYT::TNode::CreateList().Add(Sample("completed", 11, "partition_map"));
        }
        AssertCounters(GetCounters(NYT::TJobStatistics(node), NYT::EOperationType::MapReduce, true, 3), 100, 23);
    }

    Y_UNIT_TEST(DistinguishesMissingAndZero) {
        UNIT_ASSERT(GetCounters(NYT::TJobStatistics()).empty());
        NYT::TNode node;
        node["data"]["output"]["0"]["row_count"] = NYT::TNode::CreateList().Add(Sample("completed", 0));
        const auto progress = GetCounters(NYT::TJobStatistics(node));
        UNIT_ASSERT_VALUES_EQUAL(progress.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("output_rows"), 0);
    }

    Y_UNIT_TEST(IgnoresOtherMetrics) {
        const auto sample = NYT::TNode::CreateList().Add(Sample("completed", 100));
        NYT::TNode node;
        node["data"]["output"]["0"]["compressed_data_size"] = sample;
        node["data"]["output"]["0"]["data_weight"] = sample;
        node["data"]["input"]["data_weight"] = sample;
        node["data"]["output"]["0"]["nested"]["uncompressed_data_size"] = sample;
        node["data"]["output"]["total"]["uncompressed_data_size"] = sample;
        node["custom"]["data"]["output"]["0"]["uncompressed_data_size"] = sample;
        UNIT_ASSERT(GetCounters(NYT::TJobStatistics(node)).empty());
    }

    Y_UNIT_TEST(MapReduceCountsOnlyBoundaryTasks) {
        NYT::TNode node;
        AddTask(node, "partition_map(0)", 100, 80, "running");
        AddTask(node, "partition(1)", 80, 80);
        AddTask(node, "partition(2)", 80, 80);
        AddTask(node, "intermediate_sort", 80, 80);
        AddTask(node, "reduce_combiner", 80, 40);
        AddTask(node, "partition_reduce", 20, 2);
        AddTask(node, "sorted_reduce", 20, 3, "running");
        AddTask(node, "auto_merge", 5, 5);
        AddTask(node, "partition_map(0)", 900, 900, "failed");
        const auto statistics = NYT::TJobStatistics(node).JobState({NYT::EJobState::Running, NYT::EJobState::Completed});
        AssertCounters(GetCounters(statistics, NYT::EOperationType::MapReduce), 100, 5);
    }

    Y_UNIT_TEST(MapReduceCountsDirectMapperOutputs) {
        NYT::TNode node;
        AddTask(node, "partition_map", 100, 80);
        AddTask(node, "sorted_reduce", 80, 5);
        for (TStringBuf metric : {"uncompressed_data_size", "row_count"}) {
            node["data"]["output"]["1"][metric] = NYT::TNode::CreateList()
                .Add(Sample("completed", 7, "partition_map"))
                .Add(Sample("completed", 3, "sorted_reduce"));
        }
        AssertCounters(GetCounters(NYT::TJobStatistics(node), NYT::EOperationType::MapReduce, true, 3), 100, 15);
    }

    Y_UNIT_TEST(MapReduceWithoutMapper) {
        NYT::TNode node;
        AddTask(node, "partition", 100, 80);
        AddTask(node, "partition(1)", 80, 80);
        AddTask(node, "partition_reduce", 80, 5);
        AssertCounters(GetCounters(NYT::TJobStatistics(node), NYT::EOperationType::MapReduce, true), 100, 5);
    }

    Y_UNIT_TEST(SingleStageOperationsExcludeAutoMerge) {
        struct TCase {
            NYT::EOperationType Type;
            TStringBuf Task;
        };
        for (const auto& test : {
            TCase{NYT::EOperationType::Map, "map"},
            TCase{NYT::EOperationType::Map, "ordered_map"},
            TCase{NYT::EOperationType::Reduce, "sorted_reduce"},
            TCase{NYT::EOperationType::Reduce, "join_reduce"},
            TCase{NYT::EOperationType::JoinReduce, "join_reduce"},
            TCase{NYT::EOperationType::Merge, "sorted_merge"},
            TCase{NYT::EOperationType::Merge, "ordered_merge"},
            TCase{NYT::EOperationType::Merge, "unordered_merge"},
        }) {
            NYT::TNode node;
            AddTask(node, test.Task, 100, 5);
            AddTask(node, "auto_merge", 5, 5);
            AddTask(node, "shallow_auto_merge", 5, 5);
            AssertCounters(GetCounters(NYT::TJobStatistics(node), test.Type), 100, 5);
        }
    }

    Y_UNIT_TEST(PartitionedSortCountsOnlyBoundaryTasks) {
        NYT::TNode node;
        AddTask(node, "partition(0)", 100, 100);
        AddTask(node, "partition(1)", 100, 100);
        AddTask(node, "intermediate_sort", 60, 60);
        AddTask(node, "final_sort", 30, 30);
        AddTask(node, "sorted_merge", 60, 60);
        AddTask(node, "unordered_merge", 10, 10);
        AssertCounters(GetCounters(NYT::TJobStatistics(node), NYT::EOperationType::Sort), 100, 100);
    }

    Y_UNIT_TEST(SimpleSortOutputWaitsForFinalPlan) {
        NYT::TNode node;
        AddTask(node, "simple_sort", 100, 90);
        const auto statistics = NYT::TJobStatistics(node);
        const auto progress = GetCounters(statistics, NYT::EOperationType::Sort);
        UNIT_ASSERT_VALUES_EQUAL(progress.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("input_bytes"), 100);
        UNIT_ASSERT_VALUES_EQUAL(progress.at("input_rows"), 100);
        AssertCounters(GetCounters(statistics, NYT::EOperationType::Sort, true), 100, 90);

        AddTask(node, "sorted_merge", 90, 80);
        AssertCounters(GetCounters(NYT::TJobStatistics(node), NYT::EOperationType::Sort), 100, 80);
        AssertCounters(GetCounters(NYT::TJobStatistics(node), NYT::EOperationType::Sort, true), 100, 80);
    }

    Y_UNIT_TEST(UnknownOperationDoesNotSumArbitraryTasks) {
        NYT::TNode node;
        AddTask(node, "map", 100, 5);
        const NYT::TJobStatistics statistics(node);
        UNIT_ASSERT(GetProgressDataStatistics(statistics, NYT::TOperationAttributes{}, false).empty());
        UNIT_ASSERT(GetCounters(statistics, NYT::EOperationType::Vanilla).empty());
    }

}

} // namespace NYql::NNative
