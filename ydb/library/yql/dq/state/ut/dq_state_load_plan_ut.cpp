#include "dq_state_load_plan.h"

#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

namespace {

struct TGraphBuilder;
struct TTaskBuilder;

struct TTaskInputBuilder {
    TTaskBuilder* Parent;
    NYql::NDqProto::TTaskInput* In;

    TTaskInputBuilder& Channel() {
        In->AddChannels();
        In->MutableUnionAll();
        return *this;
    }

    TTaskInputBuilder& Source() {
        In->MutableSource()->SetType("Unknown");
        return *this;
    }

    TTaskInputBuilder& TopicSource(const TString& topic, ui64 partitionsCount, ui64 dqPartitionsCount, ui64 eachPartition);

    TTaskBuilder& Build() {
        return *Parent;
    }
};

struct TTaskOutputBuilder {
    TTaskBuilder* Parent;
    NYql::NDqProto::TTaskOutput* Out;

    TTaskOutputBuilder& Channel() {
        Out->AddChannels();
        Out->MutableBroadcast();
        return *this;
    }

    TTaskOutputBuilder& Sink() {
        Out->MutableSink();
        return *this;
    }

    TTaskBuilder& Build() {
        return *Parent;
    }
};

struct TTaskBuilder {
    TGraphBuilder* Parent;
    NYql::NDqProto::TDqTask* Task;

    TTaskInputBuilder Input() {
        return TTaskInputBuilder{this, Task->AddInputs()};
    }

    TTaskOutputBuilder Output() {
        return TTaskOutputBuilder{this, Task->AddOutputs()};
    }

    TGraphBuilder& Build() {
        return *Parent;
    }
};

struct TGraphBuilder {
    google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask> Graph;

    TTaskBuilder Task(ui64 id = 0) {
        auto* task = Graph.Add();
        task->SetId(id ? id : Graph.size());
        return TTaskBuilder{this, task};
    }

    google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask> Build() {
        return std::move(Graph);
    }
};

TTaskInputBuilder& TTaskInputBuilder::TopicSource(const TString& topic, ui64 partitionsCount, ui64 dqPartitionsCount, ui64 eachPartition) {
    auto* src = In->MutableSource();
    src->SetType("PqSource");

    NYql::NPq::NProto::TDqPqTopicSource topicSrcSettings;
    topicSrcSettings.SetDatabase("DB");
    topicSrcSettings.SetDatabaseId("DBID");
    topicSrcSettings.SetTopicPath(topic);
    src->MutableSettings()->PackFrom(topicSrcSettings);

    NYql::NPq::NProto::TDqReadTaskParams readTaskParams;
    auto* part = readTaskParams.MutablePartitioningParams();
    part->SetTopicPartitionsCount(partitionsCount);
    part->SetDqPartitionsCount(dqPartitionsCount);
    part->SetEachTopicPartitionGroupId(eachPartition);
    TString readTaskParamsBytes;
    UNIT_ASSERT(readTaskParams.SerializeToString(&readTaskParamsBytes));
    Yql::DqsProto::TTaskMeta meta;
    (*meta.MutableTaskParams())["pq"] = readTaskParamsBytes;
    Parent->Task->MutableMeta()->PackFrom(meta);
    return *this;
}

ui64 SourcesCount(const NYql::NDqProto::TDqTask& task) {
    ui64 cnt = 0;
    for (const auto& input : task.GetInputs()) {
        if (input.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource) {
            ++cnt;
        }
    }
    return cnt;
}

ui64 SinksCount(const NYql::NDqProto::TDqTask& task) {
    ui64 cnt = 0;
    for (const auto& output : task.GetOutputs()) {
        if (output.GetTypeCase() == NYql::NDqProto::TTaskOutput::kSink) {
            ++cnt;
        }
    }
    return cnt;
}

struct TTestCase : public NUnitTest::TBaseTestCase {
    TGraphBuilder SrcGraph;
    TGraphBuilder DstGraph;
    THashMap<ui64, NDqProto::NDqStateLoadPlan::TTaskPlan> Plan;
    TIssues Issues;

    bool MakePlan(bool force) {
        Plan.clear();
        Issues.Clear();
        const bool result = MakeContinueFromStreamingOffsetsPlan(SrcGraph.Graph, DstGraph.Graph, force, Plan, Issues);
        if (result) {
            ValidatePlan();
        } else {
            UNIT_ASSERT_UNEQUAL(Issues.Size(), 0);
        }
        return result;
    }

    void SwapGraphs() {
        SrcGraph.Graph.Swap(&DstGraph.Graph);
    }

    const NYql::NDqProto::TDqTask& FindSrcTask(ui64 taskId) const {
        for (const auto& task : SrcGraph.Graph) {
            if (task.GetId() == taskId) {
                return task;
            }
        }
        UNIT_ASSERT_C(false, "Task " << taskId << " was not found in src graph");
        // Make compiler happy
        return SrcGraph.Graph.Get(42);
    }

    void ValidatePlan() const {
        UNIT_ASSERT_VALUES_EQUAL(Plan.size(), DstGraph.Graph.size());
        for (const auto& task : DstGraph.Graph) {
            const auto taskPlanIt = Plan.find(task.GetId());
            UNIT_ASSERT_C(taskPlanIt != Plan.end(), "Task " << task.GetId() << " was not found in plan");
            const auto& taskPlan = taskPlanIt->second;
            UNIT_ASSERT_C(taskPlan.GetStateType() != NDqProto::NDqStateLoadPlan::STATE_TYPE_UNSPECIFIED, "Task " << task.GetId() << " plan: " << taskPlan);
            if (taskPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY) {
                UNIT_ASSERT_C(!taskPlan.HasProgram(), "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SourcesSize(), 0, "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SinksSize(), 0, "Task " << task.GetId() << " plan: " << taskPlan);
            } else {
                UNIT_ASSERT_C(taskPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_C(taskPlan.HasProgram(), "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_C(taskPlan.GetProgram().GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SourcesSize(), SourcesCount(task), "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SinksSize(), SinksCount(task), "Task " << task.GetId() << " plan: " << taskPlan);
                for (const auto& sourcePlan : taskPlan.GetSources()) {
                    UNIT_ASSERT_C(sourcePlan.GetStateType() != NDqProto::NDqStateLoadPlan::STATE_TYPE_UNSPECIFIED, "Task " << task.GetId() << " plan: " << taskPlan);
                    UNIT_ASSERT_C(sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY || sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, "Task " << task.GetId() << " plan: " << taskPlan);
                    UNIT_ASSERT_C(sourcePlan.GetInputIndex() < task.InputsSize(), "Task " << task.GetId() << " plan: " << taskPlan);
                    const auto& taskInput = task.GetInputs(sourcePlan.GetInputIndex());
                    UNIT_ASSERT_C(taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource, "Task " << task.GetId() << " plan: " << taskPlan);
                    // State type is foreign => source type is pq
                    UNIT_ASSERT_C(sourcePlan.GetStateType() != NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN || taskInput.GetSource().GetType() == "PqSource", "Task " << task.GetId() << " plan: " << taskPlan << ". Task input: " << taskInput);
                    if (sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
                        UNIT_ASSERT_C(sourcePlan.ForeignTasksSourcesSize() > 0, "Task " << task.GetId() << " plan: " << taskPlan);
                        const TMaybe<NPq::TTopicPartitionsSet> partitionsSet = NPq::GetTopicPartitionsSet(task.GetMeta());
                        UNIT_ASSERT_C(partitionsSet, "Task " << task.GetId() << " plan: " << taskPlan);
                        for (const auto& taskSource : sourcePlan.GetForeignTasksSources()) {
                            const auto& srcTask = FindSrcTask(taskSource.GetTaskId()); // with assertion
                            UNIT_ASSERT_C(taskSource.GetInputIndex() < srcTask.InputsSize(), "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            const auto& srcTaskInput = srcTask.GetInputs(taskSource.GetInputIndex());
                            UNIT_ASSERT_C(srcTaskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource, "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            UNIT_ASSERT_C(srcTaskInput.GetSource().GetType() == "PqSource", "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            const TMaybe<NPq::TTopicPartitionsSet> srcTaskPartitionsSet = NPq::GetTopicPartitionsSet(task.GetMeta());
                            UNIT_ASSERT_C(srcTaskPartitionsSet, "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            UNIT_ASSERT_C(partitionsSet->Intersects(*srcTaskPartitionsSet), "Task " << srcTask.GetId() << " plan: " << taskPlan);
                        }
                    }
                }
                for (const auto& sinkPlan : taskPlan.GetSinks()) {
                    UNIT_ASSERT_C(sinkPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Task " << task.GetId() << " plan: " << taskPlan);
                    UNIT_ASSERT_C(sinkPlan.GetOutputIndex() < task.OutputsSize(), "Task " << task.GetId() << " plan: " << taskPlan);
                    const auto& taskOutput = task.GetOutputs(sinkPlan.GetOutputIndex());
                    UNIT_ASSERT_C(taskOutput.GetTypeCase() == NYql::NDqProto::TTaskOutput::kSink, "Task " << task.GetId() << " plan: " << taskPlan);
                }
            }
        }
    }

    void AssertTaskPlanIsEmpty(ui64 taskId) const {
        const auto taskPlanIt = Plan.find(taskId);
        UNIT_ASSERT_C(taskPlanIt != Plan.end(), "Task " << taskId << " was not found in plan");
        UNIT_ASSERT_C(taskPlanIt->second.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, taskPlanIt->second);
    }

    void AssertTaskPlanSourceHasSourceTask(ui64 taskId, ui64 sourceIndex, ui64 srcTaskId, ui64 srcInputIndex) const {
        const auto taskPlanIt = Plan.find(taskId);
        UNIT_ASSERT_C(taskPlanIt != Plan.end(), "Task " << taskId << " was not found in plan");
        const auto& taskPlan = taskPlanIt->second;
        UNIT_ASSERT_C(taskPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, taskPlanIt->second);
        for (const auto& sourcePlan : taskPlan.GetSources()) {
            if (sourcePlan.GetInputIndex() == sourceIndex) {
                for (const auto& foreignTaskSource : sourcePlan.GetForeignTasksSources()) {
                    if (foreignTaskSource.GetTaskId() == srcTaskId) {
                        UNIT_ASSERT_VALUES_EQUAL_C(foreignTaskSource.GetInputIndex(), srcInputIndex, foreignTaskSource);
                        return;
                    }
                }
                UNIT_ASSERT_C(false, "Source task " << srcTaskId << " was not found in source plan for index " << sourceIndex);
            }
        }
        UNIT_ASSERT_C(false, "Source plan for index " << sourceIndex << " was not found");
    }

    TString IssuesStr() const {
        return Issues.ToString();
    }
};

} // namespace

Y_UNIT_TEST_SUITE_F(TContinueFromStreamingOffsetsPlanTest, TTestCase) {
    Y_UNIT_TEST(Empty) {
        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT(MakePlan(true));
    }

    Y_UNIT_TEST(OneToOneMapping) {
        SrcGraph
            .Task()
                .Input().Channel().Build()
                .Output().Channel().Build()
                .Build()
            .Task()
                .Input().Channel().Build()
                .Input().TopicSource("t", 3, 3, 0).Build();
        DstGraph
            .Task()
                .Input().TopicSource("t", 3, 3, 0).Build();

        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        AssertTaskPlanSourceHasSourceTask(1, 0, 2, 1);

        SwapGraphs();
        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        AssertTaskPlanIsEmpty(1);
    }

    Y_UNIT_TEST(DifferentPartitioning) {
        SrcGraph
            .Task()
                .Input().Channel().Build()
                .Input().TopicSource("t", 4, 1, 0).Build();
        DstGraph
            .Task()
                .Input().TopicSource("t", 4, 2, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 4, 2, 1).Build();

        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        AssertTaskPlanSourceHasSourceTask(1, 0, 1, 1);
        AssertTaskPlanSourceHasSourceTask(2, 0, 1, 1);

        SwapGraphs();
        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        AssertTaskPlanSourceHasSourceTask(1, 1, 1, 0);
        AssertTaskPlanSourceHasSourceTask(1, 1, 2, 0);
    }

    Y_UNIT_TEST(MultipleTopics) {
        SrcGraph
            .Task()
                .Input().TopicSource("t", 1, 1, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("p", 1, 1, 0).Build()
                .Build();

        DstGraph
            .Task()
                .Input().TopicSource("p", 1, 1, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 1, 1, 0).Build()
                .Build();

        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        AssertTaskPlanSourceHasSourceTask(1, 0, 2, 0);
        AssertTaskPlanSourceHasSourceTask(2, 0, 1, 0);

        SwapGraphs();
        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
    }

    Y_UNIT_TEST(AllTopicsMustBeUsedInNonForceMode) {
        SrcGraph
            .Task()
                .Input().TopicSource("t", 1, 1, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("p", 1, 1, 0).Build()
                .Build();

        DstGraph
            .Task()
                .Input().TopicSource("t", 1, 1, 0).Build()
                .Build();

        UNIT_ASSERT(!MakePlan(false));
        UNIT_ASSERT(MakePlan(true));

        SwapGraphs();
        UNIT_ASSERT(!MakePlan(false));
        UNIT_ASSERT(MakePlan(true));
        AssertTaskPlanIsEmpty(2);
    }

    Y_UNIT_TEST(NotMappedAllPartitions) {
        SrcGraph
            .Task()
                .Input().TopicSource("t", 5, 1, 0).Build()
                .Build();

        DstGraph
            .Task()
                .Input().TopicSource("t", 10, 2, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 10, 2, 1).Build()
                .Build();

        UNIT_ASSERT(!MakePlan(false));
        UNIT_ASSERT_UNEQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_UNEQUAL(Issues.Size(), 0);
        AssertTaskPlanSourceHasSourceTask(1, 0, 1, 0);
        AssertTaskPlanSourceHasSourceTask(2, 0, 1, 0);

        SwapGraphs();
        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
    }

    Y_UNIT_TEST(ReadPartitionInSeveralPlacesIsOk) {
        SrcGraph
            .Task()
                .Input().TopicSource("t", 5, 1, 0).Build()
                .Build();

        DstGraph
            .Task()
                .Input().TopicSource("t", 5, 1, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 5, 2, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 5, 2, 1).Build()
                .Build();

        UNIT_ASSERT(MakePlan(false));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_VALUES_EQUAL(Issues.Size(), 0);

        AssertTaskPlanSourceHasSourceTask(1, 0, 1, 0);
        AssertTaskPlanSourceHasSourceTask(2, 0, 1, 0);
        AssertTaskPlanSourceHasSourceTask(3, 0, 1, 0);

        SwapGraphs();
        UNIT_ASSERT(!MakePlan(false));
    }

    Y_UNIT_TEST(MapSeveralReadingsToOneIsAllowedOnlyInForceMode) {
        SrcGraph
            .Task()
                .Input().TopicSource("t", 5, 1, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 5, 1, 0).Build()
                .Build();

        DstGraph
            .Task()
                .Input().TopicSource("t", 5, 1, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 5, 2, 0).Build()
                .Build()
            .Task()
                .Input().TopicSource("t", 5, 2, 1).Build()
                .Build();

        UNIT_ASSERT(!MakePlan(false));
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT_UNEQUAL(Issues.Size(), 0);
    }
}

} // namespace NYql::NDq
