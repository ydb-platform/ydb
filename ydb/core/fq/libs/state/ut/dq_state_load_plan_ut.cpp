#include "dq_state_load_plan.h"
#include "dq_state_load_plan_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/fq/libs/state/dq_stage_state_recovery_info.h>
#include <ydb/core/kqp/federated_query/actors/pq_checkpoint_provider_integration/pq_checkpoint_provider_integration.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

#include <utility>

namespace NFq {

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
    src->SetType(TString(NYql::PqSource));

    NYql::NPq::NProto::TDqPqTopicSource topicSrcSettings;
    topicSrcSettings.SetDatabase("DB");
    topicSrcSettings.SetDatabaseId("DBID");
    topicSrcSettings.SetTopicPath(topic);
    src->MutableSettings()->PackFrom(topicSrcSettings);

    NYql::NPq::NProto::TDqReadTaskParams readTaskParams;
    auto* part = readTaskParams.AddPartitioningParams();
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
    THashMap<ui64, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan> Plan;
    NYql::TIssues Issues;

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
            UNIT_ASSERT_C(taskPlan.GetStateType() != NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_UNSPECIFIED, "Task " << task.GetId() << " plan: " << taskPlan);
            if (taskPlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY) {
                UNIT_ASSERT_C(!taskPlan.HasProgram(), "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SourcesSize(), 0, "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SinksSize(), 0, "Task " << task.GetId() << " plan: " << taskPlan);
            } else {
                UNIT_ASSERT_C(taskPlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_C(taskPlan.HasProgram(), "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_C(taskPlan.GetProgram().GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SourcesSize(), SourcesCount(task), "Task " << task.GetId() << " plan: " << taskPlan);
                UNIT_ASSERT_VALUES_EQUAL_C(taskPlan.SinksSize(), SinksCount(task), "Task " << task.GetId() << " plan: " << taskPlan);
                for (const auto& sourcePlan : taskPlan.GetSources()) {
                    UNIT_ASSERT_C(sourcePlan.GetStateType() != NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_UNSPECIFIED, "Task " << task.GetId() << " plan: " << taskPlan);
                    UNIT_ASSERT_C(sourcePlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY || sourcePlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, "Task " << task.GetId() << " plan: " << taskPlan);
                    UNIT_ASSERT_C(sourcePlan.GetInputIndex() < task.InputsSize(), "Task " << task.GetId() << " plan: " << taskPlan);
                    const auto& taskInput = task.GetInputs(sourcePlan.GetInputIndex());
                    UNIT_ASSERT_C(taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource, "Task " << task.GetId() << " plan: " << taskPlan);
                    // State type is foreign => source type is pq
                    UNIT_ASSERT_C(sourcePlan.GetStateType() != NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN || taskInput.GetSource().GetType() == NYql::PqSource, "Task " << task.GetId() << " plan: " << taskPlan << ". Task input: " << taskInput);
                    if (sourcePlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
                        UNIT_ASSERT_C(sourcePlan.ForeignTasksSourcesSize() > 0, "Task " << task.GetId() << " plan: " << taskPlan);
                        const TMaybe<NYql::NPq::TTopicPartitionsSet> partitionsSet = NYql::NPq::GetTopicPartitionsSet(task.GetMeta());
                        UNIT_ASSERT_C(partitionsSet, "Task " << task.GetId() << " plan: " << taskPlan);
                        for (const auto& taskSource : sourcePlan.GetForeignTasksSources()) {
                            const auto& srcTask = FindSrcTask(taskSource.GetTaskId()); // with assertion
                            UNIT_ASSERT_C(taskSource.GetInputIndex() < srcTask.InputsSize(), "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            const auto& srcTaskInput = srcTask.GetInputs(taskSource.GetInputIndex());
                            UNIT_ASSERT_C(srcTaskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource, "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            UNIT_ASSERT_C(srcTaskInput.GetSource().GetType() == NYql::PqSource, "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            const TMaybe<NYql::NPq::TTopicPartitionsSet> srcTaskPartitionsSet = NYql::NPq::GetTopicPartitionsSet(task.GetMeta());
                            UNIT_ASSERT_C(srcTaskPartitionsSet, "Task " << srcTask.GetId() << " plan: " << taskPlan);
                            UNIT_ASSERT_C(partitionsSet->Intersects(*srcTaskPartitionsSet), "Task " << srcTask.GetId() << " plan: " << taskPlan);
                        }
                    }
                }
                for (const auto& sinkPlan : taskPlan.GetSinks()) {
                    UNIT_ASSERT_C(sinkPlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Task " << task.GetId() << " plan: " << taskPlan);
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
        UNIT_ASSERT_C(taskPlanIt->second.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, taskPlanIt->second);
    }

    void AssertTaskPlanSourceHasSourceTask(ui64 taskId, ui64 sourceIndex, ui64 srcTaskId, ui64 srcInputIndex) const {
        const auto taskPlanIt = Plan.find(taskId);
        UNIT_ASSERT_C(taskPlanIt != Plan.end(), "Task " << taskId << " was not found in plan");
        const auto& taskPlan = taskPlanIt->second;
        UNIT_ASSERT_C(taskPlan.GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, taskPlanIt->second);
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

namespace {

// These callables exercise program inspection; execution and save/load of the
// actual hopping graph are covered by TDqMultiHoppingSaveLoadTest.
void SetReplayProgram(NYql::NDqProto::TDqTask& task, ui64 hopUs = 0, ui64 windowUs = 0, TStringBuf otherOperator = {}, bool streamOperator = false, TMaybe<bool> checkMinWindowStart = true, bool watermarkGenerator = true, ui32 earlyPolicy = 0, ui32 latePolicy = 0) {
    using namespace NKikimr::NMiniKQL;
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    const auto dataType = TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, env);
    const auto literal = [&](ui64 value) {
        return TRuntimeNode(TDataLiteral::Create(NYql::NUdf::TUnboxedValuePod(value), dataType, env), true);
    };
    auto root = literal(0);
    if (watermarkGenerator && task.InputsSize() && task.GetInputs(0).HasSource()) {
        TCallableBuilder generator(env, "DqWatermarkGenerator", dataType);
        root = TRuntimeNode(generator.Build(), false);
    }
    if (hopUs) {
        TCallableBuilder hopping(env, "MultiHoppingCore", dataType);
        for (ui32 i = 0; i <= (checkMinWindowStart ? 25U : 20U); ++i) {
            if (i == 0) {
                hopping.Add(root);
            } else if (i == 20 || i == 25) {
                const auto boolType = TDataType::Create(NYql::NUdf::TDataType<bool>::Id, env);
                hopping.Add(TRuntimeNode(TDataLiteral::Create(NYql::NUdf::TUnboxedValuePod(i == 20 || *checkMinWindowStart), boolType, env), true));
            } else if (i == 16 || i == 17) {
                const auto intervalType = TDataType::Create(NYql::NUdf::TDataType<NYql::NUdf::TInterval>::Id, env);
                hopping.Add(TRuntimeNode(TDataLiteral::Create(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i == 16 ? hopUs : windowUs)), intervalType, env), true));
            } else if (i == 23 || i == 24) {
                const auto policyType = TDataType::Create(NYql::NUdf::TDataType<ui32>::Id, env);
                hopping.Add(TRuntimeNode(TDataLiteral::Create(NYql::NUdf::TUnboxedValuePod(i == 23 ? earlyPolicy : latePolicy), policyType, env), true));
            } else {
                hopping.Add(literal(0));
            }
        }
        root = TRuntimeNode(hopping.Build(), false);
    }
    if (otherOperator) {
        if (streamOperator) {
            TCallableBuilder flow(env, "ToFlow", TFlowType::Create(dataType, env));
            flow.Add(root);
            root = TRuntimeNode(flow.Build(), false);
        }
        TCallableBuilder other(env, otherOperator, dataType);
        other.Add(root);
        root = TRuntimeNode(other.Build(), false);
    }
    task.SetStageId(task.GetId());
    task.MutableProgram()->SetRuntimeVersion(NYql::NDqProto::RUNTIME_VERSION_YQL_1_0);
    task.MutableProgram()->SetRaw(SerializeRuntimeNode(root, env));
}

enum class EOptionalInterval {
    Literal,
    Empty,
    Computed,
};

void MakeHoppingIntervalOptional(NYql::NDqProto::TDqTask& task, ui32 index, EOptionalInterval kind = EOptionalInterval::Literal) {
    using namespace NKikimr::NMiniKQL;
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    const auto root = DeserializeRuntimeNode(task.GetProgram().GetRaw(), env);
    const auto* callable = AS_CALLABLE("MultiHoppingCore", root);
    TCallableBuilder hopping(env, "MultiHoppingCore", root.GetStaticType());
    for (ui32 i = 0; i < callable->GetInputsCount(); ++i) {
        auto input = callable->GetInput(i);
        if (i == index) {
            if (kind == EOptionalInterval::Computed) {
                TCallableBuilder computed(env, "Identity", input.GetStaticType());
                computed.Add(input);
                input = TRuntimeNode(computed.Build(), false);
            }
            const auto optionalType = TOptionalType::Create(input.GetStaticType(), env);
            input = TRuntimeNode(kind == EOptionalInterval::Empty
                ? TOptionalLiteral::Create(optionalType, env)
                : TOptionalLiteral::Create(input, optionalType, env), true);
        }
        hopping.Add(input);
    }
    task.MutableProgram()->SetRaw(SerializeRuntimeNode(TRuntimeNode(hopping.Build(), false), env));
}

constexpr ui64 Second = 1000000;

void SetPqSink(NYql::NDqProto::TTaskOutput& output, bool deduplication = false, bool exactlyOnce = false) {
    auto* sink = output.MutableSink();
    sink->SetType("PqSink");
    NYql::NPq::NProto::TDqPqTopicSink settings;
    settings.SetEnableDeduplication(deduplication);
    if (exactlyOnce) {
        settings.SetDeferredPublicationExtIdPrefix("publication");
    }
    sink->MutableSettings()->PackFrom(settings);
}

struct TReplayTestGraph {
    TGraphBuilder Builder;
    TCheckpointTaskStates States;

    NProto::TGraphParams Params(bool packPrograms = true) const {
        NProto::TGraphParams graph;
        *graph.MutableTasks() = Builder.Graph;
        if (packPrograms) {
            for (auto& task : *graph.MutableTasks()) {
                if (!task.GetProgram().GetRaw().empty()) {
                    (*graph.MutableStageProgram())[task.GetStageId()] = task.GetProgram().GetRaw();
                    task.MutableProgram()->ClearRaw();
                }
            }
        }
        return graph;
    }

    void Source(ui64 id = 1, ui64 partition = 0, ui64 partitions = 1, bool watermarkGenerator = true, ui64 dqPartitions = 0) {
        auto builder = Builder.Task(id);
        builder.Input().TopicSource("topic", partitions, dqPartitions ? dqPartitions : partitions, partition);
        SetReplayProgram(*builder.Task, 0, 0, {}, false, true, watermarkGenerator);
        auto& state = States[id];
        state.MiniKqlProgram.ConstructInPlace().Data.Version = 2;
        NYql::NPq::NProto::TDqPqTopicSourceState source;
        source.AddTopics()->SetTopicPath("topic");
        source.SetStartingMessageTimestampMs(1000);
        (*builder.Task->MutableSecureParams())[""] = NYql::TStructuredTokenBuilder().SetNoAuth().ToJson();
        auto& saved = *source.AddPartitions();
        saved.SetPartition(partition);
        saved.SetOffset(100);
        state.Sources.emplace_back().Data.emplace_back(source.SerializeAsString(), 1);
    }

    void FederatedSource(ui64 id = 1, ui64 partition = 0, ui64 dqPartitions = 1, const TString& topicPath = "topic") {
        Source(id, partition, 3, true, dqPartitions);
        auto* settings = Builder.Graph.rbegin()->MutableInputs(0)->MutableSource()->MutableSettings();
        NYql::NPq::NProto::TDqPqTopicSource source;
        UNIT_ASSERT(settings->UnpackTo(&source));
        source.SetEndpoint("federation:2135");
        source.SetDatabase("/federation");
        source.SetTopicPath(topicPath);
        source.SetUseSsl(true);
        source.SetAddBearerToToken(true);
        source.MutableToken()->SetName("federation-token");
        (*Builder.Graph.rbegin()->MutableSecureParams())["federation-token"] = NYql::TStructuredTokenBuilder().SetNoAuth().ToJson();
        NYql::NPq::NProto::TDqPqTopicSourceState state;
        state.AddTopics()->SetTopicPath(topicPath);
        state.SetStartingMessageTimestampMs(1000);
        for (const TString& name : {TString("east"), TString("west")}) {
            auto& cluster = *source.AddFederatedClusters();
            cluster.SetName(name);
            cluster.SetEndpoint(name + ":2135");
            cluster.SetDatabase("/" + name);
            cluster.SetPartitionsCount(name == "east" ? 2 : 3);
            for (ui64 p = partition; p < cluster.GetPartitionsCount(); p += dqPartitions) {
                auto& saved = *state.AddPartitions();
                saved.SetCluster(name);
                saved.SetPartition(p);
                saved.SetOffset(100);
            }
        }
        settings->PackFrom(source);
        States[id].Sources.front().Data.front().Blob = state.SerializeAsString();
    }

    void FiniteSource(ui64 id, const TString& sourceType) {
        auto builder = Builder.Task(id);
        builder.Input().Source().In->MutableSource()->SetType(sourceType);
        SetReplayProgram(*builder.Task, 0, 0, {}, false, true, false);
        // Finite tasks do not save checkpoints.
    }

    void Hop(ui64 id, ui64 parent, ui64 step, ui64 window, ui64 checkpointTime, bool sink = true) {
        using namespace NKikimr::NMiniKQL;
        auto builder = Builder.Task(id);
        builder.Input().Channel().In->MutableChannels(0)->SetSrcTaskId(parent);
        if (sink) {
            builder.Output().Sink().Out->MutableSink()->SetType("SolomonSink");
        }
        SetReplayProgram(*builder.Task, step, window);
        UNIT_ASSERT_VALUES_EQUAL(checkpointTime % step, 0);
        auto& program = States[id].MiniKqlProgram.ConstructInPlace();
        program.Data.Version = 2;
        TNodeStateHelper::AddNodeState(program.Data.Blob, THoppingRecoveryState::MakeRecoveryState(checkpointTime / step));
    }
};

ui64 ReplayWindowStartIndex(const TStateLoadPlan& plan, ui64 taskId) {
    using namespace NKikimr::NMiniKQL;
    using namespace NYql::NDqProto::NDqStateLoadPlan;
    UNIT_ASSERT(plan.at(taskId).GetStateType() == STATE_TYPE_FOREIGN);
    UNIT_ASSERT(plan.at(taskId).GetProgram().GetStateType() == STATE_TYPE_FOREIGN);
    UNIT_ASSERT(plan.at(taskId).GetProgram().HasState());
    TStringBuf state(plan.at(taskId).GetProgram().GetState());
    const auto size = ReadUi64(state);
    UNIT_ASSERT_VALUES_EQUAL(size, state.size());
    return THoppingRecoveryState::Read(state).GetMinWindowStartIndex();
}

ui64 ReplayReadTime(const TStateLoadPlan& plan, ui64 taskId) {
    using namespace NYql::NDqProto::NDqStateLoadPlan;
    const auto& sourcePlan = plan.at(taskId).GetSources(0);
    UNIT_ASSERT(plan.at(taskId).GetStateType() == STATE_TYPE_FOREIGN);
    UNIT_ASSERT(sourcePlan.GetStateType() == STATE_TYPE_FOREIGN);
    UNIT_ASSERT(sourcePlan.HasState());
    UNIT_ASSERT(sourcePlan.GetForeignTasksSources().empty());
    UNIT_ASSERT_VALUES_EQUAL(sourcePlan.GetStateVersion(), 1);
    NYql::NPq::NProto::TDqPqTopicSourceState source;
    UNIT_ASSERT(source.ParseFromString(sourcePlan.GetState()));
    return source.GetStartingMessageTimestampMs() * 1000;
}

void CheckFederatedReplayPartitions(const TStateLoadPlan& plan) {
    NYql::NPq::NProto::TDqPqTopicSourceState state;
    UNIT_ASSERT(state.ParseFromString(plan.at(1).GetSources(0).GetState()));
    // Timestamp recovery leaves offsets unset; the provider uses task partitioning
    // and the reader seeks from the prepared consumer position.
    UNIT_ASSERT(state.GetPartitions().empty());
}

} // namespace

Y_UNIT_TEST_SUITE(THistoryReplayPlan) {
    Y_UNIT_TEST(RejectsAdjustWatermarkPolicy) {
        for (ui32 index : {23, 24}) {
            for (bool optional : {false, true}) {
                TReplayTestGraph graph;
                graph.Source();
                graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
                auto& task = *graph.Builder.Graph.Mutable(1);
                SetReplayProgram(task, 10 * Second, 30 * Second, {}, false, true, true, index == 23, index == 24);
                if (optional) {
                    MakeHoppingIntervalOptional(task, index);
                }
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "adjust hopping watermark policy");
                UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, false, plan, issues));
            }
        }
    }

    Y_UNIT_TEST(UninitializedHoppingRejectsTrailingCheckpointState) {
        using namespace NKikimr::NMiniKQL;
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        auto& blob = graph.States[2].MiniKqlProgram->Data.Blob;
        blob.clear();
        WriteUi64(blob, Max<ui64>());
        WriteUi64(blob, Max<ui64>());
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "additional operator state");
    }

    Y_UNIT_TEST(SharedHoppingRequiresProgramWatermarkGenerator) {
        TReplayTestGraph graph;
        graph.Source(1, 0, 1, /* watermarkGenerator */ false);
        graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        auto* settings = graph.Builder.Graph.Mutable(0)->MutableInputs(0)->MutableSource()->MutableSettings();
        NYql::NPq::NProto::TDqPqTopicSource source;
        UNIT_ASSERT(settings->UnpackTo(&source));
        source.SetSharedReading(true);
        source.MutableWatermarks()->SetEnabled(true);
        settings->PackFrom(source);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "requires a watermark generator before each hopping operator");
        for (bool useSourceDisposition : {false, true}) {
            issues.Clear();
            UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, useSourceDisposition, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "requires a watermark generator before each hopping operator");
        }
    }

    Y_UNIT_TEST(SharedStatelessBranchDoesNotRequireProgramWatermarkGenerator) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source(1, 0, 2, /* watermarkGenerator */ false);
        graph.Builder.Graph.Mutable(0)->AddOutputs()->MutableSink()->SetType("SolomonSink");
        auto* settings = graph.Builder.Graph.Mutable(0)->MutableInputs(0)->MutableSource()->MutableSettings();
        NYql::NPq::NProto::TDqPqTopicSource source;
        UNIT_ASSERT(settings->UnpackTo(&source));
        source.SetSharedReading(true);
        source.MutableWatermarks()->SetEnabled(true);
        settings->PackFrom(source);
        // An independent hopping branch still has a MiniKQL watermark generator.
        graph.Source(3, 1, 2);
        graph.Hop(4, 3, 10 * Second, 30 * Second, 600 * Second);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
        UNIT_ASSERT(plan.at(1).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 4), 57);
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        UNIT_ASSERT(plan.at(1).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 600 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 4), 57);
    }

    Y_UNIT_TEST(SharedStageProgramsAndLegacyInlinePrograms) {
        TReplayTestGraph graph;
        graph.Source(1, 0, 2);
        graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        graph.Source(3, 1, 2);
        graph.Builder.Graph.Mutable(2)->SetStageId(1);
        graph.Hop(4, 3, 10 * Second, 30 * Second, 600 * Second);
        graph.Builder.Graph.Mutable(3)->SetStageId(2);
        const auto packed = graph.Params();
        UNIT_ASSERT_VALUES_EQUAL(packed.GetTasks().size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetStageProgram().size(), 2);
        for (const auto& task : packed.GetTasks()) {
            UNIT_ASSERT(task.GetProgram().GetRaw().empty());
        }
        for (bool packOld : {false, true}) {
            for (bool packNext : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(packOld), graph.Params(packNext), graph.States, plan, issues), issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
                UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 4), 57);
                UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
                UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 3), 270 * Second);
            }
        }
    }

    Y_UNIT_TEST(MissingSharedStageProgramFailsReplay) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        auto params = graph.Params();
        params.MutableStageProgram()->erase(1);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(params, 600 * Second, /* useSourceDisposition */ false, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Missing program for stage 1");
    }

    Y_UNIT_TEST(FederatedClustersHaveIndependentPartitionProgress) {
        TReplayTestGraph old;
        old.FederatedSource();
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        TReplayTestGraph next;
        next.FederatedSource();
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);
        auto* settings = next.Builder.Graph.Mutable(0)->MutableInputs(0)->MutableSource()->MutableSettings();
        NYql::NPq::NProto::TDqPqTopicSource source;
        UNIT_ASSERT(settings->UnpackTo(&source));
        source.MutableFederatedClusters()->SwapElements(0, 1);
        settings->PackFrom(source);

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        CheckFederatedReplayPartitions(plan);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
    }

    Y_UNIT_TEST(FederatedPartitionsCanMoveBetweenTasks) {
        TReplayTestGraph old;
        old.FederatedSource(1, 0, 2);
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        old.FederatedSource(3, 1, 2);
        old.Hop(4, 3, 10 * Second, 60 * Second, 500 * Second);
        TReplayTestGraph next;
        next.FederatedSource();
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        CheckFederatedReplayPartitions(plan);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 47);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 170 * Second);
    }

    Y_UNIT_TEST(FederatedReplayRequiresSameClustersAndPartitions) {
        TReplayTestGraph old;
        old.FederatedSource();
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        for (ui32 change = 0; change < 5; ++change) {
            auto next = old.Params();
            auto* settings = next.MutableTasks(0)->MutableInputs(0)->MutableSource()->MutableSettings();
            NYql::NPq::NProto::TDqPqTopicSource source;
            UNIT_ASSERT(settings->UnpackTo(&source));
            auto* cluster = source.MutableFederatedClusters(0);
            switch (change) {
                case 0: cluster->SetName("other"); break;
                case 1: cluster->SetEndpoint("other:2135"); break;
                case 2: cluster->SetDatabase("/other"); break;
                case 3: cluster->SetPartitionsCount(3); break;
                case 4: cluster->SetPartitionsCount(1); break;
            }
            settings->PackFrom(source);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeHistoryReplayPlan(old.Params(), next, old.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), change == 4
                ? "requires the same input partitions" : "Input partition is absent from the previous query");
        }
    }

    Y_UNIT_TEST(FederatedUnstartedPartitionCanRecoverByTime) {
        TReplayTestGraph graph;
        graph.FederatedSource();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        auto& blob = graph.States[1].Sources.front().Data.front().Blob;
        NYql::NPq::NProto::TDqPqTopicSourceState state;
        UNIT_ASSERT(state.ParseFromString(blob));
        state.MutablePartitions()->DeleteSubrange(0, 1); // east:0 is absent, west:0 remains.
        blob = state.SerializeAsString();
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
    }

    Y_UNIT_TEST(FederatedExplicitOutputStartTimePreservesClusterNames) {
        TReplayTestGraph graph;
        graph.FederatedSource();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        // Offline clusters use the task's partition count, as in the reader.
        auto* settings = graph.Builder.Graph.Mutable(0)->MutableInputs(0)->MutableSource()->MutableSettings();
        NYql::NPq::NProto::TDqPqTopicSource source;
        UNIT_ASSERT(settings->UnpackTo(&source));
        source.MutableFederatedClusters(1)->SetPartitionsCount(0);
        settings->PackFrom(source);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        CheckFederatedReplayPartitions(plan);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
    }

    Y_UNIT_TEST(AcceptsOptionalHoppingIntervals) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        for (const ui32 index : {16U, 17U}) {
            MakeHoppingIntervalOptional(*graph.Builder.Graph.Mutable(1), index);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
            UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        }
    }

    Y_UNIT_TEST(RejectsEmptyOrComputedOptionalHoppingIntervals) {
        for (const auto kind : {EOptionalInterval::Empty, EOptionalInterval::Computed}) {
            for (const ui32 index : {16U, 17U}) {
                TReplayTestGraph graph;
                graph.Source();
                graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
                MakeHoppingIntervalOptional(*graph.Builder.Graph.Mutable(1), index, kind);
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues));
                UNIT_ASSERT(plan.empty());
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), kind == EOptionalInterval::Empty
                    ? "Hopping interval cannot be empty" : "requires constant hopping intervals");
            }
        }
    }

    Y_UNIT_TEST(RejectsHoppingWithoutReplayCapability) {
        for (const auto enabled : {TMaybe<bool>{}, TMaybe<bool>{false}}) {
            TReplayTestGraph old;
            old.Source();
            old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
            TReplayTestGraph next;
            next.Source();
            next.Hop(2, 1, 10 * Second, 30 * Second, 0);
            SetReplayProgram(*next.Builder.Graph.Mutable(1), 10 * Second, 30 * Second, {}, false, enabled);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(next.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping minimum window start checking is not enabled");
            UNIT_ASSERT(!MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            // Both the previous and replacement query must opt in.
            UNIT_ASSERT(!MakeHistoryReplayPlan(next.Params(), old.Params(), next.States, plan, issues));
            UNIT_ASSERT(plan.empty());
        }
    }

    Y_UNIT_TEST(RejectsLegacyCheckpointsEvenWithReplayCapability) {
        using namespace NKikimr::NMiniKQL;
        for (ui32 version : {1U, 2U}) {
            TReplayTestGraph graph;
            graph.Source();
            graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
            TString state;
            WriteUi32(state, static_cast<ui32>(EMkqlStateType::SIMPLE_BLOB));
            WriteUi32(state, version);
            WriteUi32(state, 0);
            WriteBool(state, false);
            auto& blob = graph.States[2].MiniKqlProgram->Data.Blob;
            blob.clear();
            TNodeStateHelper::AddNodeState(blob, state);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping checkpoint has no recovery metadata");
        }
    }

    Y_UNIT_TEST(RejectsOverflowingCheckpointWindowStart) {
        using namespace NKikimr::NMiniKQL;
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        auto& blob = graph.States[2].MiniKqlProgram->Data.Blob;
        blob.clear();
        TNodeStateHelper::AddNodeState(blob, THoppingRecoveryState::MakeRecoveryState(Max<ui64>()));
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping recovery time overflow");
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeIsInclusiveAndRoundedUp) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        for (const auto time : {600 * Second, 600 * Second + 1}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), time, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            const auto start = time == 600 * Second ? 570 * Second : 580 * Second;
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), start / (10 * Second));
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), start - 300 * Second);
        }
    }

    Y_UNIT_TEST(InputStartForOutputDoesNotOverflowAtUpperBound) {
        TStageStateRecoveryInfo info;
        auto& hopping = info.Hopping.ConstructInPlace();
        hopping.HopTimeUs = 10;
        hopping.WindowSizeUs = 30;
        UNIT_ASSERT_VALUES_EQUAL(info.InputStartForOutput(Max<ui64>()), Max<ui64>() - 25);
        UNIT_ASSERT_VALUES_EQUAL(info.InputStartForOutput(Max<ui64>() - 5), Max<ui64>() - 35);

        hopping.HopTimeUs = Max<i64>();
        hopping.WindowSizeUs = Max<i64>();
        UNIT_ASSERT_VALUES_EQUAL(info.InputStartForOutput(Max<ui64>()), Max<ui64>() - 1);
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeRejectsTruncatedWindow) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        for (const bool useSourceDisposition : {false, true}) {
            for (const auto time : {ui64{0}, 20 * Second}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), time, useSourceDisposition, plan, issues));
                UNIT_ASSERT(plan.empty());
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping recovery time underflow");
            }
        }
    }

    Y_UNIT_TEST(ExplicitReadFromAllowsWindowStartingAtZero) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        for (const auto time : {20 * Second + 1, 30 * Second}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), time, /* useSourceDisposition */ true, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 0);
            UNIT_ASSERT(plan.at(1).GetSources(0).GetStateType() == STATE_TYPE_EMPTY);
        }
    }

    Y_UNIT_TEST(ReplayRequiresFullEarlyEventAllowance) {
        for (const auto inputStart : {ui64{0}, 300 * Second - 1, 300 * Second, 300 * Second + 1000}) {
            const auto outputStart = inputStart + 30 * Second;
            TReplayTestGraph graph;
            graph.Source();
            graph.Hop(2, 1, 1, 30 * Second, outputStart);
            for (const bool automaticReplay : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                const bool success = automaticReplay
                    ? MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues)
                    : MakeOutputStartTimeReplayPlan(graph.Params(), outputStart, /* useSourceDisposition */ false, plan, issues);
                UNIT_ASSERT_VALUES_EQUAL_C(success, inputStart >= 300 * Second, issues.ToString());
                if (success) {
                    UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), inputStart);
                    UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), inputStart - 300 * Second);
                } else {
                    UNIT_ASSERT(plan.empty());
                    UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "History replay time underflow");
                }
            }
        }
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeRejectsTruncatedUpstreamWindow) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0, false);
        graph.Hop(3, 2, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), 50 * Second, /* useSourceDisposition */ false, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping recovery time underflow");

        issues.Clear();
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 60 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 3), 3);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 0);
        UNIT_ASSERT(plan.at(1).GetSources(0).GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeAlignsNestedHopsWithoutOldGraph) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 7 * Second, 21 * Second, 0, false);
        graph.Hop(3, 2, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 601 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 3), 58);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 80);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 260 * Second);
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeForStatelessQueryRoundsReadTimeDown) {
        for (const bool watermarkGenerator : {false, true}) {
            TReplayTestGraph graph;
            graph.Source(1, 0, 1, watermarkGenerator);
            SetPqSink(*graph.Builder.Graph.Mutable(0)->AddOutputs());
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second + 123, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), (watermarkGenerator ? 300 : 600) * Second);
        }
    }

    Y_UNIT_TEST(ExplicitReadFromPreservesNestedHoppingBoundaries) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 7 * Second, 21 * Second, 0, false);
        graph.Hop(3, 2, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 601 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 3), 58);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 80);
        const auto& source = plan.at(1).GetSources(0);
        UNIT_ASSERT(source.GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT(!source.HasState());
        UNIT_ASSERT(source.GetForeignTasksSources().empty());
    }

    Y_UNIT_TEST(StatelessOutputDoesNotRequireWatermarks) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source(1, 0, 1, /* watermarkGenerator */ false);
        SetPqSink(*graph.Builder.Graph.Mutable(0)->AddOutputs());
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 600 * Second);

        issues.Clear();
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 1);
        UNIT_ASSERT(plan.at(1).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        const auto& source = plan.at(1).GetSources(0);
        UNIT_ASSERT(source.GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT(!source.HasState());
    }

    Y_UNIT_TEST(ExplicitReadFromAllowsStatelessBranchWithoutWatermarksAlongsideHopping) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source(1, 0, 2);
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0, false);
        graph.Source(3, 1, 2, /* watermarkGenerator */ false);
        auto output = graph.Builder.Task(4);
        auto* input = output.Input().Channel().In;
        input->MutableChannels(0)->SetSrcTaskId(2);
        input->AddChannels()->SetSrcTaskId(3);
        output.Output().Sink().Out->MutableSink()->SetType("SolomonSink");
        SetReplayProgram(*output.Task);

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        UNIT_ASSERT(plan.at(4).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        for (const auto sourceId : {1, 3}) {
            const auto& source = plan.at(sourceId).GetSources(0);
            UNIT_ASSERT(source.GetStateType() == STATE_TYPE_EMPTY);
            UNIT_ASSERT(!source.HasState());
        }
    }

    Y_UNIT_TEST(HoppingWithoutWatermarksIsRejectedEvenWithExplicitReadFrom) {
        TReplayTestGraph graph;
        graph.Source(1, 0, 1, /* watermarkGenerator */ false);
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        for (const bool useSourceDisposition : {false, true}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, useSourceDisposition, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "requires a watermark generator before each hopping operator");
        }
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeRejectsOtherCheckpointedOperators) {
        for (const TStringBuf operation : {"MatchRecognizeCore", "TimeOrderRecover", "KqpStreamingAggregation"}) {
            TReplayTestGraph graph;
            graph.Source();
            graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
            SetReplayProgram(*graph.Builder.Graph.Mutable(1), 10 * Second, 30 * Second, operation);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), operation);
        }
    }

    Y_UNIT_TEST(AcceptsOperatorsWithoutCheckpointState) {
        for (const TStringBuf operation : {"Condense1", "Chain1Map", "WideChain1Map", "MapNext", "Enumerate", "Take", "WideSkipWhileInclusive",
                "WideTakeBlocks", "WideTopSortBlocks", "WideSortWithSpilling", "Collect", "Reduce", "WideLastCombiner", "GraceJoin",
                "BlockCombineAll", "BlockCombineHashed", "BlockMergeFinalizeHashed", "BlockMergeManyFinalizeHashed"}) {
            TReplayTestGraph graph;
            graph.Source();
            graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
            SetReplayProgram(*graph.Builder.Graph.Mutable(1), 10 * Second, 30 * Second, operation, true);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
            UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        }
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeAllowsSharedHoppingAndRawOutput) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source();
        auto shared = graph.Builder.Task(2);
        shared.Input().Channel().In->MutableChannels(0)->SetSrcTaskId(1);
        shared.Output().Sink().Out->MutableSink()->SetType("SolomonSink");
        SetReplayProgram(*shared.Task);
        graph.Hop(3, 2, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
        UNIT_ASSERT(plan.at(2).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 3), 57);
    }

    Y_UNIT_TEST(AutomaticReplayToStatelessOutputPreservesEarlyEventAllowance) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        for (const bool watermarkGenerator : {false, true}) {
            TReplayTestGraph next;
            next.Source(1, 0, 1, watermarkGenerator);
            next.Builder.Graph.Mutable(0)->AddOutputs()->MutableSink()->SetType("SolomonSink");
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 300 * Second);
        }
    }

    Y_UNIT_TEST(ChangedWindowReplaysHistoryAndInitializesCompleteWindows) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        TReplayTestGraph next;
        next.Source();
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
    }

    Y_UNIT_TEST(AutomaticReplayUsesInclusiveOutputBoundary) {
        for (const auto time : {600 * Second, 601 * Second}) {
            TReplayTestGraph old;
            old.Source();
            old.Hop(2, 1, Second, 3 * Second, time);
            TReplayTestGraph next;
            next.Source();
            next.Hop(2, 1, 10 * Second, 30 * Second, 0);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
            const auto start = time == 600 * Second ? 570 * Second : 580 * Second;
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), start / (10 * Second));
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), start - 300 * Second);
        }
    }

    Y_UNIT_TEST(AutomaticReplayRejectsTruncatedWindow) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 30 * Second, 20 * Second);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(old.Params(), old.Params(), old.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping recovery time underflow");
    }

    Y_UNIT_TEST(NestedWindowsAlignThroughBothQueryVersions) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second, false);
        old.Hop(3, 2, 20 * Second, 80 * Second, 560 * Second);
        TReplayTestGraph next;
        next.Source();
        next.Hop(2, 1, 7 * Second, 21 * Second, 0, false);
        next.Hop(3, 2, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 3), 50);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 69);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 183 * Second);
    }

    Y_UNIT_TEST(MergedBranchesPropagateBoundariesAndWatermarks) {
        for (bool watermarkGenerator : {true, false}) {
            TReplayTestGraph graph;
            // One input arrives directly; another passes through an extra stage.
            graph.Hop(4, 2, 10 * Second, 30 * Second, 600 * Second);
            auto* input = graph.Builder.Graph.Mutable(0)->MutableInputs(0);
            input->AddChannels()->SetSrcTaskId(3);
            input->AddChannels()->SetSrcTaskId(3);
            auto intermediate = graph.Builder.Task(2);
            intermediate.Input().Channel().In->MutableChannels(0)->SetSrcTaskId(1);
            SetReplayProgram(*intermediate.Task);
            graph.States[2].MiniKqlProgram.ConstructInPlace().Data.Version = 2;
            graph.Source(3, 1, 2, watermarkGenerator);
            graph.Source(1, 0, 2);

            for (bool automaticReplay : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                const bool success = automaticReplay
                    ? MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues)
                    : MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues);
                UNIT_ASSERT_VALUES_EQUAL_C(success, watermarkGenerator, issues.ToString());
                if (success) {
                    UNIT_ASSERT_VALUES_EQUAL(plan.size(), 4);
                    UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
                    UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 3), 270 * Second);
                    UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 4), 57);
                } else {
                    UNIT_ASSERT(plan.empty());
                    UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "requires a watermark generator before each hopping operator");
                }
            }
        }
    }

    Y_UNIT_TEST(MergedBranchesUseEarliestSourceBoundary) {
        TReplayTestGraph old;
        old.Source(1, 0, 2);
        old.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        old.Source(3, 1, 2);
        old.Hop(4, 3, 10 * Second, 30 * Second, 500 * Second);
        TReplayTestGraph next;
        next.Source(1, 0, 2);
        next.Source(3, 1, 2);
        next.Hop(5, 1, 10 * Second, 30 * Second, 0);
        next.Builder.Graph.Mutable(2)->MutableInputs(0)->AddChannels()->SetSrcTaskId(3);
        next.Hop(6, 1, 10 * Second, 60 * Second, 0);

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 5), 47);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 6), 54);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 170 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 3), 170 * Second);
    }

    Y_UNIT_TEST(SharedHoppingBranchUsesAvailableEventTimeFrontier) {
        TReplayTestGraph old;
        old.Source(1, 0, 2);
        old.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        old.Source(3, 1, 2);
        old.Builder.Graph.Mutable(2)->AddOutputs()->MutableSink()->SetType("SolomonSink");
        TReplayTestGraph next;
        next.Source(1, 0, 2);
        next.Hop(2, 1, 10 * Second, 30 * Second, 0, false);
        next.Source(3, 1, 2);
        next.Hop(4, 2, 10 * Second, 30 * Second, 0);
        next.Hop(5, 2, 10 * Second, 30 * Second, 0);
        next.Builder.Graph.Mutable(4)->MutableInputs(0)->AddChannels()->SetSrcTaskId(3);

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 54);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 4), 57);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 5), 57);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 240 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 3), 270 * Second);
    }

    Y_UNIT_TEST(RepeatedStatelessMergesKeepOutputBoundary) {
        TReplayTestGraph graph;
        graph.Source();
        ui64 last = 1;
        for (ui32 level = 0; level < 40; ++level) {
            for (ui64 id = last + 1; id <= last + 3; ++id) {
                auto task = graph.Builder.Task(id);
                auto* input = task.Input().Channel().In;
                input->MutableChannels(0)->SetSrcTaskId(id == last + 3 ? last + 1 : last);
                if (id == last + 3) {
                    input->AddChannels()->SetSrcTaskId(last + 2);
                }
                SetReplayProgram(*task.Task);
            }
            last += 3;
        }
        graph.Builder.Graph.rbegin()->AddOutputs()->MutableSink()->SetType("SolomonSink");

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), last);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 300 * Second);
    }

    Y_UNIT_TEST(PartitionsKeepIndependentRecoveryProgress) {
        TReplayTestGraph old;
        old.Source(1, 0, 2);
        old.Source(3, 1, 2);
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        old.Hop(4, 3, 10 * Second, 60 * Second, 500 * Second);
        TReplayTestGraph next;
        next.Source(1, 0, 2);
        next.Source(3, 1, 2);
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);
        next.Hop(4, 3, 10 * Second, 30 * Second, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 3), 170 * Second);
    }

    Y_UNIT_TEST(FiniteSourcesAreNotMatchedOrRewound) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        TReplayTestGraph next;
        next.Source();
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);
        const auto addFiniteInput = [](TReplayTestGraph& graph, const TString& sourceType) {
            auto& task = *graph.Builder.Graph.Mutable(0);
            task.AddInputs()->MutableSource()->SetType(sourceType);
            task.MutableInputs()->SwapElements(0, 1);
            auto& state = graph.States.at(1);
            state.Sources.front().InputIndex = 1;
            state.Sources.emplace_back().Data.emplace_back("not topic checkpoint state", 42);
        };
        addFiniteInput(old, "KqpReadRangesSource");
        addFiniteInput(next, "S3Source");

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(1).SourcesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetInputIndex(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);

        for (const bool useSourceDisposition : {false, true}) {
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(next.Params(), 600 * Second, useSourceDisposition, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(plan.at(1).SourcesSize(), 1);
            const auto& source = plan.at(1).GetSources(0);
            UNIT_ASSERT_VALUES_EQUAL(source.GetInputIndex(), 1);
            UNIT_ASSERT(source.GetStateType() == (useSourceDisposition ? STATE_TYPE_EMPTY : STATE_TYPE_FOREIGN));
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        }
    }

    Y_UNIT_TEST(FiniteBranchesDoNotRequireCheckpointsOrWatermarks) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        TReplayTestGraph next;
        next.Source();
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);
        const auto addFiniteBranch = [](TReplayTestGraph& graph, ui64 sourceId, const TString& sourceType) {
            graph.FiniteSource(sourceId, sourceType);
            // Finite programs are not subject to streaming checkpoint restrictions.
            SetReplayProgram(*graph.Builder.Graph.Mutable(2), 0, 0, "TimeOrderRecover", false, true, false);
            auto* input = graph.Builder.Graph.Mutable(1)->AddInputs();
            input->MutableMerge();
            auto* channel = input->AddChannels();
            channel->SetSrcTaskId(sourceId);
            channel->SetCheckpointingMode(NYql::NDqProto::CHECKPOINTING_MODE_DISABLED);
            channel->SetWatermarksMode(NYql::NDqProto::WATERMARKS_MODE_DISABLED);
        };
        addFiniteBranch(old, 3, "KqpReadRangesSource");
        addFiniteBranch(next, 4, "S3Source");

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        UNIT_ASSERT(!plan.contains(4));
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);

        for (const bool useSourceDisposition : {false, true}) {
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(next.Params(), 600 * Second, useSourceDisposition, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
            UNIT_ASSERT(!plan.contains(4));
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        }
    }

    Y_UNIT_TEST(SameTopicPathAtAnotherEndpointIsNotTheSameInput) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        TReplayTestGraph next;
        next.Source();
        next.Hop(2, 1, 10 * Second, 30 * Second, 0);
        auto& settings = *next.Builder.Graph.Mutable(0)->MutableInputs(0)->MutableSource()->MutableSettings();
        NYql::NPq::NProto::TDqPqTopicSource source;
        UNIT_ASSERT(settings.UnpackTo(&source));
        source.SetEndpoint("another-cluster:2135");
        settings.PackFrom(source);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Input partition is absent from the previous query");
    }

    Y_UNIT_TEST(StatelessOldQueryPreservesOffsetsAndStartsNewOperatorsEmpty) {
        TReplayTestGraph old;
        old.Source();
        old.Builder.Graph.Mutable(0)->AddOutputs()->MutableSink()->SetType("SolomonSink");
        for (const bool nested : {false, true}) {
            TReplayTestGraph next;
            next.Source();
            next.Hop(2, 1, 10 * Second, 30 * Second, 0, !nested);
            if (nested) {
                next.Hop(3, 2, 20 * Second, 80 * Second, 0);
            }
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeHistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
            UNIT_ASSERT(plan.at(2).GetProgram().GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            if (nested) {
                UNIT_ASSERT(plan.at(3).GetProgram().GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            }
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);
            NYql::NPq::NProto::TDqPqTopicSourceState state;
            UNIT_ASSERT(state.ParseFromString(plan.at(1).GetSources(0).GetState()));
            UNIT_ASSERT_VALUES_EQUAL(state.GetPartitions(0).GetOffset(), 100);
        }
    }

    Y_UNIT_TEST(StatelessOutputTransformsAndEffectsAllowReplay) {
        for (bool effects : {false, true}) {
            TReplayTestGraph graph;
            graph.Source();
            graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
            auto* output = graph.Builder.Graph.Mutable(1)->MutableOutputs(0);
            if (effects) {
                output->MutableEffects();
            } else {
                output->MutableSink()->SetType("StatelessSink");
            }
            output->MutableTransform()->SetType("StatelessTransform");
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
        }
    }

    Y_UNIT_TEST(PqSinkCountersDoNotPreventReplay) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        TReplayTestGraph graph;
        graph.Source(1, 0, 1, /* watermarkGenerator */ false);
        SetPqSink(*graph.Builder.Graph.Mutable(0)->AddOutputs());
        NYql::NPq::NProto::TDqPqTopicSinkState saved;
        saved.SetSourceId("producer");
        saved.SetConfirmedSeqNo(100);
        saved.SetEgressBytes(1000);
        auto& state = graph.States[1].Sinks.emplace_back();
        state.Data.Version = 1;
        state.Data.Blob = saved.SerializeAsString();
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
        UNIT_ASSERT(plan.at(1).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);

        saved.SetDeferredPublicationIntId(42);
        state.Data.Blob = saved.SerializeAsString();
        plan.clear();
        UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "PQ sinks with exactly-once delivery");
    }

    Y_UNIT_TEST(PqSinkReplayRestrictionsApplyOnlyToPreviousCheckpoint) {
        for (bool exactlyOnce : {false, true}) {
            TReplayTestGraph graph;
            graph.Source();
            SetPqSink(*graph.Builder.Graph.Mutable(0)->AddOutputs(), !exactlyOnce, exactlyOnce);
            NYql::NPq::NProto::TDqPqTopicSinkState saved;
            saved.SetSourceId("producer");
            saved.SetConfirmedSeqNo(100);
            if (exactlyOnce) {
                saved.SetDeferredPublicationIntId(42);
            }
            auto& sinkState = graph.States[1].Sinks.emplace_back();
            sinkState.Data.Version = 1;
            sinkState.Data.Blob = saved.SerializeAsString();
            TReplayTestGraph stateless;
            stateless.Source();
            SetPqSink(*stateless.Builder.Graph.Mutable(0)->AddOutputs());
            stateless.States[1].Sinks.emplace_back().Data.Version = 1;
            const TString expected = exactlyOnce ? "PQ sinks with exactly-once delivery" : "PQ sinks with deduplication";
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), stateless.Params(), graph.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), expected);
            issues.Clear();
            UNIT_ASSERT_C(MakeHistoryReplayPlan(stateless.Params(), graph.Params(), stateless.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(plan.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);
            issues.Clear();
            UNIT_ASSERT_C(MakeOutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(plan.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 300 * Second);
        }
    }

    Y_UNIT_TEST(RejectsSinkStateWithoutPublishingPartialPlan) {
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        graph.States[2].Sinks.emplace_back().Data.Blob = "producer state";
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "sink has state");
    }

    Y_UNIT_TEST(RejectsOtherCheckpointedOperators) {
        TReplayTestGraph supported;
        supported.Source();
        supported.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
        for (const TStringBuf operation : {"MatchRecognizeCore", "TimeOrderRecover", "KqpStreamingAggregation"}) {
            TReplayTestGraph unsupported;
            unsupported.Source();
            unsupported.Hop(2, 1, 10 * Second, 30 * Second, 600 * Second);
            SetReplayProgram(*unsupported.Builder.Graph.Mutable(1), 10 * Second, 30 * Second, operation);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!MakeHistoryReplayPlan(unsupported.Params(), supported.Params(), unsupported.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), operation);
            issues.Clear();
            UNIT_ASSERT(!MakeHistoryReplayPlan(supported.Params(), unsupported.Params(), supported.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), operation);
        }
    }
}

namespace {

class THistoryTestClient final : public NYql::ITopicClient {
public:
    bool Expired = false;
    TMaybe<ui64> FirstRetainedWriteTimeUs;
    ui64 Describes = 0;
    ui64 Reads = 0;
    TVector<std::pair<TString, ui64>> DescribedPartitions;
    std::shared_ptr<NYdb::TDriver> Driver;
    NTestUtils::IMockPqGateway::TPtr Gateway;

    NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& topicPath, i64 partitionId,
            const NYdb::NTopic::TDescribePartitionSettings&) override {
        ++Describes;
        DescribedPartitions.emplace_back(topicPath, partitionId);
        Ydb::Topic::DescribePartitionResult description;
        auto* partition = description.mutable_partition();
        partition->set_partition_id(partitionId);
        auto* stats = partition->mutable_partition_stats();
        stats->mutable_partition_offsets()->set_start(FirstRetainedWriteTimeUs ? 10 : (Expired ? 100 : 0));
        stats->mutable_partition_offsets()->set_end(100);
        return NThreading::MakeFuture(NYdb::NTopic::TDescribePartitionResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), std::move(description)));
    }
    NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString&, const NYdb::NTopic::TDescribeTopicSettings&) override {
        ythrow yexception() << "Unexpected DescribeTopic";
    }
    NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString&, const TString&, const NYdb::NTopic::TDescribeConsumerSettings&) override {
        ythrow yexception() << "Unexpected DescribeConsumer";
    }
    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override {
        UNIT_ASSERT(FirstRetainedWriteTimeUs);
        UNIT_ASSERT(settings.WithoutConsumer_);
        ++Reads;
        Driver = std::make_shared<NYdb::TDriver>(NYdb::TDriverConfig{});
        Gateway = NTestUtils::CreateMockPqGateway();
        auto client = Gateway->GetTopicClient(*Driver, {});
        auto session = client->CreateReadSession(settings);
        UNIT_ASSERT_VALUES_EQUAL(settings.Topics_.size(), 1);
        Gateway->WaitReadSession(TString(settings.Topics_.front().Path_))->AddDataReceivedEvent(
            FirstRetainedWriteTimeUs ? 10 : 100, "unused", TInstant::MicroSeconds(FirstRetainedWriteTimeUs.GetOrElse(600 * Second)));
        return session;
    }
    std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const NYdb::NTopic::TWriteSessionSettings&) override {
        ythrow yexception() << "Unexpected write session";
    }
    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings&) override {
        ythrow yexception() << "Unexpected write session";
    }
    NYdb::TAsyncStatus CommitOffset(const TString&, ui64, const TString&, ui64, const NYdb::NTopic::TCommitOffsetSettings&) override {
        ythrow yexception() << "Replay validation must not commit consumer offsets";
    }
};

class THistoryTestGateway final : public NYql::IPqStaticGateway {
public:
    std::function<NYql::ITopicClient::TPtr(const NYdb::NTopic::TTopicClientSettings&)> Factory;

    NYql::IDeferredPublishClient::TPtr GetDeferredPublishClient(const NYdb::TDriver&, const NYdb::TCommonClientSettings&) override { return {}; }
    NYql::ITopicClient::TPtr GetTopicClient(const NYdb::TDriver&, const NYdb::NTopic::TTopicClientSettings& settings) override { return Factory(settings); }
    NYql::IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver&, const NYdb::NFederatedTopic::TFederatedTopicClientSettings&) override { return {}; }
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const override { return {}; }
    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override { return {}; }
};

void SetRecoveryProvider(TStateLoadPlanResolverSettings& settings, NActors::TTestActorRuntimeBase& runtime, TIntrusivePtr<THistoryTestGateway> gateway) {
    settings.ProviderIntegrations["pq"] = NKikimr::NKqp::CreatePqCheckpointProviderIntegration(
        runtime.GetActorSystem(0), std::move(gateway), NYdb::TDriver(NYdb::TDriverConfig{}), NYql::CreateStructuredTokenCredentialsFactory());
}

struct THistoryTestRuntime : NActors::TTestActorRuntimeBase {
    explicit THistoryTestRuntime(bool enabled = true)
        : Enabled(enabled)
    {
        InitNodes();
        AppendToLogSettings(NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>);
    }

    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        auto appData = std::make_shared<NKikimr::TAppData>(0, 0, 0, 0, TMap<TString, ui32>{}, nullptr, nullptr, nullptr, nullptr);
        appData->FeatureFlags.SetEnableStreamingQueryStateRecompute(Enabled);
        node->AppData0 = std::move(appData);
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }

    const bool Enabled;
};

void CheckResolver(bool force, bool expired, bool enabled = true, TMaybe<ui64> firstRetainedWriteTimeUs = {}, bool explicitOutputStartTime = false) {
    using namespace NYql::NDq;
    using namespace NYql::NDqProto::NDqStateLoadPlan;
    THistoryTestRuntime runtime(enabled);
    const auto owner = runtime.AllocateEdgeActor();
    const auto storage = runtime.AllocateEdgeActor();
    auto client = MakeIntrusive<THistoryTestClient>();
    client->Expired = expired;
    client->FirstRetainedWriteTimeUs = firstRetainedWriteTimeUs;
    TReplayTestGraph old;
    old.Source();
    old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
    old.FiniteSource(3, "KqpReadRangesSource");
    TReplayTestGraph next;
    next.Source();
    next.Hop(2, 1, 10 * Second, 30 * Second, 0);
    next.FiniteSource(4, "S3Source");
    TStateLoadPlanResolverSettings settings;
    settings.StorageProxy = storage;
    settings.GraphId = "graph";
    settings.Checkpoint.SetId(5);
    settings.Checkpoint.SetGeneration(1);
    settings.CoordinatorGeneration = 2;
    if (explicitOutputStartTime) {
        settings.OutputStartTimeUs = 590 * Second;
    }
    settings.Force = force;
    auto gateway = MakeIntrusive<THistoryTestGateway>();
    gateway->Factory = [client](const auto&) { return client; };
    SetRecoveryProvider(settings, runtime, gateway);
    auto noCheckpointReads = runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([&](auto&) {
        UNIT_ASSERT_C(enabled && !explicitOutputStartTime, "Only history replay must read checkpoints");
    });
    constexpr ui64 cookie = 42;
    const auto resolver = runtime.Register(CreateStateLoadPlanResolver(
        explicitOutputStartTime ? NProto::TGraphParams{} : old.Params(),
        next.Params(), settings, cookie),
        0, 0, NActors::TMailboxType::Simple, 0, owner);
    if (enabled && !explicitOutputStartTime) {
        const auto request = runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(storage);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds.size(), 2);
        auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(settings.Checkpoint, NYql::TIssues{}, 2);
        for (const auto taskId : request->Get()->TaskIds) {
            response->States.push_back(std::move(old.States.at(taskId)));
        }
        runtime.Send(new NActors::IEventHandle(resolver, storage, response.release()));
    }
    auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
    UNIT_ASSERT_VALUES_EQUAL(result->Cookie, cookie);
    if (!enabled && !explicitOutputStartTime) {
        UNIT_ASSERT_C(result->Get()->Result, result->Get()->Issues.ToString());
        UNIT_ASSERT(result->Get()->Issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(client->Describes, 0);
        UNIT_ASSERT_VALUES_EQUAL(client->Reads, 0);
        UNIT_ASSERT(result->Get()->Plan.at(1).GetSources(0).GetForeignTasksSources().size());
        UNIT_ASSERT(result->Get()->Plan.at(2).GetStateType() == STATE_TYPE_EMPTY);
        return;
    }
    UNIT_ASSERT_VALUES_EQUAL(client->Describes, 1);
    UNIT_ASSERT_VALUES_EQUAL(client->Reads, firstRetainedWriteTimeUs ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Result, !expired || (force && !explicitOutputStartTime), result->Get()->Issues.ToString());
    if (!expired) {
        UNIT_ASSERT(result->Get()->Issues.Empty());
        UNIT_ASSERT(!result->Get()->Plan.contains(4));
        UNIT_ASSERT(result->Get()->Plan.at(1).GetStateType() == STATE_TYPE_FOREIGN);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(result->Get()->Plan, 2), explicitOutputStartTime ? 56 : 57);
    } else {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "Required history has expired");
        if (force && !explicitOutputStartTime) {
            UNIT_ASSERT(result->Get()->Plan.at(1).GetSources(0).GetForeignTasksSources().size());
            UNIT_ASSERT(result->Get()->Plan.at(2).GetStateType() == STATE_TYPE_EMPTY);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "FORCE=true");
        } else {
            UNIT_ASSERT(result->Get()->Plan.empty());
        }
    }
}

void CheckFederatedResolver(TStateLoadPlanResolverSettings settings = {}, bool expired = false,
        bool readFirstRetained = false, const TString& topicPath = "topic") {
    using namespace NYql::NDq;
    using namespace NYql::NDqProto::NDqStateLoadPlan;
    THistoryTestRuntime runtime;
    const auto owner = runtime.AllocateEdgeActor();
    const auto storage = runtime.AllocateEdgeActor();
    auto east = MakeIntrusive<THistoryTestClient>();
    auto west = MakeIntrusive<THistoryTestClient>();
    west->Expired = expired;
    if (readFirstRetained) {
        east->FirstRetainedWriteTimeUs = 200 * Second;
        west->FirstRetainedWriteTimeUs = (expired ? 300 : 210) * Second;
    }
    TReplayTestGraph old;
    old.FederatedSource(1, 0, readFirstRetained ? 3 : 1, topicPath);
    old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
    TReplayTestGraph next;
    next.FederatedSource(1, 0, readFirstRetained ? 3 : 1, topicPath);
    next.Hop(2, 1, 10 * Second, 30 * Second, 0);
    settings.StorageProxy = storage;
    settings.GraphId = "graph";
    settings.Checkpoint.SetId(5);
    settings.Checkpoint.SetGeneration(1);
    settings.CoordinatorGeneration = 2;
    ui64 clientsCreated = 0;
    auto gateway = MakeIntrusive<THistoryTestGateway>();
    gateway->Factory = [&](const NYdb::NTopic::TTopicClientSettings& clientSettings) {
        ++clientsCreated;
        const auto& endpoint = *clientSettings.DiscoveryEndpoint_;
        UNIT_ASSERT(endpoint == "east:2135" || endpoint == "west:2135");
        UNIT_ASSERT_VALUES_EQUAL(*clientSettings.Database_, endpoint == "east:2135" ? "/east" : "/west");
        return endpoint == "east:2135" ? east : west;
    };
    SetRecoveryProvider(settings, runtime, gateway);
    auto noCheckpointReads = runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([&](auto&) {
        UNIT_ASSERT_C(!settings.OutputStartTimeUs, "Explicit OUTPUT_FROM must not read checkpoints");
    });
    const auto resolver = runtime.Register(CreateStateLoadPlanResolver(
        settings.OutputStartTimeUs ? NProto::TGraphParams{} : old.Params(),
        next.Params(), settings, 0), 0, 0, NActors::TMailboxType::Simple, 0, owner);
    if (!settings.OutputStartTimeUs) {
        const auto request = runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(storage);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds.size(), 2);
        auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(settings.Checkpoint, NYql::TIssues{}, 2);
        for (const auto taskId : request->Get()->TaskIds) {
            response->States.push_back(std::move(old.States.at(taskId)));
        }
        runtime.Send(new NActors::IEventHandle(resolver, storage, response.release()));
    }
    const auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
    if (settings.UseSourceDisposition) {
        UNIT_ASSERT_C(result->Get()->Result, result->Get()->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(clientsCreated, 0);
        UNIT_ASSERT(result->Get()->Plan.at(1).GetSources(0).GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(result->Get()->Plan, 2), 57);
        return;
    }
    UNIT_ASSERT_VALUES_EQUAL(clientsCreated, 2);
    for (const auto& client : {east, west}) {
        const TString database = client == east ? "/east" : "/west";
        const ui64 partitions = readFirstRetained ? 1 : (client == east ? 2 : 3);
        UNIT_ASSERT_VALUES_EQUAL(client->Describes, partitions);
        Sort(client->DescribedPartitions);
        for (ui64 i = 0; i < client->DescribedPartitions.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(client->DescribedPartitions[i].first, topicPath.StartsWith('/') ? topicPath : database + "/" + topicPath);
            UNIT_ASSERT_C(client->DescribedPartitions[i].second < (client == east ? 2 : 3), client->DescribedPartitions[i].second);
        }
        UNIT_ASSERT_VALUES_EQUAL(client->Reads, readFirstRetained ? partitions : 0);
    }
    const bool fallback = settings.Force && !settings.OutputStartTimeUs;
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Result, !expired || fallback, result->Get()->Issues.ToString());
    if (!expired) {
        UNIT_ASSERT(result->Get()->Issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(result->Get()->Plan, 2), 57);
        if (!readFirstRetained) {
            CheckFederatedReplayPartitions(result->Get()->Plan);
        }
    } else {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "Required history has expired");
        if (fallback) {
            UNIT_ASSERT(result->Get()->Plan.at(1).GetSources(0).GetForeignTasksSources().size());
            UNIT_ASSERT(result->Get()->Plan.at(2).GetStateType() == STATE_TYPE_EMPTY);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "FORCE=true");
        } else {
            UNIT_ASSERT(result->Get()->Plan.empty());
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(THistoryReplayResolver) {
    Y_UNIT_TEST(LegacyRunnerWithoutProvidersKeepsContinuationPath) {
        THistoryTestRuntime runtime(/* enabled */ false);
        const auto owner = runtime.AllocateEdgeActor();
        auto observer = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvGetTaskState>([](auto&) {
            UNIT_FAIL("Legacy continuation loads state in the compute actors");
        });
        TReplayTestGraph graph;
        graph.Source();
        runtime.Register(CreateStateLoadPlanResolver(graph.Params(), graph.Params(), {}, 0),
            0, 0, NActors::TMailboxType::Simple, 0, owner);
        const auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
        UNIT_ASSERT_C(result->Get()->Result, result->Get()->Issues.ToString());
        UNIT_ASSERT(!result->Get()->Plan.at(1).GetSources(0).GetForeignTasksSources().empty());
    }

    Y_UNIT_TEST(FederatedHistoryUsesClusterEndpointsAndPartitionCounts) { CheckFederatedResolver(); }
    Y_UNIT_TEST(FederatedHistoryPreservesAbsoluteTopicPaths) { CheckFederatedResolver({}, false, false, "/absolute/topic"); }
    Y_UNIT_TEST(FederatedHistoryReadsFirstRetainedMessageInEachCluster) { CheckFederatedResolver({}, false, true); }
    Y_UNIT_TEST(FederatedHistoryExpiresInOneCluster) { CheckFederatedResolver({}, true, true); }
    Y_UNIT_TEST(FederatedHistoryFallsBackOnExpiredCluster) {
        TStateLoadPlanResolverSettings settings;
        settings.Force = true;
        CheckFederatedResolver(settings, true);
    }
    Y_UNIT_TEST(FederatedExplicitOutputStartTime) {
        TStateLoadPlanResolverSettings settings;
        settings.OutputStartTimeUs = 600 * Second;
        CheckFederatedResolver(settings, false, true);
    }
    Y_UNIT_TEST(FederatedExplicitOutputStartTimeDoesNotFallBack) {
        TStateLoadPlanResolverSettings settings;
        settings.OutputStartTimeUs = 600 * Second;
        settings.Force = true;
        CheckFederatedResolver(settings, true);
    }
    Y_UNIT_TEST(FederatedExplicitReadFromDoesNotValidateCalculatedHistory) {
        TStateLoadPlanResolverSettings settings;
        settings.OutputStartTimeUs = 600 * Second;
        settings.UseSourceDisposition = true;
        CheckFederatedResolver(settings);
    }

    Y_UNIT_TEST(ExplicitReadFromDoesNotReadCheckpointsOrValidateCalculatedHistory) {
        using namespace NYql::NDq;
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        struct TRuntime : NActors::TTestActorRuntimeBase {
            TRuntime() { InitNodes(); }
        } runtime;
        const auto owner = runtime.AllocateEdgeActor();
        auto noCheckpointReads = runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([](auto&) {
            UNIT_FAIL("Explicit input and output start times must not read checkpoint state");
        });
        TReplayTestGraph graph;
        graph.Source();
        graph.Hop(2, 1, 10 * Second, 30 * Second, 0);
        TStateLoadPlanResolverSettings settings;
        settings.OutputStartTimeUs = 590 * Second;
        settings.UseSourceDisposition = true;
        // No topic client: explicit READ_FROM must not probe the calculated history range.
        runtime.Register(CreateStateLoadPlanResolver({}, graph.Params(), settings, 0),
            0, 0, NActors::TMailboxType::Simple, 0, owner);
        const auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
        UNIT_ASSERT_C(result->Get()->Result, result->Get()->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(result->Get()->Plan, 2), 56);
        UNIT_ASSERT(result->Get()->Plan.at(1).GetSources(0).GetStateType() == STATE_TYPE_EMPTY);
    }

    Y_UNIT_TEST(ExplicitOutputStartTimeDoesNotReadCheckpoints) { CheckResolver(false, false, false, {}, true); }
    Y_UNIT_TEST(ExplicitOutputStartTimeValidatesRetainedHistory) { CheckResolver(false, false, false, 250 * Second, true); }
    Y_UNIT_TEST(ExplicitOutputStartTimeDoesNotFallBackOnExpiredHistory) { CheckResolver(true, true, false, {}, true); }
    Y_UNIT_TEST(DisabledFlagSkipsCheckpointAndSourceRecovery) { CheckResolver(false, true, false); }
    Y_UNIT_TEST(DisabledFlagSkipsCheckpointAndSourceRecoveryWithForce) { CheckResolver(true, true, false); }
    Y_UNIT_TEST(ForceStillAttemptsReplay) { CheckResolver(true, false); }
    Y_UNIT_TEST(RequiredHistoryRetainedAfterOlderDataExpired) { CheckResolver(false, false, true, 250 * Second); }
    Y_UNIT_TEST(RequiredHistoryIsOlderThanFirstRetainedMessage) { CheckResolver(false, true, true, 280 * Second); }
    Y_UNIT_TEST(FirstRetainedMessageAtReplayBoundIsAvailable) { CheckResolver(false, false, true, 270 * Second); }
    Y_UNIT_TEST(ExpiredHistoryFailsWithoutForce) { CheckResolver(false, true); }
    Y_UNIT_TEST(ExpiredHistoryFallsBackToOffsets) { CheckResolver(true, true); }
}

} // namespace NFq
