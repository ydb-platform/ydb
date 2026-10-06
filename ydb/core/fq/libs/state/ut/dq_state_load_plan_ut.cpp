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

    bool MakePlan(bool force);

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

    Y_UNIT_TEST(AddingOrRemovingAllSourcesRequiresForce) {
        DstGraph.Task().Input().TopicSource("t", 1, 1, 0).Build();
        UNIT_ASSERT(!MakePlan(false));
        UNIT_ASSERT(Plan.empty());
        UNIT_ASSERT(MakePlan(true));
        AssertTaskPlanIsEmpty(1);

        SwapGraphs();
        UNIT_ASSERT(!MakePlan(false));
        UNIT_ASSERT(Plan.empty());
        UNIT_ASSERT(MakePlan(true));
        UNIT_ASSERT(Plan.empty());
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

bool TTestCase::MakePlan(bool force) {
    Plan.clear();
    Issues.Clear();
    NProto::TGraphParams src, dst;
    *src.MutableTasks() = SrcGraph.Graph;
    *dst.MutableTasks() = DstGraph.Graph;

    for (auto* graph : {&src, &dst}) {
        for (auto& task : *graph->MutableTasks()) {
            SetReplayProgram(task);
        }
    }

    const TGraphStateContext context;
    const TGraphStateInfo previous(src, context), next(dst, context);
    TSourceRecoverySet sourcesToPrepare;
    const bool result = MakeContinueFromStreamingOffsetsPlan(previous, next, force, Plan, sourcesToPrepare, Issues);
    if (result) {
        ValidatePlan();
    } else {
        UNIT_ASSERT_UNEQUAL(Issues.Size(), 0);
    }

    return result;
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

namespace {

using TRecoveryTypeFactory = std::function<NKikimr::NMiniKQL::TType*(const NKikimr::NMiniKQL::TTypeEnvironment&)>;

void SetAggregationProgram(NYql::NDqProto::TDqTask& task, const TString& table = "/Root/output",
        TRecoveryTypeFactory keyType = {}, TRecoveryTypeFactory savedType = {}, bool tied = true) {
    using namespace NKikimr::NMiniKQL;
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto* data = TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, env);
    const auto string = [&](const TString& value) {
        return TRuntimeNode(BuildDataLiteral(NYql::NUdf::TStringRef(value.data(), value.size()), NYql::NUdf::EDataSlot::String, env), true);
    };
    auto binding = string(table);

    if (tied) {
        binding = TRuntimeNode(TTupleLiteralBuilder(env).Add(binding).Add(string("mapping")).Build(), true);
    }

    TCallableBuilder aggregation(env, "KqpStreamingAggregation", data);
    for (ui32 i = 0; i < 12; ++i) {
        if (i == 8) {
            aggregation.Add(binding);
        } else {
            auto* type = i == 3 && keyType ? keyType(env) : i == 9 && savedType ? savedType(env) : data;
            aggregation.Add(TRuntimeNode(TCallableBuilder(env, "Arg", type).Build(), false));
        }
    }

    task.MutableProgram()->SetRuntimeVersion(NYql::NDqProto::RUNTIME_VERSION_YQL_1_0);
    task.MutableProgram()->SetRaw(SerializeRuntimeNode(TRuntimeNode(aggregation.Build(), false), env));
}

bool OffsetPlan(const NProto::TGraphParams& old, const NProto::TGraphParams& next, bool force,
        TStateLoadPlan& plan, NYql::TIssues& issues, TSourceRecoverySet* changed = nullptr) {
    const TGraphStateContext context;
    const TGraphStateInfo previous(old, context), target(next, context);
    TSourceRecoverySet prepared;
    const bool success = MakeContinueFromStreamingOffsetsPlan(previous, target, force, plan, prepared, issues);

    if (changed) {
        *changed = std::move(prepared);
    }

    return success;
}

void SetConsumer(NYql::NDqProto::TDqTask& task, const TString& consumer) {
    auto* settings = task.MutableInputs(0)->MutableSource()->MutableSettings();
    NYql::NPq::NProto::TDqPqTopicSource source;
    UNIT_ASSERT(settings->UnpackTo(&source));
    source.SetConsumerName(consumer);
    settings->PackFrom(source);
}

} // namespace

Y_UNIT_TEST_SUITE(TStreamingOffsetRecoveryPlan) {
    Y_UNIT_TEST(ReplayFailureAndOffsetFallbackReuseDecodedPrograms) {
        TReplayTestGraph old;
        old.Source();
        SetReplayProgram(*old.Builder.Graph.Mutable(0), 0, 0, "TimeOrderRecover");
        auto before = old.Params(false), after = old.Params(false);
        const TGraphStateContext context;
        const TGraphStateInfo previous(before, context), next(after, context);
        // Any attempt to deserialize again must fail. The retained nodes remain valid.
        before.MutableTasks(0)->MutableProgram()->SetRaw("invalid MiniKQL");
        after.MutableTasks(0)->MutableProgram()->SetRaw("invalid MiniKQL");
        TStateLoadPlan plan;
        TSourceRecoverySet changed;
        NYql::TIssues issues;
        UNIT_ASSERT(!MakeHistoryReplayPlan(previous, next, old.States, plan, issues));
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");
        issues.Clear();
        UNIT_ASSERT_C(MakeContinueFromStreamingOffsetsPlan(previous, next, true, plan, changed, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
    }

    Y_UNIT_TEST(OffsetPlanningPreservesEarlierIssues) {
        TReplayTestGraph old;
        old.Source();
        SetReplayProgram(*old.Builder.Graph.Mutable(0), 0, 0, "TimeOrderRecover");

        for (bool ambiguousBinding : {false, true}) {
            auto next = old.Params(false);

            if (ambiguousBinding) {
                SetAggregationProgram(*next.MutableTasks(0));
                auto duplicate = next.GetTasks(0);
                duplicate.SetId(2);
                duplicate.SetStageId(2);
                *next.AddTasks() = duplicate;
            }

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue("Previous replay failure"));
                UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), next, force, plan, issues), force && !ambiguousBinding);
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Previous replay failure");
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");

                if (ambiguousBinding) {
                    UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Ambiguous streaming aggregation output table binding");
                }

                if (force && !ambiguousBinding) {
                    UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                } else {
                    UNIT_ASSERT(plan.empty());
                }
            }
        }
    }

    Y_UNIT_TEST(DiscoveryRejectsMalformedStreamingGraphs) {
        for (ui32 corruption = 0; corruption < 4; ++corruption) {
            TReplayTestGraph graph;
            graph.Source();
            auto params = graph.Params(false);

            if (corruption == 0) {
                params.MutableTasks(0)->MutableProgram()->SetRaw("invalid MiniKQL");
            } else if (corruption == 1) {
                params.MutableTasks(0)->MutableProgram()->ClearRaw();
            } else {
                auto replica = params.GetTasks(0);

                if (corruption == 3) {
                    replica.SetId(2);
                    SetReplayProgram(replica, 0, 0, "TimeOrderRecover");
                    replica.SetStageId(params.GetTasks(0).GetStageId());
                }

                *params.AddTasks() = replica;
            }

            const TGraphStateContext context;
            UNIT_ASSERT_EXCEPTION(TGraphStateInfo(params, context), yexception);
        }
    }

    Y_UNIT_TEST(ForceKeepsCompatibleMappingWhileDroppingIncompatibleAggregation) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 30 * Second, 0);
        old.Hop(3, 1, 10 * Second, 30 * Second, 0);
        SetAggregationProgram(*old.Builder.Graph.Mutable(1), "/Root/first");
        SetAggregationProgram(*old.Builder.Graph.Mutable(2), "/Root/second");
        auto next = old.Params(false);
        SetAggregationProgram(*next.MutableTasks(2), "/Root/third");
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next, true, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(2).GetProgram().GetForeignTaskId(), 2);
        UNIT_ASSERT(!plan.at(3).GetProgram().HasForeignTaskId());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "/Root/second");
    }

    Y_UNIT_TEST(AmbiguousBindingIsRejectedEvenWithForce) {
        TReplayTestGraph old;
        old.Source();
        SetAggregationProgram(*old.Builder.Graph.Mutable(0));
        auto next = old.Params(false);
        auto duplicate = next.GetTasks(0);
        duplicate.SetId(2);
        duplicate.SetStageId(2);
        *next.AddTasks() = duplicate;
        const TGraphStateContext context;
        const TGraphStateInfo discovery(next, context); // Discovery does not validate binding uniqueness.
        UNIT_ASSERT_VALUES_EQUAL(discovery.GetStages().size(), 2);

        for (bool force : {false, true}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!OffsetPlan(old.Params(), next, force, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Ambiguous streaming aggregation output table binding");
        }
    }

    Y_UNIT_TEST(AddingTiedAggregationStartsFresh) {
        TReplayTestGraph old;
        old.Source();
        auto next = old.Params(false);
        SetAggregationProgram(*next.MutableTasks(0));
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next, false, plan, issues), issues.ToString());
        UNIT_ASSERT(!plan.at(1).GetProgram().HasForeignTaskId());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
    }

    Y_UNIT_TEST(AnyContributingConsumerChangeRequiresPreparation) {
        TReplayTestGraph old;
        old.Source(1);
        old.Source(2);
        SetConsumer(*old.Builder.Graph.Mutable(0), "old");
        SetConsumer(*old.Builder.Graph.Mutable(1), "new");
        TReplayTestGraph next;
        next.Source(10);
        SetConsumer(*next.Builder.Graph.Mutable(0), "new");

        for (bool reverseTasks : {false, true}) {
            auto previous = old.Params();

            if (reverseTasks) {
                previous.MutableTasks()->SwapElements(0, 1);
            }

            TStateLoadPlan plan;
            NYql::TIssues issues;
            TSourceRecoverySet changed;
            UNIT_ASSERT_C(OffsetPlan(previous, next.Params(), true, plan, issues, &changed), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(plan.at(10).GetSources(0).ForeignTasksSourcesSize(), 2);
            UNIT_ASSERT(changed.contains(std::pair<ui64, ui64>{10, 0}));
        }
    }

    Y_UNIT_TEST(ConsumerChangesAreScopedToContributingPartitions) {
        TReplayTestGraph old, next;
        old.Source(1, 0, 2);
        old.Source(2, 1, 2);
        next.Source(10, 0, 2);
        next.Source(20, 1, 2);
        SetConsumer(*old.Builder.Graph.Mutable(0), "a");
        SetConsumer(*old.Builder.Graph.Mutable(1), "b");
        SetConsumer(*next.Builder.Graph.Mutable(1), "b");

        for (bool reverseTasks : {false, true}) {
            auto previous = old.Params();

            if (reverseTasks) {
                previous.MutableTasks()->SwapElements(0, 1);
            }

            for (bool changeConsumer : {false, true}) {
                SetConsumer(*next.Builder.Graph.Mutable(0), changeConsumer ? "b" : "a");
                TStateLoadPlan plan;
                NYql::TIssues issues;
                TSourceRecoverySet changed;
                UNIT_ASSERT_C(OffsetPlan(previous, next.Params(), false, plan, issues, &changed), issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(plan.at(10).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                UNIT_ASSERT_VALUES_EQUAL(plan.at(20).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 2);
                UNIT_ASSERT_VALUES_EQUAL(changed.size(), changeConsumer ? 1 : 0);
                UNIT_ASSERT_VALUES_EQUAL(changed.contains(std::pair<ui64, ui64>{10, 0}), changeConsumer);
                UNIT_ASSERT(!changed.contains(std::pair<ui64, ui64>{20, 0}));
            }
        }
    }

    Y_UNIT_TEST(DiscoverySharesStageProgramsAndDefersOperatorValidation) {
        for (bool packed : {false, true}) {
            TReplayTestGraph graph;
            graph.Source();
            SetReplayProgram(*graph.Builder.Graph.Mutable(0), 0, 0, "TimeOrderRecover");
            auto replica = graph.Builder.Graph.Get(0);
            replica.SetId(2);
            *graph.Builder.Graph.Add() = replica;
            graph.FiniteSource(3, "S3Source");
            graph.Builder.Graph.Mutable(2)->MutableProgram()->SetRaw("invalid finite program is irrelevant");
            const auto params = graph.Params(packed);
            const TGraphStateContext context;
            const TGraphStateInfo info(params, context);
            UNIT_ASSERT_VALUES_EQUAL(info.GetStages().size(), 1);
            const auto& stage = info.GetStages().front();
            UNIT_ASSERT_VALUES_EQUAL(stage.Tasks.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stage.StatefulOperators.size(), 1);
            UNIT_ASSERT(!info.HasHopping());
            auto guard = info.BindAllocator();
            UNIT_ASSERT_EXCEPTION_CONTAINS(TStageStateRecoveryInfo{stage}, yexception, "Unsupported checkpointed operator");
        }
    }

    Y_UNIT_TEST(AggregationWithoutDirectSourceAndNewOperator) {
        TReplayTestGraph old;
        old.Source();
        old.Hop(2, 1, 10 * Second, 30 * Second, 0);
        SetAggregationProgram(*old.Builder.Graph.Mutable(1));
        auto next = old.Params(false);
        next.MutableTasks(1)->SetId(5);
        next.MutableTasks(1)->SetStageId(5);
        auto added = next.GetTasks(1);
        added.SetId(6);
        added.SetStageId(6);
        SetReplayProgram(added, 0, 0, "TimeOrderRecover");
        *next.AddTasks() = added;
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next, false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(5).GetProgram().GetForeignTaskId(), 2);
        UNIT_ASSERT(plan.at(5).GetSources().empty());
        UNIT_ASSERT(!plan.at(6).GetProgram().HasForeignTaskId());
    }

    Y_UNIT_TEST(RemovedAggregationAndChangedTaskCountRequireForce) {
        for (bool removed : {false, true}) {
            TReplayTestGraph old;
            old.Source();
            SetAggregationProgram(*old.Builder.Graph.Mutable(0));
            auto next = old.Params(false);

            if (removed) {
                SetReplayProgram(*next.MutableTasks(0));
            } else {
                auto replica = next.GetTasks(0);
                replica.SetId(2);
                *next.AddTasks() = replica;
            }

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), next, force, plan, issues), force);
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), removed
                    ? "Streaming aggregation output table is missing in the new query: /Root/output, available output tables: none"
                    : "Streaming aggregation task count changed for output table /Root/output: 1 -> 2 on stage 1");

                if (force) {
                    UNIT_ASSERT(!plan.at(1).GetProgram().HasForeignTaskId());
                    UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                } else {
                    UNIT_ASSERT(plan.empty());
                }
            }
        }
    }

    Y_UNIT_TEST(MissingAggregationReportsSortedOutputTables) {
        TReplayTestGraph old;
        old.Source();
        SetAggregationProgram(*old.Builder.Graph.Mutable(/* index */ 0));
        TReplayTestGraph next;
        for (const TString table : {"/Root/zeta", "/Root/alpha", "/Root/middle"}) {
            next.Source(next.Builder.Graph.size() + 1);
            SetAggregationProgram(*next.Builder.Graph.rbegin(), table);
        }

        for (bool force : {false, true}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), next.Params(), force, plan, issues), force);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), TStringBuilder()
                << "Streaming aggregation output table is missing in the new query: /Root/output"
                << ", available output tables: /Root/alpha, /Root/middle, /Root/zeta"
                << (force ? ", FORCE=true discards this state" : ""));
        }
    }

    Y_UNIT_TEST(ExactStructuralKeyAndSavedStateCompatibility) {
        using namespace NKikimr::NMiniKQL;
        const auto makeType = [](ui32 variant) -> TRecoveryTypeFactory {
            return [variant](const TTypeEnvironment& env) -> TType* {
                auto* u64 = TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, env);
                auto* u32 = TDataType::Create(NYql::NUdf::TDataType<ui32>::Id, env);
                TType* item = u64;
                switch (variant) {
                    case 2: item = u32; break;
                    case 3: item = TOptionalType::Create(u64, env); break;
                    case 4: item = TListType::Create(TOptionalType::Create(u64, env), env); break;
                    case 5: item = TListType::Create(u64, env); break;
                    case 6: item = TDataDecimalType::Create(20, 2, env); break;
                    case 7: item = TDataDecimalType::Create(20, 3, env); break;
                    case 8: item = TTaggedType::Create(u64, "a", env); break;
                    case 9: item = TTaggedType::Create(u64, "b", env); break;
                    case 10: case 11: {
                        TType* elements[] = {variant == 10 ? u64 : u32, variant == 10 ? u32 : u64};
                        item = TTupleType::Create(2, elements, env);
                        break;
                    }
                }
                TStructTypeBuilder type(env);
                type.Add(variant == 1 ? "renamed" : "value", item);

                if (variant == 12) {
                    type.Add("extra", u64);
                }

                return type.Build();
            };
        };

        for (bool key : {false, true}) {
            for (const auto [before, after] : {std::pair{0, 0}, {0, 1}, {0, 2}, {0, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}, {0, 12}, {12, 0}}) {
                TReplayTestGraph old, next;
                old.Source();
                next.Source();
                SetAggregationProgram(*old.Builder.Graph.Mutable(0), "/Root/output", key ? makeType(before) : TRecoveryTypeFactory{}, key ? TRecoveryTypeFactory{} : makeType(before));
                SetAggregationProgram(*next.Builder.Graph.Mutable(0), "/Root/output", key ? makeType(after) : TRecoveryTypeFactory{}, key ? TRecoveryTypeFactory{} : makeType(after));

                for (bool force : {false, true}) {
                    TStateLoadPlan plan;
                    NYql::TIssues issues;
                    UNIT_ASSERT_VALUES_EQUAL_C(OffsetPlan(old.Params(), next.Params(), force, plan, issues), force || before == after, issues.ToString());

                    if (before == after) {
                        UNIT_ASSERT(plan.at(1).GetProgram().HasForeignTaskId());
                    } else {
                        const auto message = issues.ToString();
                        UNIT_ASSERT_STRING_CONTAINS(message, key ? "key type changed" : "saved state type changed");
                        UNIT_ASSERT_STRING_CONTAINS(message, ", previous: ");
                        UNIT_ASSERT_STRING_CONTAINS(message, ", new: ");

                        if (before == 0 && after == 2) {
                            UNIT_ASSERT_STRING_CONTAINS(message, "Uint64");
                            UNIT_ASSERT_STRING_CONTAINS(message, "Uint32");
                        }

                        UNIT_ASSERT(force ? !plan.at(1).GetProgram().HasForeignTaskId() : plan.empty());
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(PreviousUnsupportedOperatorsAndBackendsRequireForce) {
        for (ui32 kind = 0; kind < 4; ++kind) {
            TReplayTestGraph old;
            old.Source();
            auto& task = *old.Builder.Graph.Mutable(0);

            if (kind < 2) {
                SetReplayProgram(task, 0, 0, kind ? "MatchRecognizeCore" : "TimeOrderRecover");
            } else {
                SetAggregationProgram(task, kind == 2 ? "" : "/Root/state", {}, {}, false);
            }

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), old.Params(), force, plan, issues), force);
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), kind < 2
                    ? "Unsupported checkpointed operator" : "Unsupported checkpointed streaming aggregation setup");
                UNIT_ASSERT(force ? !plan.at(1).GetProgram().HasForeignTaskId() : plan.empty());
            }
        }
    }

    Y_UNIT_TEST(PreviousSinkStateRequiresForce) {
        for (bool dedup : {false, true}) {
            TReplayTestGraph old;
            old.Source();
            SetPqSink(*old.Builder.Graph.Mutable(0)->AddOutputs(), dedup, !dedup);

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), old.Params(), force, plan, issues), force);
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), dedup ? "deduplication" : "exactly-once");

                if (force) {
                    UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                } else {
                    UNIT_ASSERT(plan.empty());
                }
            }
        }
    }

    Y_UNIT_TEST(StatelessSinksStartFresh) {
        for (const TString type : {"PqSink", "KqpTableSink", "SolomonSink", "OtherSink"}) {
            TReplayTestGraph old;
            old.Source();
            auto& output = *old.Builder.Graph.Mutable(0)->AddOutputs();

            if (type == "PqSink") {
                SetPqSink(output, false, false);
            } else {
                output.MutableSink()->SetType(type);
            }

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_C(OffsetPlan(old.Params(), old.Params(), force, plan, issues), issues.ToString());
                UNIT_ASSERT_C(issues.Empty(), issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                UNIT_ASSERT(plan.at(1).GetSinks(0).GetStateType() == NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            }
        }
    }

    Y_UNIT_TEST(PhysicalSourceIdentityIncludesFederationAndEndpoint) {
        for (bool federated : {false, true}) {
            TReplayTestGraph old;

            if (federated) {
                old.FederatedSource();
            } else {
                old.Source();
            }

            auto next = old.Params();
            auto* settings = next.MutableTasks(0)->MutableInputs(0)->MutableSource()->MutableSettings();
            NYql::NPq::NProto::TDqPqTopicSource source;
            UNIT_ASSERT(settings->UnpackTo(&source));

            if (federated) {
                source.MutableFederatedClusters(0)->SetEndpoint("other:2135");
            } else {
                source.SetEndpoint("other:2135");
            }

            settings->PackFrom(source);
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT(!OffsetPlan(old.Params(), next, false, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "not found in previous query");
        }
    }
}

namespace {

void SetShuffleProgram(NYql::NDqProto::TDqTask& task, bool block = false, bool narrowKey = false, TMaybe<ui32> blockColumn = {}) {
    using namespace NKikimr::NMiniKQL;
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto* u64 = TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, env);
    TType* first = narrowKey ? TDataType::Create(NYql::NUdf::TDataType<ui32>::Id, env) : u64;
    TType* items[] = {first, u64};

    if (block) {
        for (ui32 i = 0; i < 2; ++i) {
            if (!blockColumn || i == *blockColumn) {
                items[i] = TBlockType::Create(items[i], TBlockType::EShape::Many, env);
            }
        }
    }

    auto* type = TStreamType::Create(TMultiType::Create(2, items, env), env);
    const auto program = TRuntimeNode(TCallableBuilder(env, "Output", type).Build(), false);
    const auto root = TRuntimeNode(TStructLiteralBuilder(env).Add("Program", program).Build(), true);
    task.MutableProgram()->SetRaw(SerializeRuntimeNode(root, env));
    task.MutableProgram()->SetRuntimeVersion(NYql::NDqProto::RUNTIME_VERSION_YQL_1_0);
}

TReplayTestGraph MakeShuffleAggregation(ui32 producers = 1, bool map = false, bool block = false) {
    TReplayTestGraph graph;
    TVector<NYql::NDqProto::TDqTask*> inputs, outputs, maps;
    for (ui32 i = 0; i < producers; ++i) {
        graph.Source(i + 1, i, 2, false, producers);
        auto* task = graph.Builder.Graph.Mutable(i);
        SetShuffleProgram(*task, block);
        task->SetStageId(1);
        inputs.push_back(task);
    }

    for (ui32 slot = 0; slot < 2; ++slot) {
        auto task = graph.Builder.Task(100 + slot);
        task.Input().In->MutableUnionAll();
        task.Task->SetStageId(50);
        SetAggregationProgram(*task.Task);
        outputs.push_back(task.Task);

        if (map) {
            auto middle = graph.Builder.Task(10 + slot);
            middle.Input().In->MutableUnionAll();
            SetReplayProgram(*middle.Task);
            middle.Task->SetStageId(40);
            maps.push_back(middle.Task);
        }
    }

    ui64 channelId = 0;
    const auto connect = [&](auto& output, auto* from, auto* to) {
        auto& channel = *output.AddChannels();
        channel.SetId(++channelId);
        channel.SetSrcTaskId(from->GetId());
        channel.SetDstTaskId(to->GetId());
        channel.SetSrcStageId(from->GetStageId());
        channel.SetDstStageId(to->GetStageId());
        *to->MutableInputs(0)->AddChannels() = channel;
    };

    for (auto* task : inputs) {
        auto& output = *task->AddOutputs();
        auto& hash = *output.MutableHashPartition();
        hash.AddKeyColumns("0");
        hash.AddKeyColumns("1");
        hash.SetPartitionsCount(2);
        hash.MutableHashV1();

        for (ui32 slot = 0; slot < 2; ++slot) {
            connect(output, task, map ? maps[slot] : outputs[slot]);
        }
    }

    for (ui32 slot = 0; slot < maps.size(); ++slot) {
        auto& output = *maps[slot]->AddOutputs();
        output.MutableMap();
        connect(output, maps[slot], outputs[slot]);
    }

    return graph;
}

} // namespace

Y_UNIT_TEST_SUITE(TAggregationShuffleRecovery) {
    Y_UNIT_TEST(MatchesTablesAndSortedTasksAcrossStageAndTaskIds) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;
        auto old = MakeShuffleAggregation(2);
        auto next = MakeShuffleAggregation(2);
        const auto remapChannel = [](auto& channel) {
            channel.SetSrcTaskId(channel.GetSrcTaskId() + 1000);
            channel.SetDstTaskId(channel.GetDstTaskId() + 1000);
            channel.SetSrcStageId(channel.GetSrcStageId() + 100);
            channel.SetDstStageId(channel.GetDstStageId() + 100);
        };

        for (auto& task : next.Builder.Graph) {
            task.SetId(task.GetId() + 1000);
            task.SetStageId(task.GetStageId() + 100);

            for (auto& input : *task.MutableInputs()) {
                for (auto& channel : *input.MutableChannels()) {
                    remapChannel(channel);
                }
            }

            for (auto& output : *task.MutableOutputs()) {
                for (auto& channel : *output.MutableChannels()) {
                    remapChannel(channel);
                }
            }
        }

        old.Builder.Graph.SwapElements(0, 3);
        next.Builder.Graph.SwapElements(1, 2);

        for (bool packed : {false, true}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            TSourceRecoverySet changed;
            UNIT_ASSERT_C(OffsetPlan(old.Params(packed), next.Params(packed), false, plan, issues, &changed), issues.ToString());
            UNIT_ASSERT(changed.empty());

            for (const auto previous : {100, 101}) {
                const auto& taskPlan = plan.at(previous + 1000);
                UNIT_ASSERT(taskPlan.GetProgram().GetStateType() == STATE_TYPE_FOREIGN);
                UNIT_ASSERT_VALUES_EQUAL(taskPlan.GetProgram().GetForeignTaskId(), previous);
                UNIT_ASSERT(taskPlan.GetSources().empty());
            }

            for (const auto previous : {1, 2}) {
                UNIT_ASSERT_VALUES_EQUAL(plan.at(previous + 1000).GetSources(0).GetForeignTasksSources(0).GetTaskId(), previous);
            }
        }
    }

    Y_UNIT_TEST(KeylessUnionAllAggregationRestoresProgram) {
        auto old = MakeShuffleAggregation(2);
        old.Builder.Graph.RemoveLast(); // One aggregation task receives both producer streams.
        SetAggregationProgram(*old.Builder.Graph.Mutable(2), "/Root/output",
            [](const NKikimr::NMiniKQL::TTypeEnvironment& env) -> NKikimr::NMiniKQL::TType* {
                return NKikimr::NMiniKQL::TStructTypeBuilder(env).Build();
            });

        for (ui32 i = 0; i < 2; ++i) {
            auto& output = *old.Builder.Graph.Mutable(i)->MutableOutputs(0);
            output.MutableMap();
            output.MutableChannels()->RemoveLast();
        }

        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), old.Params(), false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(100).GetProgram().GetForeignTaskId(), 100);
        UNIT_ASSERT(plan.at(100).GetSources().empty());
        UNIT_ASSERT(issues.Empty());
    }

    Y_UNIT_TEST(HashingModeUsesOnlyKeyColumns) {
        auto old = MakeShuffleAggregation();
        auto& task = *old.Builder.Graph.Mutable(0);
        auto& hash = *task.MutableOutputs(0)->MutableHashPartition();
        hash.ClearKeyColumns();
        hash.AddKeyColumns("1");
        auto next = old.Params(false);
        // A non-key block column does not select block hashing.
        SetShuffleProgram(*next.MutableTasks(0), true, false, 0);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next, false, plan, issues), issues.ToString());
        // A block key does, even when the first column is not a block.
        SetShuffleProgram(*next.MutableTasks(0), true, false, 1);
        UNIT_ASSERT(!OffsetPlan(old.Params(), next, false, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "shuffle routing changed");
    }

    Y_UNIT_TEST(ConflictingProducerSlotOwnershipCannotBeRecovered) {
        auto old = MakeShuffleAggregation(2);
        old.Builder.Graph.Mutable(1)->MutableOutputs(0)->MutableChannels()->SwapElements(0, 1);

        for (bool force : {false, true}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), old.Params(), force, plan, issues), force);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unexpected aggregation hash shuffle routing");

            if (force) {
                UNIT_ASSERT(!plan.at(100).GetProgram().HasForeignTaskId());
                UNIT_ASSERT(!plan.at(101).GetProgram().HasForeignTaskId());
            } else {
                UNIT_ASSERT(plan.empty());
            }
        }
    }

    Y_UNIT_TEST(MultiTaskAggregationRequiresDirectShuffle) {
        TReplayTestGraph old;
        old.Source(1, 0, 2);
        old.Source(2, 1, 2);

        for (auto& task : old.Builder.Graph) {
            SetAggregationProgram(task);
            task.SetStageId(1);
        }

        auto next = old.Params();
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT(!OffsetPlan(old.Params(), next, false, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "expected direct hash shuffle");
        UNIT_ASSERT(OffsetPlan(old.Params(), next, true, plan, issues));
        UNIT_ASSERT(!plan.at(1).GetProgram().HasForeignTaskId());
    }

    Y_UNIT_TEST(ProducerParallelismDoesNotChangeKeyOwnership) {
        auto old = MakeShuffleAggregation();
        auto next = MakeShuffleAggregation(2);
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next.Params(), false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(100).GetProgram().GetForeignTaskId(), 100);
        UNIT_ASSERT_VALUES_EQUAL(plan.at(101).GetProgram().GetForeignTaskId(), 101);
    }

    Y_UNIT_TEST(HashKindKeysTypesRepresentationAndSlotsMustMatch) {
        for (ui32 change = 0; change < 5; ++change) {
            auto old = MakeShuffleAggregation();
            auto next = old.Params(false);
            auto& output = *next.MutableTasks(0)->MutableOutputs(0);
            auto& hash = *output.MutableHashPartition();
            switch (change) {
                case 0: hash.MutableHashV2(); break;
                case 1: hash.MutableKeyColumns()->SwapElements(0, 1); break;
                case 2: output.MutableChannels()->SwapElements(0, 1); break;
                case 3: SetShuffleProgram(*next.MutableTasks(0), true); break;
                case 4: SetShuffleProgram(*next.MutableTasks(0), false, true); break;
            }

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_VALUES_EQUAL_C(OffsetPlan(old.Params(), next, force, plan, issues), force, issues.ToString());
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "shuffle routing");

                if (force) {
                    UNIT_ASSERT(!plan.at(100).GetProgram().HasForeignTaskId());
                    UNIT_ASSERT(!plan.at(101).GetProgram().HasForeignTaskId());
                    UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                } else {
                    UNIT_ASSERT(plan.empty());
                }
            }
        }
    }

    Y_UNIT_TEST(ColumnShardBucketMappingMustMatch) {
        auto old = MakeShuffleAggregation();
        auto* hash = old.Builder.Graph.Mutable(0)->MutableOutputs(0)->MutableHashPartition()->MutableColumnShardHashV1();
        hash->SetShardCount(2);
        hash->AddTaskIndexByHash(0);
        hash->AddTaskIndexByHash(1);
        hash->AddKeyColumnTypes(NYql::NUdf::TDataType<ui64>::Id);
        hash->AddKeyColumnTypes(NYql::NUdf::TDataType<ui64>::Id);
        auto next = old.Params();
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next, false, plan, issues), issues.ToString());
        next.MutableTasks(0)->MutableOutputs(0)->MutableHashPartition()->MutableColumnShardHashV1()->MutableTaskIndexByHash()->SwapElements(0, 1);
        UNIT_ASSERT(!OffsetPlan(old.Params(), next, false, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "shuffle routing");
    }

    Y_UNIT_TEST(OmittedHashKindUsesHashV1) {
        auto old = MakeShuffleAggregation();
        auto next = old.Params();
        next.MutableTasks(0)->MutableOutputs(0)->MutableHashPartition()->ClearHashV1();
        TStateLoadPlan plan;
        NYql::TIssues issues;
        UNIT_ASSERT_C(OffsetPlan(old.Params(), next, false, plan, issues), issues.ToString());
    }

    Y_UNIT_TEST(MapBetweenShuffleAndAggregationRequiresForce) {
        for (bool previousHasMap : {false, true}) {
            auto old = MakeShuffleAggregation(1, previousHasMap);
            auto next = MakeShuffleAggregation(1, !previousHasMap);

            for (bool force : {false, true}) {
                TStateLoadPlan plan;
                NYql::TIssues issues;
                UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), next.Params(), force, plan, issues), force);
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "expected direct hash shuffle");

                if (force) {
                    UNIT_ASSERT(!plan.at(100).GetProgram().HasForeignTaskId());
                    UNIT_ASSERT(!plan.at(101).GetProgram().HasForeignTaskId());
                    UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
                } else {
                    UNIT_ASSERT(plan.empty());
                }
            }
        }
    }

    Y_UNIT_TEST(UnverifiableRoutingRequiresForce) {
        auto old = MakeShuffleAggregation();
        auto next = old.Params();
        next.MutableTasks(0)->MutableOutputs(0)->MutableBroadcast();

        for (bool force : {false, true}) {
            TStateLoadPlan plan;
            NYql::TIssues issues;
            UNIT_ASSERT_VALUES_EQUAL(OffsetPlan(old.Params(), next, force, plan, issues), force);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported aggregation input routing");
        }
    }
}

namespace {

bool HistoryReplayPlan(const NProto::TGraphParams& src, const NProto::TGraphParams& dst,
    const TCheckpointTaskStates& states, TStateLoadPlan& plan, NYql::TIssues& issues)
{
    try {
        const TGraphStateContext context;
        const TGraphStateInfo previous(src, context), next(dst, context);
        return MakeHistoryReplayPlan(previous, next, states, plan, issues);
    } catch (const std::exception& e) {
        issues.AddIssue(NYql::TIssue(e.what()));
        return false;
    }
}

bool OutputStartTimeReplayPlan(const NProto::TGraphParams& graph, ui64 outputStartTimeUs,
    bool useSourceDisposition, TStateLoadPlan& plan, NYql::TIssues& issues)
{
    try {
        const TGraphStateContext context;
        const TGraphStateInfo discovered(graph, context);
        return MakeOutputStartTimeReplayPlan(discovered, outputStartTimeUs, useSourceDisposition, plan, issues);
    } catch (const std::exception& e) {
        issues.AddIssue(NYql::TIssue(e.what()));
        return false;
    }
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
                UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "adjust hopping watermark policy");
                UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), 600 * Second, false, plan, issues));
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
        UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
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
        UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "requires a watermark generator before each hopping operator");

        for (bool useSourceDisposition : {false, true}) {
            issues.Clear();
            UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), 600 * Second, useSourceDisposition, plan, issues));
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
        UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
        UNIT_ASSERT(plan.at(1).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 4), 57);
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
                UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(packOld), graph.Params(packNext), graph.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT(!OutputStartTimeReplayPlan(params, 600 * Second, /* useSourceDisposition */ false, plan, issues));
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
            UNIT_ASSERT(!HistoryReplayPlan(old.Params(), next, old.States, plan, issues));
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
        UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
            UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
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
                UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues));
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
            UNIT_ASSERT(!OutputStartTimeReplayPlan(next.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping minimum window start checking is not enabled");
            UNIT_ASSERT(!HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            // Both the previous and replacement query must opt in.
            UNIT_ASSERT(!HistoryReplayPlan(next.Params(), old.Params(), next.States, plan, issues));
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
            UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
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
        UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
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
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), time, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
                UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), time, useSourceDisposition, plan, issues));
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
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), time, /* useSourceDisposition */ true, plan, issues), issues.ToString());
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
                    ? HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues)
                    : OutputStartTimeReplayPlan(graph.Params(), outputStart, /* useSourceDisposition */ false, plan, issues);
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
        UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), 50 * Second, /* useSourceDisposition */ false, plan, issues));
        UNIT_ASSERT(plan.empty());
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Hopping recovery time underflow");

        issues.Clear();
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 60 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 601 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second + 123, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 601 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 600 * Second);

        issues.Clear();
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ true, plan, issues), issues.ToString());
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
            UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), 600 * Second, useSourceDisposition, plan, issues));
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
            UNIT_ASSERT(!OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues));
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
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
            UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
            UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
            UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT(!HistoryReplayPlan(old.Params(), old.Params(), old.States, plan, issues));
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
                    ? HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues)
                    : OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues);
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.at(1).SourcesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan.at(1).GetSources(0).GetInputIndex(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);

        for (const bool useSourceDisposition : {false, true}) {
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(next.Params(), 600 * Second, useSourceDisposition, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        UNIT_ASSERT(!plan.contains(4));
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), 270 * Second);
        UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);

        for (const bool useSourceDisposition : {false, true}) {
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(next.Params(), 600 * Second, useSourceDisposition, plan, issues), issues.ToString());
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
        UNIT_ASSERT(!HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues));
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
            UNIT_ASSERT_C(HistoryReplayPlan(old.Params(), next.Params(), old.States, plan, issues), issues.ToString());
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
            UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReplayWindowStartIndex(plan, 2), 57);
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
        UNIT_ASSERT_C(HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues), issues.ToString());
        UNIT_ASSERT(plan.at(1).GetProgram().GetStateType() == STATE_TYPE_EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);

        saved.SetDeferredPublicationIntId(42);
        state.Data.Blob = saved.SerializeAsString();
        plan.clear();
        UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
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
            UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), stateless.Params(), graph.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), expected);
            issues.Clear();
            UNIT_ASSERT_C(HistoryReplayPlan(stateless.Params(), graph.Params(), stateless.States, plan, issues), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(plan.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ReplayReadTime(plan, 1), Second);
            issues.Clear();
            UNIT_ASSERT_C(OutputStartTimeReplayPlan(graph.Params(), 600 * Second, /* useSourceDisposition */ false, plan, issues), issues.ToString());
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
        UNIT_ASSERT(!HistoryReplayPlan(graph.Params(), graph.Params(), graph.States, plan, issues));
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
            UNIT_ASSERT(!HistoryReplayPlan(unsupported.Params(), supported.Params(), unsupported.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), operation);
            issues.Clear();
            UNIT_ASSERT(!HistoryReplayPlan(supported.Params(), unsupported.Params(), supported.States, plan, issues));
            UNIT_ASSERT(plan.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Unsupported checkpointed operator");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), operation);
        }
    }
}

namespace {

class THistoryTestClient final {
public:
    bool Expired = false;
    TMaybe<ui64> FirstRetainedWriteTimeUs;
    TMaybe<ui64> StartOffset;
    ui64 EndOffset = 100;
    ui64 CommittedOffset = 150;
    ui64 PartitionCount = 1;
    TString Consumer;
    bool FailCommit = false;
    TVector<ui64> Commits;
    ui64 Describes = 0;
    ui64 Reads = 0;
    TVector<std::pair<TString, ui64>> DescribedPartitions;
    std::shared_ptr<NYdb::TDriver> Driver;
    NTestUtils::IMockPqGateway::TPtr Gateway;

    NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(const TString& topicPath, TMessageStreamPartitionId partitionId) {
        ++Describes;
        DescribedPartitions.emplace_back(topicPath, partitionId.Value);
        TMessageStreamPartitionDescription description;
        description.PartitionId = partitionId;
        description.StartOffset = GetStartOffset();
        description.EndOffset = EndOffset;
        return NThreading::MakeFuture(TMessageStreamResult<TMessageStreamPartitionDescription>::Success(description));
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamDescription>> DescribeStream(const TString&) {
        ythrow yexception() << "Unexpected DescribeTopic";
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString&, const TString& consumer, const TMessageStreamDescribeConsumerSettings&)
    {
        UNIT_ASSERT(Consumer);
        UNIT_ASSERT_VALUES_EQUAL(consumer, Consumer);
        ++Describes;
        TMessageStreamConsumerDescription description;
        for (ui64 id = 0; id < PartitionCount; ++id) {
            TMessageStreamConsumerPartition partition;
            partition.PartitionId = {id};
            partition.StartOffset = GetStartOffset();
            partition.EndOffset = EndOffset;
            partition.CommittedOffset = CommittedOffset;
            description.Partitions.push_back(std::move(partition));
        }

        return NThreading::MakeFuture(TMessageStreamResult<TMessageStreamConsumerDescription>::Success(std::move(description)));
    }
    std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TString& stream, const TMessageStreamReadSessionSettings& settings) {
        UNIT_ASSERT(FirstRetainedWriteTimeUs);
        UNIT_ASSERT_VALUES_EQUAL(!settings.Consumer, Consumer.empty());
        ++Reads;
        Driver = std::make_shared<NYdb::TDriver>(NYdb::TDriverConfig{});
        Gateway = NTestUtils::CreateMockPqGateway();
        auto client = Gateway->GetTopicClient(stream, *Driver, {});
        auto session = client->CreateReadSession(settings);
        Gateway->WaitReadSession(stream)->AddDataReceivedEvent(
            GetStartOffset(), "unused", TInstant::MicroSeconds(FirstRetainedWriteTimeUs.GetOrElse(600 * Second)));
        return session;
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerPosition>> CommitPosition(
        const TString&, TMessageStreamPartitionId partitionId, const TString& consumer, ui64 offset)
    {
        UNIT_ASSERT(Consumer);
        UNIT_ASSERT_VALUES_EQUAL(consumer, Consumer);
        Commits.push_back(offset);

        if (!FailCommit) {
            CommittedOffset = offset;
        }

        return NThreading::MakeFuture(FailCommit
            ? TMessageStreamResult<TMessageStreamConsumerPosition>::Failure(EMessageStreamStatus::Unauthorized)
            : TMessageStreamResult<TMessageStreamConsumerPosition>::Success({.PartitionId = partitionId, .NextOffset = offset}));
    }

private:
    ui64 GetStartOffset() const { return StartOffset.GetOrElse(FirstRetainedWriteTimeUs ? 10 : (Expired ? 100 : 0)); }
};

// Each facade is bound to one stream; test counters/promises can be shared.
class TBoundHistoryTestClient final : public IMessageStreamClient {
public:
    TBoundHistoryTestClient(TString stream, std::shared_ptr<THistoryTestClient> state)
        : Stream(std::move(stream))
        , State(std::move(state))
    {}

    const TString& GetStream() const override {
        return Stream;
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamDescription>> DescribeStream() override {
        return State->DescribeStream(Stream);
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& consumer, const TMessageStreamDescribeConsumerSettings& settings) override
    {
        return State->DescribeConsumer(Stream, consumer, settings);
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(TMessageStreamPartitionId id) override {
        return State->DescribePartition(Stream, id);
    }
    std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TMessageStreamReadSessionSettings& settings) override {
        return State->CreateReadSession(Stream, settings);
    }
    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerPosition>> CommitPosition(
        TMessageStreamPartitionId id, const TString& consumer, ui64 offset) override
    {
        return State->CommitPosition(Stream, id, consumer, offset);
    }

private:
    const TString Stream;
    const std::shared_ptr<THistoryTestClient> State;
};

class THistoryTestGateway final : public NYql::IPqStaticGateway {
public:
    std::function<std::shared_ptr<THistoryTestClient>(const NYdb::NTopic::TTopicClientSettings&)> Factory;

    NYql::IDeferredPublishClient::TPtr GetDeferredPublishClient(const NYdb::TDriver&, const NYdb::TCommonClientSettings&) override { return {}; }
    std::shared_ptr<IMessageStreamClient> GetTopicClient(const TString& stream, const NYdb::TDriver&, const NYdb::NTopic::TTopicClientSettings& settings) override { return std::make_shared<TBoundHistoryTestClient>(stream, Factory(settings)); }
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
    auto client = std::make_shared<THistoryTestClient>();
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

        runtime.Send(new NActors::IEventHandle(resolver, storage, response.release(), 0, request->Cookie));
    }

    auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
    UNIT_ASSERT_VALUES_EQUAL(result->Cookie, cookie);

    if (!enabled && !explicitOutputStartTime) {
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Result, force, result->Get()->Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "Unsupported checkpointed operator for offset recovery: MultiHoppingCore");
        UNIT_ASSERT_VALUES_EQUAL(client->Describes, 0);
        UNIT_ASSERT_VALUES_EQUAL(client->Reads, 0);

        if (force) {
            UNIT_ASSERT(result->Get()->Plan.at(1).GetSources(0).GetForeignTasksSources().size());
            UNIT_ASSERT(result->Get()->Plan.at(2).GetStateType() == STATE_TYPE_EMPTY);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "FORCE=true");

            for (const auto& issue : result->Get()->Issues) {
                UNIT_ASSERT(issue.GetSeverity() == NYql::TSeverityIds::S_WARNING);
            }
        } else {
            UNIT_ASSERT(result->Get()->Plan.empty());
        }

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
    auto east = std::make_shared<THistoryTestClient>();
    auto west = std::make_shared<THistoryTestClient>();
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

        runtime.Send(new NActors::IEventHandle(resolver, storage, response.release(), 0, request->Cookie));
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

namespace {

struct TRecordingRecoveryProvider : ICheckpointProviderIntegration {
    TVector<TPrepareSource> Prepared;
    ui32 Failure = 0;
    bool Hold = false;
    NThreading::TPromise<NYql::TIssues> Pending = NThreading::NewPromise<NYql::TIssues>();

    TStringBuf GetSourceName() const override { return NYql::PqSource; }
    TStringBuf GetSinkName() const override { return "PqSink"; }
    NThreading::TFuture<NYql::TIssues> CleanupGraphSinks(TVector<TCleanupGraphSink>&&, std::optional<ui64>) override {
        return NThreading::MakeFuture(NYql::TIssues{});
    }
    NThreading::TFuture<NYql::TIssues> PrepareSourceRecovery(TVector<TPrepareSource>&& sources) override {
        UNIT_ASSERT(Prepared.empty());
        Prepared = std::move(sources);

        if (Hold) {
            return Pending.GetFuture();
        }

        if (Failure == 3) {
            ythrow yexception() << "rewind request failed";
        }

        if (Failure == 2) {
            Pending.SetException("rewind future failed");
            return Pending.GetFuture();
        }

        return NThreading::MakeFuture(Failure == 1
            ? NYql::TIssues{NYql::TIssue("Required checkpoint offset has expired by TTL")}
            : NYql::TIssues{});
    }
};

void CheckOffsetPreparationWithPqProvider(bool force, ui32 failure, bool fallback, bool federated = false, bool consumer = true) {
    using namespace NYql::NDq;
    THistoryTestRuntime runtime;
    const auto owner = runtime.AllocateEdgeActor();
    const auto storage = runtime.AllocateEdgeActor();
    TReplayTestGraph old, next;

    if (federated) {
        old.FederatedSource();
        next.FederatedSource(10);
    } else {
        old.Source();
        next.Source(10);
    }

    SetConsumer(*old.Builder.Graph.Mutable(0), "old");
    SetConsumer(*next.Builder.Graph.Mutable(0), consumer ? "new" : "");

    if (fallback) {
        old.Hop(2, 1, 10 * Second, 60 * Second, 600 * Second);
        next.Hop(20, 10, 10 * Second, 30 * Second, 0);
    } else {
        SetAggregationProgram(*old.Builder.Graph.Mutable(0));
        SetAggregationProgram(*next.Builder.Graph.Mutable(0));
    }

    switch (failure) {
        case 3: old.States[1].Sources.front().Data.clear(); break;
        case 4: old.States[1].Sources.front().Data.front().Blob = "corrupt protobuf"; break;
        case 5: old.States[1].Sources.front().Data.front().Version = 0; break;
    }

    TVector<std::shared_ptr<THistoryTestClient>> clients;
    auto gateway = MakeIntrusive<THistoryTestGateway>();
    gateway->Factory = [&](const NYdb::NTopic::TTopicClientSettings& settings) {
        auto client = std::make_shared<THistoryTestClient>();
        client->Consumer = consumer ? "new" : "";
        client->StartOffset = failure == 1 ? 101 : 10;
        client->EndOffset = 200;
        client->CommittedOffset = federated ? 100 : 150;
        client->FirstRetainedWriteTimeUs = 300 * Second; // Too late for replay, but checkpoint offset 100 may remain.
        client->FailCommit = failure == 2;

        if (federated) {
            UNIT_ASSERT(settings.DiscoveryEndpoint_ == "east:2135" || settings.DiscoveryEndpoint_ == "west:2135");
            client->PartitionCount = settings.DiscoveryEndpoint_ == "east:2135" ? 2 : 3;
        }

        clients.push_back(client);
        return client;
    };
    TStateLoadPlanResolverSettings settings;
    settings.StorageProxy = storage;
    settings.GraphId = "graph";
    settings.Checkpoint.SetId(5);
    settings.Checkpoint.SetGeneration(1);
    settings.CoordinatorGeneration = 2;
    settings.Force = force;
    SetRecoveryProvider(settings, runtime, gateway);
    THashSet<ui64> requests;
    auto observer = runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([&](auto& ev) {
        requests.insert(ev->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1); // Fallback reuses the checkpoint read.
    });
    const auto resolver = runtime.Register(CreateStateLoadPlanResolver(old.Params(), next.Params(), settings, 45),
        0, 0, NActors::TMailboxType::Simple, 0, owner);
    const auto request = runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(storage);
    UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds, (fallback ? std::vector<ui64>{1, 2} : std::vector<ui64>{1}));
    auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(settings.Checkpoint, NYql::TIssues{}, 2);
    for (auto id : request->Get()->TaskIds) {
        response->States.push_back(old.States.at(id));
    }

    runtime.Send(new NActors::IEventHandle(resolver, storage, response.release(), 0, request->Cookie));
    const auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
    UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 45);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Result, force || (!failure && !fallback), result->Get()->Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(clients.size(), failure >= 3 ? 0 : (federated ? 2 : (fallback && force ? 2 : 1)));

    if (result->Get()->Result) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Plan.at(10).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);

        if (!fallback) {
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Plan.at(10).GetProgram().GetForeignTaskId(), 1);
        }
    } else {
        UNIT_ASSERT(result->Get()->Plan.empty());
    }

    for (const auto& client : clients) {
        UNIT_ASSERT_VALUES_EQUAL(client->Describes, consumer ? 1 : client->PartitionCount);

        if (federated || !consumer || (failure == 1 && !fallback)) {
            UNIT_ASSERT(client->Commits.empty());
        }
    }

    if (failure == 1 && (!fallback || force)) {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "Required checkpoint offset is unavailable");
    } else if (failure == 2) {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "Cannot rewind consumer");
    } else if (failure >= 3) {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), failure == 3
            ? "Missing PQ source checkpoint data" : "Invalid PQ source checkpoint for recovery");
    }

    if (fallback && failure != 2) {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "Required history has expired");
    }

    if (force && (failure || fallback)) {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), "FORCE=true");

        for (const auto& issue : result->Get()->Issues) {
            UNIT_ASSERT(issue.GetSeverity() == NYql::TSeverityIds::S_WARNING);
        }
    }
}

void CheckOffsetResolver(const TString& previousConsumer, const TString& targetConsumer,
        bool force = false, ui32 failure = 0, bool fallback = false, bool cancel = false, ui32 corrupt = 0, bool enabled = true) {
    using namespace NYql::NDq;
    using namespace NYql::NDqProto::NDqStateLoadPlan;
    THistoryTestRuntime runtime(enabled);
    const auto owner = runtime.AllocateEdgeActor();
    const auto storage = runtime.AllocateEdgeActor();
    TReplayTestGraph old, next;
    old.Source(1, 0, 2);
    old.Source(2, 1, 2);
    next.Source(10, 0, 2);
    next.Source(20, 1, 2);

    for (auto* graph : {&old, &next}) {
        SetAggregationProgram(*graph->Builder.Graph.Mutable(0));
        SetAggregationProgram(*graph->Builder.Graph.Mutable(1), "/Root/second");
    }

    SetConsumer(*old.Builder.Graph.Mutable(0), previousConsumer);
    SetConsumer(*next.Builder.Graph.Mutable(0), targetConsumer);

    if (fallback) {
        old.Hop(3, 1, 10 * Second, 30 * Second, 600 * Second);
    }

    if (corrupt == 1) {
        old.States[1].Sources.clear();
    }

    const bool changed = previousConsumer != targetConsumer;
    auto provider = MakeIntrusive<TRecordingRecoveryProvider>();
    provider->Failure = failure;
    provider->Hold = cancel;
    TStateLoadPlanResolverSettings settings;
    settings.StorageProxy = storage;
    settings.GraphId = "graph";
    settings.Checkpoint.SetId(5);
    settings.Checkpoint.SetGeneration(1);
    settings.CoordinatorGeneration = 2;
    settings.Force = force;
    settings.ProviderIntegrations["pq"] = provider;
    THashSet<ui64> readCookies;
    auto readObserver = runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([&](auto& ev) {
        UNIT_ASSERT(changed || fallback);
        readCookies.insert(ev->Cookie);
        // Edge events can be observed repeatedly before GrabEdgeEvent removes them.
        UNIT_ASSERT_VALUES_EQUAL(readCookies.size(), 1); // Fallback must reuse the replay read.
    });
    THashSet<const NActors::IEventHandle*> replies;
    auto replyObserver = runtime.AddObserver<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>([&](auto& ev) {
        UNIT_ASSERT(!cancel);
        replies.insert(ev.Get());
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
    });
    const auto resolver = runtime.Register(CreateStateLoadPlanResolver(old.Params(), next.Params(), settings, 123),
        0, 0, NActors::TMailboxType::Simple, 0, owner);

    if (changed || fallback) {
        auto request = runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(storage);
        const std::vector<ui64> expected = fallback ? std::vector<ui64>{1, 2, 3} : std::vector<ui64>{1};
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds, expected);
        auto checkpoint = settings.Checkpoint;

        if (corrupt == 10) {
            checkpoint.SetId(6);
        }

        auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(checkpoint,
            corrupt == 11 ? NYql::TIssues{NYql::TIssue("Checkpoint storage read failed")} : NYql::TIssues{}, corrupt == 9 ? 3 : 2);
        for (auto id : request->Get()->TaskIds) {
            response->States.push_back(old.States.at(id));
        }

        if (corrupt == 8) {
            response->States.clear();
        }

        runtime.Send(new NActors::IEventHandle(resolver, storage, response.release(), 0, request->Cookie));
    }

    if (cancel) {
        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&] { return !provider->Prepared.empty(); };
        runtime.DispatchEvents(options);
        // Repeated poison must not call PassAway twice during coroutine cancellation.
        runtime.Send(new NActors::IEventHandle(resolver, owner, new NActors::TEvents::TEvPoison));
        runtime.Send(new NActors::IEventHandle(resolver, owner, new NActors::TEvents::TEvPoison));
        runtime.Send(new NActors::IEventHandle(owner, owner, new NActors::TEvents::TEvWakeup));
        runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(owner);
        provider->Pending.SetValue(NYql::TIssues{}); // Late callback after cancellation is harmless.
        runtime.Send(new NActors::IEventHandle(owner, owner, new NActors::TEvents::TEvWakeup));
        runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(owner);
        return;
    }

    auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
    UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 123);
    const bool success = !corrupt && (!failure || force);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Result, success, result->Get()->Issues.ToString());

    if (success) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Plan.at(10).GetProgram().GetForeignTaskId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Plan.at(20).GetProgram().GetForeignTaskId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Plan.at(10).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
    } else {
        UNIT_ASSERT(result->Get()->Plan.empty());
    }

    if (changed && !corrupt) {
        UNIT_ASSERT_VALUES_EQUAL(provider->Prepared.size(), 1);
        const auto& tasks = provider->Prepared.front().Tasks;
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks.front().TaskId, 10);
        NYql::NPq::NProto::TDqPqTopicSourceState source;
        UNIT_ASSERT(source.ParseFromString(tasks.front().State.Data.front().Blob));
        UNIT_ASSERT_VALUES_EQUAL(source.GetPartitions(0).GetOffset(), 100);
    } else {
        UNIT_ASSERT(provider->Prepared.empty());
    }

    if (failure && !corrupt) {
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Issues.ToString(), failure == 1 ? "expired by TTL" : "rewind");
    }

    if (force && success) {
        const auto warning = [](auto&& self, const NYql::TIssue& issue) -> void {
            UNIT_ASSERT(issue.GetSeverity() == NYql::TSeverityIds::S_WARNING);

            for (const auto& sub : issue.GetSubIssues()) {
                self(self, *sub);
            }
        };

        for (const auto& issue : result->Get()->Issues) {
            warning(warning, issue);
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TStreamingOffsetRecoveryResolver) {
    Y_UNIT_TEST(DisabledReplayFlagUsesCommonOffsetRecovery) {
        for (bool force : {false, true}) {
            CheckOffsetResolver("consumer", "consumer", force, 0, false, false, 0, /* enabled */ false);
            CheckOffsetResolver("old", "new", force, 0, false, false, 0, /* enabled */ false);
            CheckOffsetResolver("old", "new", force, 1, false, false, 0, /* enabled */ false);
        }
    }

    Y_UNIT_TEST(ChangedSourceTasksDeduplicateCheckpointReads) {
        using namespace NYql::NDq;
        THistoryTestRuntime runtime;
        const auto owner = runtime.AllocateEdgeActor();
        const auto storage = runtime.AllocateEdgeActor();
        TReplayTestGraph old, next;
        old.Source(1, 0, 2, true, 1);
        next.Source(10, 0, 2);
        next.Source(20, 1, 2);

        for (auto& task : next.Builder.Graph) {
            task.SetStageId(10);
            SetConsumer(task, "new");
        }

        auto provider = MakeIntrusive<TRecordingRecoveryProvider>();
        TStateLoadPlanResolverSettings settings;
        settings.StorageProxy = storage;
        settings.CoordinatorGeneration = 2;
        settings.ProviderIntegrations["pq"] = provider;
        const auto resolver = runtime.Register(CreateStateLoadPlanResolver(old.Params(), next.Params(), settings, 77),
            0, 0, NActors::TMailboxType::Simple, 0, owner);
        const auto request = runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(storage);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds, (std::vector<ui64>{1}));
        auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(settings.Checkpoint, NYql::TIssues{}, 2);
        response->States.push_back(old.States.at(1));
        runtime.Send(new NActors::IEventHandle(resolver, storage, response.release(), 0, request->Cookie));
        const auto result = runtime.GrabEdgeEvent<TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult>(owner);
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 77);
        UNIT_ASSERT_C(result->Get()->Result, result->Get()->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(provider->Prepared.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(provider->Prepared.front().Tasks.size(), 2);

        for (const auto& task : provider->Prepared.front().Tasks) {
            UNIT_ASSERT_VALUES_EQUAL(task.State.Data.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Plan.at(task.TaskId).GetSources(0).GetForeignTasksSources(0).GetTaskId(), 1);
        }
    }

    Y_UNIT_TEST(ChangedConsumerUsesPqRetentionAndRewindValidation) {
        for (bool force : {false, true}) {
            for (ui32 failure : {0, 1, 2}) {
                CheckOffsetPreparationWithPqProvider(force, failure, false);
            }
        }
    }
    Y_UNIT_TEST(ExpiredReplayFallsBackToCheckpointOffsetsForChangedConsumer) {
        for (bool force : {false, true}) {
            for (ui32 failure : {0, 1, 2}) {
                CheckOffsetPreparationWithPqProvider(force, failure, true);
            }
        }
    }
    Y_UNIT_TEST(FederatedChangedOrRemovedConsumerValidatesEveryCluster) {
        for (bool consumer : {false, true}) {
            for (bool force : {false, true}) {
                for (ui32 failure : {0, 1}) {
                    CheckOffsetPreparationWithPqProvider(force, failure, false, true, consumer);
                }
            }
        }
    }
    Y_UNIT_TEST(CheckpointReadErrorsRemainFatalWithForce) {
        for (bool force : {false, true}) {
            for (ui32 failure : {1, 8, 9, 10, 11}) {
                CheckOffsetResolver("old", "new", force, 0, false, false, failure);
            }
        }
    }
    Y_UNIT_TEST(UnchangedConsumersNeedNoReadsOrPreparation) {
        CheckOffsetResolver("consumer", "consumer");
    }
    Y_UNIT_TEST(ChangedAndAddedOrRemovedConsumersPrepareOnlyRequiredTasks) {
        CheckOffsetResolver("old", "new");
        CheckOffsetResolver("", "new");
        CheckOffsetResolver("old", "");
    }
    Y_UNIT_TEST(PreparationFailuresRequireForceAndKeepCompatiblePrograms) {
        for (ui32 failure : {1, 2, 3}) {
            for (bool force : {false, true}) {
                CheckOffsetResolver("old", "new", force, failure);
            }
        }
    }
    Y_UNIT_TEST(ReplayFallbackReusesStatesAndPreparesOnlyChangedConsumers) {
        CheckOffsetResolver("old", "old", true, 0, true);

        for (ui32 failure : {0, 1, 2, 3}) {
            CheckOffsetResolver("old", "new", true, failure, true);
        }
    }
    Y_UNIT_TEST(CorruptSourceCheckpointPreparationRequiresForce) {
        for (bool force : {false, true}) {
            for (ui32 failure : {3, 4, 5}) {
                CheckOffsetPreparationWithPqProvider(force, failure, /* fallback */ false);
            }
        }
    }
    Y_UNIT_TEST(CancellationWhileProviderFutureIsPending) {
        CheckOffsetResolver("old", "new", false, 0, false, true);
    }
}

Y_UNIT_TEST_SUITE(THistoryReplayResolver) {
    Y_UNIT_TEST(DisabledReplayWithoutProvidersUsesOffsetRecovery) {
        THistoryTestRuntime runtime(/* enabled */ false);
        const auto owner = runtime.AllocateEdgeActor();
        auto observer = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvGetTaskState>([](auto&) {
            UNIT_FAIL("Unchanged consumers restore offsets in the compute actors");
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
    Y_UNIT_TEST(DisabledReplayRejectsHoppingWithoutForce) { CheckResolver(false, true, false); }
    Y_UNIT_TEST(DisabledReplayDiscardsHoppingWithForce) { CheckResolver(true, true, false); }
    Y_UNIT_TEST(ForceStillAttemptsReplay) { CheckResolver(true, false); }
    Y_UNIT_TEST(RequiredHistoryRetainedAfterOlderDataExpired) { CheckResolver(false, false, true, 250 * Second); }
    Y_UNIT_TEST(RequiredHistoryIsOlderThanFirstRetainedMessage) { CheckResolver(false, true, true, 280 * Second); }
    Y_UNIT_TEST(FirstRetainedMessageAtReplayBoundIsAvailable) { CheckResolver(false, false, true, 270 * Second); }
    Y_UNIT_TEST(ExpiredHistoryFailsWithoutForce) { CheckResolver(false, true); }
    Y_UNIT_TEST(ExpiredHistoryFallsBackToOffsets) { CheckResolver(true, true); }
}

} // namespace NFq
