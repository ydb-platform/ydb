#include "dq_stage_state_recovery_info.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>

#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <algorithm>

namespace NFq {

namespace {

using namespace NKikimr::NMiniKQL;

bool IsStatefulOperator(const TStringBuf name) {
    // Exact runtime callable names from mkql_factory.cpp, dq_tasks_runner.cpp
    // and kqp_compute.cpp. Universal accumulators are conservative positives:
    // their lambdas may retain state even without a combiner.
    return IsIn({
        "CombineCore", "GroupingCore", "Condense", "Condense1",
        "WideCombiner", "WideLastCombiner", "WideLastCombinerWithSpilling", "WideCondense1",
        "BlockCombineAll", "BlockCombineHashed", "BlockMergeFinalizeHashed", "BlockMergeManyFinalizeHashed",
        "HoppingCore", "MultiHoppingCore", "KqpStreamingAggregation",
        "Fold", "Fold1", "Squeeze", "Squeeze1", "ChainMap", "Chain1Map", "WideChain1Map",
        "Chopper", "WideChopper", "TimeOrderRecover", "MatchRecognizeCore"
    }, name);
}

ui64 ParseHoppingInterval(const TCallable& callable, const ui32 index) {
    auto input = callable.GetInput(index);
    if (input.IsImmediate() && input.GetStaticType()->IsOptional()) {
        const auto* optional = AS_VALUE(TOptionalLiteral, input);
        YQL_ENSURE(optional->HasItem(), "Hopping interval cannot be empty");
        input = optional->GetItem();
    }

    YQL_ENSURE(input.IsImmediate() && input.GetStaticType()->IsData(), "History replay requires constant hopping intervals");
    const auto value = AS_VALUE(TDataLiteral, input)->AsValue().Get<i64>();
    YQL_ENSURE(value > 0, "Invalid hopping interval");
    return static_cast<ui64>(value);
}

void ValidateHoppingWatermarkPolicy(const TCallable& callable, const ui32 index) {
    auto policy = callable.GetInput(index);
    if (policy.IsImmediate() && policy.GetStaticType()->IsOptional()) {
        const auto* optional = AS_VALUE(TOptionalLiteral, policy);
        if (!optional->HasItem()) {
            return;
        }

        policy = optional->GetItem();
    }

    if (policy.GetStaticType()->IsVoid()) {
        return;
    }

    YQL_ENSURE(policy.IsImmediate() && policy.GetStaticType()->IsData(), "History replay requires constant hopping watermark policies");
    YQL_ENSURE(AS_VALUE(TDataLiteral, policy)->AsValue().Get<ui32>() != static_cast<ui32>(NYql::NHoppingWindow::EPolicy::Adjust), "History replay does not support the adjust hopping watermark policy");
}

} // anonymous namespace

//// THoppingRecoveryState

THoppingRecoveryState THoppingRecoveryState::Read(const TStringBuf state) {
    TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);
    YQL_ENSURE(in.GetStateVersion() == STATE_VERSION, "Hopping checkpoint has no recovery metadata");
    THoppingRecoveryState result;
    in(result.MinWindowStartIndex);
    in(result.KeysCount);
    return result;
}

TString THoppingRecoveryState::MakeRecoveryState(const ui64 minWindowStartIndex) {
    TString result;
    WriteUi32(result, static_cast<ui32>(EMkqlStateType::SIMPLE_BLOB));
    WriteUi32(result, STATE_VERSION);
    WriteUi64(result, minWindowStartIndex);
    WriteUi32(result, 0); // No keys or aggregate values are transferred.
    WriteBool(result, false); // Finished.
    return result;
}

//// TGraphStateContext

TGraphStateContext::TGraphStateContext() {
    const auto guard = Guard(Alloc);
    Env = std::make_unique<TTypeEnvironment>(Alloc);
}

TGraphStateContext::~TGraphStateContext() {
    const auto guard = Guard(Alloc);
    Env.reset();
}

const TTypeEnvironment& TGraphStateContext::GetTypeEnvironment() const {
    return *Env;
}

TGuard<TScopedAlloc> TGraphStateContext::BindAllocator() const {
    return Env->BindAllocator();
}

//// TGraphStateInfo

TGraphStateInfo::TGraphStateInfo(const NProto::TGraphParams& graph, const TGraphStateContext& context)
    : Graph(&graph)
    , Context(&context)
{
    const auto guard = Context->BindAllocator();
    const auto& env = Context->GetTypeEnvironment();

    THashMap<ui32, size_t> stages;
    THashMap<ui32, TStringBuf> programs;
    THashSet<ui64> taskIds;
    stages.reserve(graph.StageProgramSize());
    programs.reserve(graph.StageProgramSize());
    taskIds.reserve(graph.TasksSize());

    for (const auto& task : graph.GetTasks()) {
        YQL_ENSURE(taskIds.insert(task.GetId()).second, "Duplicate task ID in recovery graph: " << task.GetId());

        if (NYql::NDq::GetTaskCheckpointingMode(task) == NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
            continue;
        }

        const TString* program = &task.GetProgram().GetRaw();
        if (program->empty()) {
            const auto it = graph.GetStageProgram().find(task.GetStageId());
            YQL_ENSURE(it != graph.GetStageProgram().end(), "Missing program for stage " << task.GetStageId());
            program = &it->second;
        }

        const auto [it, inserted] = stages.emplace(task.GetStageId(), Stages.size());
        if (inserted) {
            auto& stage = Stages.emplace_back();
            stage.StageId = task.GetStageId();
            stage.RuntimeVersion = task.GetProgram().GetRuntimeVersion();
            YQL_ENSURE(stage.RuntimeVersion == NYql::NDqProto::RUNTIME_VERSION_YQL_1_0, "Unsupported program runtime for recovery");

            programs.emplace(stage.StageId, *program);

            const auto root = DeserializeRuntimeNode(*program, env);
            if (root.IsImmediate() && root.GetStaticType()->IsStruct()) {
                if (const auto* programStruct = AS_VALUE(TStructLiteral, root); const auto index = programStruct->GetType()->FindMemberIndex("Program")) {
                    if (const auto* type = programStruct->GetValue(*index).GetStaticType(); type->IsStream()) {
                        if (const auto* item = static_cast<const TStreamType*>(type)->GetItemType(); item->IsVariant()) {
                            const auto* underlying = static_cast<const TVariantType*>(item)->GetUnderlyingType();
                            YQL_ENSURE(underlying->IsTuple(), "Expected tuple of stage output types");

                            const auto* outputs = static_cast<const TTupleType*>(underlying);
                            stage.OutputTypes.reserve(outputs->GetElementsCount());
                            for (ui32 i = 0; i < outputs->GetElementsCount(); ++i) {
                                stage.OutputTypes.emplace_back(outputs->GetElementType(i));
                            }
                        } else {
                            stage.OutputTypes.emplace_back(item);
                        }
                    }
                }
            }

            TExploringNodeVisitor explorer;
            explorer.Walk(root.GetNode(), env);
            for (const auto* node : explorer.GetNodes()) {
                if (!node->GetType()->IsCallable()) {
                    continue;
                }

                const auto& callable = static_cast<const TCallable&>(*node);
                const TStringBuf name = callable.GetType()->GetName();
                if (IsStatefulOperator(name)) {
                    stage.StatefulOperators.push_back(&callable);
                } else if (name == "DqWatermarkGenerator") {
                    stage.HasWatermarkGenerator = true;
                }
            }
        }

        auto& stage = Stages[it->second];
        YQL_ENSURE(stage.RuntimeVersion == task.GetProgram().GetRuntimeVersion() && programs.at(stage.StageId) == *program, "Inconsistent programs for stage " << stage.StageId);
        stage.Tasks.push_back(&task);
    }

    for (auto& stage : Stages) {
        std::sort(stage.Tasks.begin(), stage.Tasks.end(), [](const auto* lhs, const auto* rhs) { return lhs->GetId() < rhs->GetId(); });
    }

    std::sort(Stages.begin(), Stages.end(), [](const auto& lhs, const auto& rhs) { return lhs.StageId < rhs.StageId; });
}

bool TGraphStateInfo::HasHopping() const {
    for (const auto& stage : Stages) {
        for (const auto* callable : stage.StatefulOperators) {
            if (callable->GetType()->GetName() == "MultiHoppingCore") {
                return true;
            }
        }
    }
    return false;
}

TGuard<TScopedAlloc> TGraphStateInfo::BindAllocator() const {
    return Context->BindAllocator();
}

//// TStageStateRecoveryInfo

TStageStateRecoveryInfo::TStageStateRecoveryInfo(const TStageStateInfo& stage, EMode mode)
    : HasWatermarkGenerator(stage.HasWatermarkGenerator)
{
    for (const auto* node : stage.StatefulOperators) {
        const auto& callable = *node;
        const TStringBuf name = callable.GetType()->GetName();
        HasState |= IsStatefulOperator(name);
        HasWatermarkGenerator |= name == "DqWatermarkGenerator";
        if (mode != EMode::HistoryReplay) {
            continue;
        }
        if (name == "MultiHoppingCore") {
            YQL_ENSURE(!Hopping, "History replay supports at most one hopping operator per stage");
            Y_VALIDATE(callable.GetInputsCount() > 20, "Expected at least 21 hopping operator inputs");

            auto& hopping = Hopping.ConstructInPlace();
            hopping.HopTimeUs = ParseHoppingInterval(callable, 16);
            hopping.WindowSizeUs = ParseHoppingInterval(callable, 17);
            YQL_ENSURE(hopping.WindowSizeUs >= hopping.HopTimeUs && hopping.WindowSizeUs % hopping.HopTimeUs == 0, "Invalid hopping window");

            const auto watermarkMode = callable.GetInput(20);
            YQL_ENSURE(watermarkMode.IsImmediate() && watermarkMode.GetStaticType()->IsData() && AS_VALUE(TDataLiteral, watermarkMode)->AsValue().Get<bool>(), "History replay requires watermark-driven hopping");

            YQL_ENSURE(callable.GetInputsCount() > 25, "Hopping minimum window start checking is not enabled");
            ValidateHoppingWatermarkPolicy(callable, 23);
            ValidateHoppingWatermarkPolicy(callable, 24);

            const auto checkMinWindowStart = callable.GetInput(25);
            Y_VALIDATE(checkMinWindowStart.IsImmediate() && checkMinWindowStart.GetStaticType()->IsData(), "Expected a literal hopping minimum window start flag");
            YQL_ENSURE(AS_VALUE(TDataLiteral, checkMinWindowStart)->AsValue().Get<bool>(), "Hopping minimum window start checking is not enabled");
        } else if (name == "DqWatermarkGenerator") {
            HasWatermarkGenerator = true;
        } else {
            YQL_ENSURE(!IsIn({
                "MatchRecognizeCore", "TimeOrderRecover", "KqpStreamingAggregation"
            }, name), "Unsupported checkpointed operator for history replay: " << name);
        }
    }
}

ui64 TStageStateRecoveryInfo::InputStartForOutput(const ui64 outputStartTimeUs) const {
    if (!Hopping) {
        return outputStartTimeUs;
    }

    const ui64 hop = Hopping->HopTimeUs;
    const ui64 remainder = outputStartTimeUs % hop;
    const ui64 end = outputStartTimeUs - remainder;
    const ui64 recoveryWindow = Hopping->WindowSizeUs - (remainder ? hop : 0);
    YQL_ENSURE(end >= recoveryWindow, "Hopping recovery time underflow: required input precedes timestamp zero");
    return end - recoveryWindow;
}

} // namespace NFq
