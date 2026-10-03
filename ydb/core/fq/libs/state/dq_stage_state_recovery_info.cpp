#include "dq_stage_state_recovery_info.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NFq {

namespace {

using namespace NKikimr::NMiniKQL;

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

TStageStateRecoveryInfo::TStageStateRecoveryInfo(const ui32 runtimeVersion, const TString& program, TStageStateRecoveryContext& context) {
    YQL_ENSURE(runtimeVersion == NYql::NDqProto::RUNTIME_VERSION_YQL_1_0, "Unsupported program runtime for history replay");

    const auto root = DeserializeRuntimeNode(program, context.Env);
    TExploringNodeVisitor explorer;
    explorer.Walk(root.GetNode(), context.Env);

    for (const auto* node : explorer.GetNodes()) {
        if (!node->GetType()->IsCallable()) {
            continue;
        }

        const auto& callable = static_cast<const TCallable&>(*node);
        const TStringBuf name = callable.GetType()->GetName();
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
