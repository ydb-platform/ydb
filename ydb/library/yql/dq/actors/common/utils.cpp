#include "utils.h"

namespace NYql::NDq {

static bool IsInfiniteSourceType(const TString& sourceType) {
    return sourceType == "PqSource";
}

NYql::NDqProto::ECheckpointingMode GetTaskCheckpointingMode(const NYql::NDq::TDqTaskSettings& task) {
    for (const auto& input : task.GetInputs()) {
        if (const TString& srcType = input.GetSource().GetType(); srcType && IsInfiniteSourceType(srcType)) {
            return NYql::NDqProto::CHECKPOINTING_MODE_DEFAULT;
        }
        for (const auto& channel : input.GetChannels()) {
            if (channel.GetCheckpointingMode() != NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
                return NYql::NDqProto::CHECKPOINTING_MODE_DEFAULT;
            }
        }
    }
    return NYql::NDqProto::CHECKPOINTING_MODE_DISABLED;
}


bool IsIngress(const NYql::NDq::TDqTaskSettings& task) {
    // No inputs at all or the only inputs are sources.
    for (const auto& input : task.GetInputs()) {
        if (!input.HasSource()) {
            return false;
        }
    }
    return true;
}

bool IsEgress(const NYql::NDq::TDqTaskSettings& task) {
    for (const auto& output : task.GetOutputs()) {
        if (output.HasSink()) {
            return true;
        }
    }
    return false;
}

bool HasState(const NYql::NDq::TDqTaskSettings& task) {
    Y_UNUSED(task);
    return true;
}

} // namespace NYql::NDq
