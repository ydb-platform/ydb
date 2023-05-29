#include "utils.h"

namespace NFq {

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

} // namespace NFq
