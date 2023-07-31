#pragma once

namespace NYql {
    namespace NPureCalc {
        enum class EProcessorMode {
            PullList,
            PullStream,
            PushStream
        };
    }
}
