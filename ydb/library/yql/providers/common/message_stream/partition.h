#pragma once
#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <optional>

namespace NFq::NMessageStream {
struct TPartitionProgress {
    std::optional<ui64> Offset; // Next unread offset.
    std::optional<ui64> EndOffset; // Exclusive snapshot boundary.
    TMaybe<TInstant> EndWriteTime;
    TInstant LastMessageWriteTime;

    bool IsFinishedInTableMode() const {
        return (EndOffset && (*EndOffset == 0 || (Offset && *Offset >= *EndOffset)))
            || (EndWriteTime && LastMessageWriteTime >= *EndWriteTime);
    }
};
}
