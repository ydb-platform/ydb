#pragma once

#include "format_string.h"
#include "macros.h"

#include <library/cpp/yson_pull/exceptions.h>
#include <library/cpp/yson_pull/position_info.h>

namespace NYsonPull {
    namespace NDetail {
        template <typename... Args>
        [[noreturn]] ATTRIBUTE(noinline, cold)
        void fail(
            const TPositionInfo& info,
            Args&&... args) {
            auto formatted_message = format_string(std::forward<Args>(args)...);
            throw NException::TBadInput(formatted_message, info);
        }
    }
}
