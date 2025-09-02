#pragma once

namespace NYql {
    enum class EJsonQueryWrap {
        NoWrap = 0,
        Wrap = 1,
        ConditionalWrap = 2,
    };

    enum class EJsonQueryHandler {
        Null = 0,
        Error = 1,
        EmptyArray = 2,
        EmptyObject = 3,
    };

    enum class EJsonValueHandlerMode {
        Error = 0,
        DefaultValue = 1,
    };
}
