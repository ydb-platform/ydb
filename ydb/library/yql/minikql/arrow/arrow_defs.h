#pragma once

#include <ydb/library/yql/minikql/defs.h>

#define ARROW_CHECK_STATUS(s, op, ...)                                                 \
    MKQL_ENSURE(s.ok(), "Operation failed: [" << #op << "]\n"                          \
        << "" __VA_ARGS__ << ": [" << s.ToString() << "]")                             \

#define ARROW_OK_S(op, ...)                                                            \
do {                                                                                   \
    ::arrow::Status _s = (op);                                                         \
    ARROW_CHECK_STATUS(_s, op, __VA_ARGS__);                                           \
  } while (false)

#define ARROW_OK(op)           ARROW_OK_S(op, "Bad status")

#define ARROW_RESULT_S(op, ...)                                                        \
    [&]() {                                                                            \
        auto result = (op);                                                            \
        ARROW_CHECK_STATUS(result.status(), op, __VA_ARGS__);                          \
        return std::move(result).ValueOrDie();                                         \
    }()

#define ARROW_RESULT(op)       ARROW_RESULT_S(op, "Bad status")
