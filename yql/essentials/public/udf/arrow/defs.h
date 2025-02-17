#pragma once
#include <arrow/status.h>
#include <arrow/result.h>
#include <util/generic/yexception.h>

#define ARROW_CHECK_STATUS(s, op, ...)                                                 \
    if (!s.ok()) {                                                                     \
        ythrow yexception() << "Operation failed: [" << #op << "]\n"                   \
            << "" __VA_ARGS__ << ": [" << s.ToString() << "]";                         \
    }

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

#define ARROW_DEBUG_CHECK_DATUM_TYPES(expected, got) do {                              \
    Y_DEBUG_ABORT_UNLESS((expected) == (got), "Bad datum type: %s expected, %s got",   \
                         (expected).ToString().c_str(), (got).ToString().c_str());     \
} while(false)
