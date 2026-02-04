#pragma once

#include <util/generic/yexception.h>

namespace NKikimr {

// This exception can separate code line and file name from the error message
struct TCodeLineException : public yexception {
    TSourceLocation SourceLocation;
    mutable TString Message;
    ui32 Code;

    explicit TCodeLineException(ui32 code);

    TCodeLineException(const TSourceLocation& sl, const TCodeLineException& t);

    const char* what() const noexcept override;

    const char* GetRawMessage() const;
};

TCodeLineException operator+(const TSourceLocation& sl, TCodeLineException&& t);

#define Y_ENSURE_CODELINE(CONDITION, CODE, ...)                                                                                                                                                   \
    do {                                                                                                                                                                                          \
        static_assert(!std::is_array_v<std::remove_cvref_t<decltype(CONDITION)>>, "An array type always evaluates to true in a condition; this is likely an error in the condition expression."); \
        if (Y_UNLIKELY(!(CONDITION))) {                                                                                                                                                           \
            ythrow ::NKikimr::TCodeLineException(CODE) << __VA_ARGS__;                                                                                                                            \
        }                                                                                                                                                                                         \
    } while (0)

} // namespace NKikimr
