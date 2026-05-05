#pragma once

#include <yt/cpp/mapreduce/interface/errors.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief RAII guard that sets a predicate for identifying "expected" errors.
///
/// Can be used for logging: If predicate is set, log errors at Info level instead of Error.
class TExpectedErrorGuard {
public:
    using TErrorPredicate = std::function<bool(const TErrorResponse&)>;

    explicit TExpectedErrorGuard(const TErrorPredicate& predicate);
    ~TExpectedErrorGuard();

    TExpectedErrorGuard(const TExpectedErrorGuard&) = delete;
    TExpectedErrorGuard& operator=(const TExpectedErrorGuard&) = delete;

    static bool IsErrorExpected(const TErrorResponse& error);

    /// @brief Release current predicate, restore previously saved predicate.
    void Release();

private:
    TErrorPredicate PreviousPredicate_;
    bool Released_ = false;

    static TErrorPredicate& GetCurrentPredicate();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
