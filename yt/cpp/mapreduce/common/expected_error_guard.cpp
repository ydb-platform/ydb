#include "expected_error_guard.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TExpectedErrorGuard::TExpectedErrorGuard(const TErrorPredicate& predicate)
    : PreviousPredicate_(std::exchange(GetCurrentPredicate(), predicate))
{ }

TExpectedErrorGuard::~TExpectedErrorGuard()
{
    Release();
}

bool TExpectedErrorGuard::IsErrorExpected(const TErrorResponse& error)
{
    auto predicate = GetCurrentPredicate();
    return predicate && predicate(error);
}

void TExpectedErrorGuard::Release()
{
    if (!Released_) {
        GetCurrentPredicate() = PreviousPredicate_;
        Released_ = true;
    }
}

TExpectedErrorGuard::TErrorPredicate& TExpectedErrorGuard::GetCurrentPredicate()
{
    thread_local TErrorPredicate CurrentPredicate;
    return CurrentPredicate;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
