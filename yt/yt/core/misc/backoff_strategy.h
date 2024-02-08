#pragma once

#include "config.h"
#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Implements exponential backoffs with jitter.
//! Expected use after restart is Next() -> GetBackoff() (not the other way around).
class TBackoffStrategy
{
public:
    explicit TBackoffStrategy(const TExponentialBackoffOptions& options);

    void Restart();
    bool Next();

    int GetInvocationIndex() const;
    int GetInvocationCount() const;

    //! backoffstrategy.GetBackoff return value changes only after
    //! Next or Restart calls.
    TDuration GetBackoff() const;

    void UpdateOptions(const TExponentialBackoffOptions& newOptions);

private:
    TExponentialBackoffOptions Options_;

    TDuration BackoffWithJitter_;
    TDuration Backoff_;
    int InvocationIndex_;

    void ApplyJitter();
};

////////////////////////////////////////////////////////////////////////////////

//! Tracks if backoff time has passed since the last time invocation was recorded.
class TRelativeConstantBackoffStrategy
{
public:
    //! Technically, calls to IsOverBackoff and GetDeadline only make sense
    //! if they are made after the call to RecordInvocation.
    //! In order to simplify the semantics, we treat such calls to
    //! IsOverBackoff and GetDeadline as if there is always a call to
    //! RecordInvocation made in the infinitely long past.
    //! Formally, we return TInstant::Zero() in GetDeadline
    //! which compares <= with any other number.
    TRelativeConstantBackoffStrategy(TConstantBackoffOptions options);

    bool IsOverBackoff(TInstant now = TInstant::Now()) const;

    void RecordInvocation();

    //! If now is over backoff deadline, makes RecordInvocation as if
    //! it was called exactly at the moment now.
    bool RecordInvocationIfOverBackoff(TInstant now = TInstant::Now());

    TInstant GetLastInvocationTime() const;
    TInstant GetBackoffDeadline() const;

    //! Resets state as if it was freshly constructed.
    void Restart();

    //! Does not reset state.
    void UpdateOptions(TConstantBackoffOptions newOptions);

private:
    TConstantBackoffOptions Options_;

    TDuration BackoffWithJitter_;
    TInstant LastInvocationTime_;

    void DoRecordInvocation(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
