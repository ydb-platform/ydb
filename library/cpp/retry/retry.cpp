#include "retry.h"

#include <util/stream/output.h>

namespace {
class TRetryOptionsWithRetCodePolicy : public IRetryPolicy<bool> {
public:
    explicit TRetryOptionsWithRetCodePolicy(const TRetryOptions& opts)
        : Opts(opts)
    {
    }

    class TRetryState : public IRetryState {
    public:
        explicit TRetryState(const TRetryOptions& opts)
            : Opts(opts)
        {
        }

        TMaybe<TDuration> GetNextRetryDelay(bool ret) override {
            if (ret || Attempt == Opts.RetryCount) {
                return Nothing();
            }
            return Opts.GetTimeToSleep(Attempt++);
        }

    private:
        const TRetryOptions Opts;
        size_t Attempt = 0;
    };

    IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TRetryState>(Opts);
    }

private:
    const TRetryOptions Opts;
};
} // namespace

bool DoWithRetryOnRetCode(std::function<bool()> func, TRetryOptions retryOptions) {
    return DoWithRetryOnRetCode<bool>(std::move(func), std::make_shared<TRetryOptionsWithRetCodePolicy>(retryOptions), retryOptions.SleepFunction);
}

TRetryOptions MakeRetryOptions(const NRetry::TRetryOptionsPB& retryOptions) {
    return TRetryOptions(retryOptions.GetMaxTries(),
                         TDuration::MilliSeconds(retryOptions.GetInitialSleepMs()),
                         TDuration::MilliSeconds(retryOptions.GetRandomDeltaMs()),
                         TDuration::MilliSeconds(retryOptions.GetSleepIncrementMs()),
                         TDuration::MilliSeconds(retryOptions.GetExponentalMultiplierMs()));
}
