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

        std::optional<TDuration> GetNextRetryDelay(bool ret) override {
            if (ret || Attempt == Opts.RetryCount) {
                return std::nullopt;
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
    return TRetryOptions(retryOptions.maxtries(),
                         TDuration::MilliSeconds(retryOptions.initialsleepms()),
                         TDuration::MilliSeconds(retryOptions.randomdeltams()),
                         TDuration::MilliSeconds(retryOptions.sleepincrementms()),
                         TDuration::MilliSeconds(retryOptions.exponentalmultiplierms()));
}
