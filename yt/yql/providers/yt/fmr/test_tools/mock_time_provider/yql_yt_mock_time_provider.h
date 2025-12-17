#pragma once

#include <library/cpp/time_provider/time_provider.h>
#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NYql::NFmr {

class TMockTimeProvider : public ITimeProvider {
public:
    explicit TMockTimeProvider(TDuration sleepTime = TDuration::MilliSeconds(50));

    TInstant Now() override;

    void Advance(TDuration duration, TMaybe<TDuration> sleepTime = Nothing());

private:
    TInstant CurrentTime_;
    TDuration SleepTime_;
};

} // namespace NYql::NFmr

