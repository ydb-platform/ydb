#pragma once

#include <ydb/library/actors/core/monotonic_provider.h>

namespace NKikimr::NDbStatTest {

    class TManualMonotonicTimeProvider : public NMonotonic::IMonotonicTimeProvider {
    public:
        TMonotonic Now() override {
            ++Calls;
            return CurrentTime;
        }

        void Advance(TDuration duration) {
            CurrentTime += duration;
        }

        ui32 GetCalls() const {
            return Calls;
        }

    private:
        TMonotonic CurrentTime = TMonotonic::Zero();
        ui32 Calls = 0;
    };

} // namespace NKikimr::NDbStatTest
