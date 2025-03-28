#pragma once

#include <cstddef>
#include <ydb/library/accessor/accessor.h>

namespace NYdb {
namespace NConsoleClient {

class TProgressIndication {
public:
    struct TCurrentStats {
        ui64 ReadRows = 0;
        ui64 ReadBytes = 0;
    };
    explicit TProgressIndication();

    ~TProgressIndication();

    void UpdateProgress(const TCurrentStats& stats);
    void SetDurationUs(ui64 durationUs);
    void Render();
    void Finish();

private:

    TCurrentStats CurrentStats;
    bool Finished = false;
    ui32 RendersCount = 0;
    ui64 DurationUs = 0;
};

} // namespace NConsoleClient
} // namespace NYdb
