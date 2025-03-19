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
        ui64 UpdateRows = 0;
        ui64 UpdateBytes = 0;
        ui64 DeleteRows = 0;
        ui64 DeleteBytes = 0;
    };
    explicit TProgressIndication(bool onlyReadStats = false);

    ~TProgressIndication();

    void UpdateProgress(const TCurrentStats& stats);
    void Render();
    void Finish();

private:

    TCurrentStats CurrentStats;
    bool Finished = false;
    ui32 RendersCount = 0;
    bool OnlyReadStats = false;
};

} // namespace NConsoleClient
} // namespace NYdb
