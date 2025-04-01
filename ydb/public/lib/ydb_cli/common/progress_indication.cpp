#include "print_utils.h"
#include "progress_indication.h"

#include <library/cpp/colorizer/colors.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb {
namespace NConsoleClient {

TProgressIndication::TProgressIndication() {
}

void TProgressIndication::UpdateProgress(const TCurrentStats& stats)
{
    CurrentStats.ReadRows += stats.ReadRows;
    CurrentStats.ReadBytes += stats.ReadBytes;
}

void TProgressIndication::SetDurationUs(ui64 durationUs) {
    DurationUs = durationUs;
}

TProgressIndication::~TProgressIndication() {
    if (!Finished) {
        Finish();
    }
}

void TProgressIndication::Finish() {
    Finished = true;
    Cerr << "\r" "\033[K";
}

void TProgressIndication::Render()
{
    if (!GetTerminalWidth())
        return;

    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cerr << "\r" "\033[K";
    Cerr << colors.LightGreen();
    switch (RendersCount % 4) {
        case 0: Cerr << "|"; break;
        case 1: Cerr << "/"; break;
        case 2: Cerr << "-"; break;
        case 3: Cerr << "\\"; break;
    }
    Cerr << colors.Default() << "Progress: " << colors.Default();

    Cerr << colors.Default() << PrettyNumber(CurrentStats.ReadRows) << " rows read, " << PrettySize(CurrentStats.ReadBytes) << " read";
    if (DurationUs) {
        Cerr << colors.Default() << " (" << PrettyNumber(CurrentStats.ReadRows * 1000000.0 / DurationUs) << "/s" << ", " <<
            PrettySize(CurrentStats.ReadBytes * 1000000.0 / DurationUs) << "/s" << ")";
    }

    Cerr << colors.Default() << ".";

    Cerr.Flush();

    RendersCount++;

    CurrentStats = TCurrentStats();
}

} // namespace NConsoleClient
} // namespace NYdb
