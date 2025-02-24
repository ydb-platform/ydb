#include "print_utils.h"
#include "progress_indication.h"

namespace NYdb {
namespace NConsoleClient {

TProgressIndication::TProgressIndication(bool onlyReadStats)
    : OnlyReadStats(onlyReadStats) {
}

void TProgressIndication::UpdateProgress(const TCurrentStats& stats)
{
    CurrentStats.ReadRows += stats.ReadRows;
    CurrentStats.ReadBytes += stats.ReadBytes;
    CurrentStats.UpdateRows += stats.UpdateRows;
    CurrentStats.UpdateBytes += stats.UpdateBytes;
    CurrentStats.DeleteRows += stats.DeleteRows;
    CurrentStats.DeleteBytes += stats.DeleteBytes;
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
    Cerr << "\r" "\033[K";
    switch (RendersCount % 4) {
        case 0: Cerr << "|"; break;
        case 1: Cerr << "/"; break;
        case 2: Cerr << "-"; break;
        case 3: Cerr << "\\"; break;
    }
    Cerr << "Progress: ";

    Cerr << PrettyNumber(CurrentStats.ReadRows) << " rows read, " << PrettySize(CurrentStats.ReadBytes) << " read"; 

    if (OnlyReadStats) {
        Cerr << ".";
    } else {
        Cerr << ", " << PrettyNumber(CurrentStats.UpdateRows) << " rows updated, " << PrettySize(CurrentStats.UpdateBytes) << " updated, " <<
        PrettyNumber(CurrentStats.DeleteRows) << " rows deleted, " << PrettySize(CurrentStats.DeleteBytes) << " deleted.";
    }

    RendersCount++;

    CurrentStats = TCurrentStats();
}

} // namespace NConsoleClient
} // namespace NYdb
