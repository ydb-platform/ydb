#include "print_utils.h"
#include "progress_indication.h"

namespace NYdb {
namespace NConsoleClient {

TProgressIndication::TProgressIndication(bool onlyReadStats) : OnlyReadStats(onlyReadStats) {
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

    Cerr << PrettyNumber(CurrentStats.ReadRows) << " read rows, " << PrettySize(CurrentStats.ReadBytes) << " read bytes"; 
    
    if (OnlyReadStats) {
        Cerr << ".";
    } else {
        Cerr << ", " << PrettyNumber(CurrentStats.UpdateRows) << " update rows, " << PrettySize(CurrentStats.UpdateBytes) << " update bytes, " <<
        PrettyNumber(CurrentStats.DeleteRows) << " delete rows, " << PrettySize(CurrentStats.DeleteBytes) << " delete bytes.";
    }

    RendersCount++;
}

} // namespace NConsoleClient
} // namespace NYdb
