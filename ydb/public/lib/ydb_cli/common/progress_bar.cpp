#include "common.h"
#include "progress_bar.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <util/string/cast.h>

namespace NYdb {
namespace NConsoleClient {

TProgressBar::TProgressBar(size_t capacity) : Capacity(capacity) {
}

void TProgressBar::SetProcess(size_t progress)
{
    CurProgress = Min(progress, Capacity);
    Render();
}

void TProgressBar::AddProgress(size_t value) {
    CurProgress = Min(CurProgress + value, Capacity);
    if (Capacity == 0) {
        return;
    }
    Render();
}

void TProgressBar::Render()
{
    std::optional<size_t> barLenOpt = GetTerminalWidth();
    if (!barLenOpt)
        return;

    size_t barLen = *barLenOpt;
    TString output = ToString(CurProgress * 100 / Capacity);
    output += "% |";
    TString outputEnd = "| [";
    outputEnd += ToString(CurProgress);
    outputEnd += "/";
    outputEnd += ToString(Capacity);
    outputEnd += "]";

    if (barLen > output.Size()) {
        barLen -= output.Size();
    } else {
        barLen = 1;
    }

    if (barLen > outputEnd.Size()) {
        barLen -= outputEnd.Size();
    } else {
        barLen = 1;
    }

    size_t filledBarLen = CurProgress * barLen / Capacity;
    output += TString("█") * filledBarLen;
    output += TString("░") * (barLen - filledBarLen);
    output += outputEnd;
    output += "\r";
    Cout << output;
    if (CurProgress == Capacity) {
        Cout << "\n";
    }
    Cout.Flush();
}

} // namespace NConsoleClient
} // namespace NYdb
