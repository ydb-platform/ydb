#include "common.h"
#include "progress_bar.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <util/string/cast.h>

namespace NYdb {
namespace NConsoleClient {

TProgressBar::TProgressBar(size_t capacity, size_t parts)
     : Capacity(capacity)
     , Parts(parts)
{}

void TProgressBar::SetProgress(size_t progress)
{
    const auto partSize = Parts ? Max<size_t>(Capacity / Parts, 1) : Max<size_t>();
    const auto curPartNum = CurProgress / partSize;
    CurProgress = Min(progress, Capacity);
    if (Capacity != 0) {
        Render(Parts && CurProgress / partSize != curPartNum);
    }
}

void TProgressBar::AddProgress(size_t value) {
    SetProgress(CurProgress + value);
}

TProgressBar::~TProgressBar() {
    if (!Finished) {
        Cerr << Endl;
    }
}

void TProgressBar::Render(bool newPart)
{
    std::optional<size_t> barLenOpt = GetErrTerminalWidth();
    if (!barLenOpt) {
        if (newPart) {
            Cerr << CurProgress << " / " << Capacity << Endl;
        }
        return;
    }

    size_t barLen = *barLenOpt;
    TString output = "\r";
    output += ToString(CurProgress * 100 / Capacity);
    output += "% |";
    TString outputEnd = "| [";
    outputEnd += ToString(CurProgress);
    outputEnd += "/";
    outputEnd += ToString(Capacity);
    outputEnd += "]";

    if (barLen > output.size() - 1) {
        barLen -= output.size() - 1;
    } else {
        barLen = 1;
    }

    if (barLen > outputEnd.size()) {
        barLen -= outputEnd.size();
    } else {
        barLen = 1;
    }

    size_t filledBarLen = CurProgress * barLen / Capacity;
    output += TString("█") * filledBarLen;
    output += TString("░") * (barLen - filledBarLen);
    output += outputEnd;
    Cerr << output;
    if (CurProgress == Capacity) {
        Cerr << "\n";
        Finished = true;
    }
    Cerr.Flush();
}

} // namespace NConsoleClient
} // namespace NYdb
