#include "message_capture.h"

#include <util/datetime/base.h>
#include <util/stream/file.h>

namespace NLsp::NYql {

std::pair<IOutputStream*, THolder<IOutputStream>> OpenMessageCapture(TMaybe<TFsPath> path) {
    if (!path) {
        return {&Cerr, nullptr};
    }

    if (path->IsDirectory()) {
        TInstant t = TInstant::Now();
        path = *path / t.FormatLocalTime("yql-lsp-capture-%Y-%m-%d-%H-%M-%S.jsonl");
    }

    auto fmout = MakeHolder<TFileOutput>(*path);
    auto* mout = fmout.Get();
    return {mout, std::move(fmout)};
}

} // namespace NLsp::NYql
