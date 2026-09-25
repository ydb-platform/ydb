#include "line_break.h"

namespace NLogParsing {

bool SplitLineBreak(TStringBuf chunk, TVector<TStringBuf>* records) {
    records->clear();
    // Avoid StringSplitter: SkipEmpty takes address of Y_HIDDEN
    // SPLITTER_EMPTY_SENTINEL from yutil, which ld_plugin strips from the
    // guest link line (sdk provides yutil at runtime) and wasm-ld cannot
    // relocate undefined data symbols.
    TStringBuf line;
    while (chunk.NextTok('\n', line)) {
        if (!line.empty()) {
            records->push_back(line);
        }
    }
    return !records->empty();
}

} // namespace NLogParsing
