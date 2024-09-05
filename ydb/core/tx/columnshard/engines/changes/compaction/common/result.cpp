#include "result.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NCompaction {

TString TColumnPortionResult::DebugString() const {
    return TStringBuilder() << "chunks=" << Chunks.size() << ";";
}

}
