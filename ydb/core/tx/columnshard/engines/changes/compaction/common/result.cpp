#include "result.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NCompaction {

TString TColumnPortionResult::DebugString() const {
    return TStringBuilder() << "chunks=" << Chunks.size() << ";";
}

ui32 TColumnPortionResult::GetRecordsCount() const {
    ui32 result = 0;
    for (auto&& i : Chunks) {
        AFL_VERIFY(i->GetRecordsCount());
        result += *i->GetRecordsCount();
    }
    return result;
}

}
