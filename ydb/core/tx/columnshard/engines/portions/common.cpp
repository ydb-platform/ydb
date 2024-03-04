#include "common.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap {

TString TChunkAddress::DebugString() const {
    return TStringBuilder() << "(column_id=" << ColumnId << ";chunk=" << Chunk << ";)";
}

}
