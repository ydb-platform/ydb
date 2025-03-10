#include "common.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap {

TString TChunkAddress::DebugString() const {
    return TStringBuilder() << "(column_id=" << ColumnId << ";chunk=" << Chunk << ";)";
}

TString TFullChunkAddress::DebugString() const {
    return TStringBuilder() << "(path_id=" << PathId << ";portion_id=" << PortionId << ";column_id=" << ColumnId << ";chunk=" << Chunk << ";)";
}

}
