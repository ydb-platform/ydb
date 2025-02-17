#include "const.h"

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

TString TConstants::GetRecordsCountIntervalString() {
    return TStringBuilder() << "[" << MinRecordsCount << ", " << MaxRecordsCount << "]";
}

TString TConstants::GetHashesCountIntervalString() {
    return TStringBuilder() << "[" << MinHashesCount << ", " << MaxHashesCount << "]";
}

TString TConstants::GetFilterSizeBytesIntervalString() {
    return TStringBuilder() << "[" << MinFilterSizeBytes << ", " << MaxFilterSizeBytes << "]";
}

TString TConstants::GetNGrammSizeIntervalString() {
    return TStringBuilder() << "[" << MinNGrammSize << ", " << MaxNGrammSize << "]";
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
