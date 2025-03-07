#include "abstract.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NIndexes {

const NKikimr::NOlap::TBlobRange& TChunkOriginalData::GetBlobRangeVerified() const {
    AFL_VERIFY(Range);
    return *Range;
}

const TString& TChunkOriginalData::GetDataVerified() const {
    AFL_VERIFY(Data);
    return *Data;
}

}   // namespace NKikimr::NOlap::NIndexes
