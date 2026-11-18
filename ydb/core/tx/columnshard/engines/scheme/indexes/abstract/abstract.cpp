#include "abstract.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NIndexes {

const NKikimr::NOlap::TBlobRange& TChunkOriginalData::GetBlobRangeVerified() const {
    YDB_LOG_VERIFY(Range);
    return *Range;
}

const TString& TChunkOriginalData::GetDataVerified() const {
    YDB_LOG_VERIFY(Data);
    return *Data;
}

}   // namespace NKikimr::NOlap::NIndexes
