#include "like.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

TString TLikeDescription::ToString() const {
    TStringBuilder sb;
    sb << "[";
    ui32 idx = 0;
    for (auto&& i : LikeSequences) {
        sb << i.first;
        if (idx + 1 < LikeSequences.size()) {
            sb << ",";
        }
        ++idx;
    }
    sb << "];";
    return sb;
}

TString TLikePart::ToString() const {
    if (Operation == EOperation::StartsWith) {
        return Value + '%';
    }
    if (Operation == EOperation::EndsWith) {
        return '%' + Value;
    }
    if (Operation == EOperation::Contains) {
        return '%' + Value + '%';
    }
    AFL_VERIFY(false);
    return "";
}

}   // namespace NKikimr::NOlap::NIndexes::NRequest
