#include "common.h"

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/xxhash/xxhash.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

TString TNodeId::ToString() const {
    return TStringBuilder() << "[" << ColumnId << "." << GenerationId << "." << NodeType << "]";
}

TString TOriginalDataAddress::DebugString() const {
    if (SubColumnName) {
        return TStringBuilder() << "{cId=" << ColumnId << ";sub=" << SubColumnName << "}";
    } else {
        return ::ToString(ColumnId);
    }
}

ui64 TOriginalDataAddress::CalcSubColumnHash(const std::string_view sv) {
    return XXH3_64bits(sv.data(), sv.size());
}

}   // namespace NKikimr::NOlap::NIndexes::NRequest
