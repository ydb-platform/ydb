#include "checker.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/checker.h>

#include <ydb/library/formats/arrow/common/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

void TFilterChecker::DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
    for (auto&& i : HashValues) {
        proto.MutableBloomNGrammFilter()->AddHashValues(i);
    }
}

bool TFilterChecker::DoCheckImpl(const std::vector<TString>& blobs) const {
    AFL_VERIFY(blobs.size() == 1);
    for (auto&& blob : blobs) {
        TFixStringBitsStorage bits(blob);
        bool found = true;
        for (auto&& i : HashValues) {
            if (!bits.Get(i % bits.GetSizeBits())) {
                found = false;
                break;
            }
        }
        if (found) {
            //            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("size", bArray.length())("data", bArray.ToString())("index_id", GetIndexId());
            return true;
        }
    }
    return false;
}

bool TFilterChecker::DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
    if (!proto.HasBloomNGrammFilter()) {
        return false;
    }
    for (auto&& i : proto.GetBloomNGrammFilter().GetHashValues()) {
        HashValues.emplace(i);
    }
    if (HashValues.empty()) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
