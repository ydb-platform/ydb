#include "checker.h"
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexes {

void TBloomFilterChecker::DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
    for (auto&& i : HashValues) {
        proto.MutableBloomFilter()->AddHashValues(i);
    }
}

bool TBloomFilterChecker::DoCheckImpl(const std::vector<TString>& blobs) const {
    for (auto&& blob : blobs) {
        auto rb = NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer()->Deserialize(blob));
        AFL_VERIFY(rb);
        AFL_VERIFY(rb->schema()->num_fields() == 1);
        AFL_VERIFY(rb->schema()->field(0)->type()->id() == arrow::Type::BOOL);
        auto& bArray = static_cast<const arrow::BooleanArray&>(*rb->column(0));
        bool found = true;
        for (auto&& i : HashValues) {
            if (!bArray.Value(i % bArray.length())) {
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

bool TBloomFilterChecker::DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
    if (!proto.HasBloomFilter()) {
        return false;
    }
    for (auto&& i : proto.GetBloomFilter().GetHashValues()) {
        HashValues.emplace(i);
    }
    if (HashValues.empty()) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap::NIndexes