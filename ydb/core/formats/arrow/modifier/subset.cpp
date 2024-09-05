#include "subset.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow {

TSchemaSubset::TSchemaSubset(const std::set<ui32>& fieldsIdx, const ui32 fieldsCount) {
    AFL_VERIFY(fieldsIdx.size() <= fieldsCount);
    AFL_VERIFY(fieldsIdx.size());
    if (fieldsCount == fieldsIdx.size()) {
        return;
    }
    Exclude = (fieldsCount - fieldsIdx.size()) < fieldsIdx.size();
    if (!Exclude) {
        FieldIdx = std::vector<ui32>(fieldsIdx.begin(), fieldsIdx.end());
    } else {
        auto it = fieldsIdx.begin();
        for (ui32 i = 0; i < fieldsCount; ++i) {
            if (it == fieldsIdx.end() || i < *it) {
                FieldIdx.emplace_back(i);
            } else if (*it == i) {
                ++it;
            } else {
                AFL_VERIFY(false);
            }
        }
    }
}

NKikimrArrowSchema::TSchemaSubset TSchemaSubset::SerializeToProto() const {
    NKikimrArrowSchema::TSchemaSubset result;
    result.MutableList()->SetExclude(Exclude);
    for (auto&& i : FieldIdx) {
        result.MutableList()->AddFieldsIdx(i);
    }
    return result;
}

TConclusionStatus TSchemaSubset::DeserializeFromProto(const NKikimrArrowSchema::TSchemaSubset& proto) {
    if (!proto.HasList()) {
        return TConclusionStatus::Fail("no schema subset data");
    }
    Exclude = proto.GetList().GetExclude();
    std::vector<ui32> fieldIdx;
    for (auto&& i : proto.GetList().GetFieldsIdx()) {
        fieldIdx.emplace_back(i);
    }
    std::swap(fieldIdx, FieldIdx);
    return TConclusionStatus::Success();
}

}