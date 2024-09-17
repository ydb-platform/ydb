#include "meta.h"

namespace NKikimr::NOlap {

NKikimrTxColumnShard::TLogicalMetadata TInsertedDataMeta::SerializeToProto() const {
    return OriginalProto;
}

std::shared_ptr<NArrow::TFirstLastSpecialKeys> TInsertedDataMeta::GetSpecialKeys(const std::shared_ptr<arrow::Schema>& schema) const {
    if (KeyInitialized.Val()) {
        return SpecialKeysParsed;
    }
    std::shared_ptr<NArrow::TFirstLastSpecialKeys> result;
    if (OriginalProto.HasSpecialKeysPayloadData()) {
        result = std::make_shared<NArrow::TFirstLastSpecialKeys>(OriginalProto.GetSpecialKeysPayloadData(), schema);
    } else if (OriginalProto.HasSpecialKeysRawData()) {
        result = std::make_shared<NArrow::TFirstLastSpecialKeys>(OriginalProto.GetSpecialKeysRawData());
    } else {
        AFL_VERIFY(false);
    }
    if (AtomicCas(&KeyInitialization, 1, 0)) {
        SpecialKeysParsed = result;
        KeyInitialized = 1;
    }
    return result;
}

}
