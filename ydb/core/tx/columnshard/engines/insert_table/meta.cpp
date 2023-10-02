#include "meta.h"

namespace NKikimr::NOlap {

NKikimrTxColumnShard::TLogicalMetadata TInsertedDataMeta::SerializeToProto() const {
    return OriginalProto;
}

const std::optional<NKikimr::NArrow::TFirstLastSpecialKeys>& TInsertedDataMeta::GetSpecialKeys() const {
    if (!KeysParsed) {
        if (OriginalProto.HasSpecialKeysRawData()) {
            SpecialKeysParsed = NArrow::TFirstLastSpecialKeys(OriginalProto.GetSpecialKeysRawData());
        }
        KeysParsed = true;
    }
    return SpecialKeysParsed;
}

}
