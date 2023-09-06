#include "meta.h"

namespace NKikimr::NOlap {

bool TInsertedDataMeta::DeserializeFromProto(const NKikimrTxColumnShard::TLogicalMetadata& proto) {
    if (proto.HasDirtyWriteTimeSeconds()) {
        DirtyWriteTime = TInstant::Seconds(proto.GetDirtyWriteTimeSeconds());
    }
    if (proto.HasSpecialKeysRawData()) {
        SpecialKeys = NArrow::TFirstLastSpecialKeys(proto.GetSpecialKeysRawData());
    }
    NumRows = proto.GetNumRows();
    RawBytes = proto.GetRawBytes();
    return true;
}

NKikimrTxColumnShard::TLogicalMetadata TInsertedDataMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TLogicalMetadata result;
    result.SetDirtyWriteTimeSeconds(DirtyWriteTime.Seconds());
    if (SpecialKeys) {
        result.SetSpecialKeysRawData(SpecialKeys->SerializeToString());
    }
    result.SetNumRows(NumRows);
    result.SetRawBytes(RawBytes);
    return result;
}

}
