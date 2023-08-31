#pragma once
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TInsertedDataMeta {
private:
    YDB_READONLY_DEF(TInstant, DirtyWriteTime);
    YDB_READONLY(ui32, NumRows, 0);
    YDB_READONLY(ui64, RawBytes, 0);
    std::optional<NArrow::TFirstLastSpecialKeys> SpecialKeys;

    bool DeserializeFromProto(const NKikimrTxColumnShard::TLogicalMetadata& proto);

public:
    TInsertedDataMeta(const NKikimrTxColumnShard::TLogicalMetadata& proto) {
        Y_VERIFY(DeserializeFromProto(proto));
    }

    TInsertedDataMeta(const TInstant dirtyWriteTime, const ui32 numRows, const ui64 rawBytes, std::shared_ptr<arrow::RecordBatch> batch, const std::vector<TString>& columnNames = {})
        : DirtyWriteTime(dirtyWriteTime)
        , NumRows(numRows)
        , RawBytes(rawBytes)
    {
        if (batch) {
            SpecialKeys = NArrow::TFirstLastSpecialKeys(batch, columnNames);
        }
    }

    std::optional<NArrow::TReplaceKey> GetMin(const std::shared_ptr<arrow::Schema>& schema) const {
        if (SpecialKeys) {
            return SpecialKeys->GetMin(schema);
        } else {
            return {};
        }
    }
    std::optional<NArrow::TReplaceKey> GetMax(const std::shared_ptr<arrow::Schema>& schema) const {
        if (SpecialKeys) {
            return SpecialKeys->GetMax(schema);
        } else {
            return {};
        }
    }

    NKikimrTxColumnShard::TLogicalMetadata SerializeToProto() const;

};

}
