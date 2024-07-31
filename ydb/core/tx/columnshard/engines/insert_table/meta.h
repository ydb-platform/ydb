#pragma once
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/formats/arrow/modifier/subset.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/tx/data_events/common/modification_type.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TInsertedDataMeta {
private:
    YDB_READONLY_DEF(TInstant, DirtyWriteTime);
    YDB_READONLY(ui32, NumRows, 0);
    YDB_READONLY(ui64, RawBytes, 0);
    YDB_READONLY(NEvWrite::EModificationType, ModificationType, NEvWrite::EModificationType::Upsert);
    YDB_READONLY_DEF(NArrow::TSchemaSubset, SchemaSubset);

    mutable bool KeysParsed = false;
    mutable std::optional<NArrow::TFirstLastSpecialKeys> SpecialKeysParsed;

    NKikimrTxColumnShard::TLogicalMetadata OriginalProto;

    const std::optional<NArrow::TFirstLastSpecialKeys>& GetSpecialKeys() const;
public:
    ui64 GetTxVolume() const {
        return 2 * sizeof(ui64) + sizeof(ui32) + sizeof(OriginalProto) + (SpecialKeysParsed ? SpecialKeysParsed->GetMemoryBytes() : 0);
    }

    TInsertedDataMeta(const NKikimrTxColumnShard::TLogicalMetadata& proto)
        : OriginalProto(proto)
    {
        AFL_VERIFY(proto.HasDirtyWriteTimeSeconds())("data", proto.DebugString());
        DirtyWriteTime = TInstant::Seconds(proto.GetDirtyWriteTimeSeconds());
        NumRows = proto.GetNumRows();
        RawBytes = proto.GetRawBytes();
        if (proto.HasModificationType()) {
            ModificationType = TEnumOperator<NEvWrite::EModificationType>::DeserializeFromProto(proto.GetModificationType());
        }
        if (proto.HasSchemaSubset()) {
            SchemaSubset.DeserializeFromProto(proto.GetSchemaSubset()).Validate();
        }
    }

    std::optional<NArrow::TReplaceKey> GetFirstPK(const std::shared_ptr<arrow::Schema>& schema) const {
        if (GetSpecialKeys()) {
            return GetSpecialKeys()->GetFirst(schema);
        } else {
            return {};
        }
    }
    std::optional<NArrow::TReplaceKey> GetLastPK(const std::shared_ptr<arrow::Schema>& schema) const {
        if (GetSpecialKeys()) {
            return GetSpecialKeys()->GetLast(schema);
        } else {
            return {};
        }
    }

    NKikimrTxColumnShard::TLogicalMetadata SerializeToProto() const;

};

}
