#pragma once

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NSchemeShard {
class TColumnFamily {
private:
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(std::optional<ui32>, Id);
    YDB_ACCESSOR_DEF(std::optional<NKikimrSchemeOp::EColumnCodec>, ColumnCodec);
    YDB_ACCESSOR_DEF(std::optional<i32>, ColumnCodecLevel);
    YDB_ACCESSOR_DEF(std::optional<NKikimrArrowAccessorProto::TRequestedConstructor>, AccessorConstructor);

public:
    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TFamilyDescription& proto);
    TConclusionStatus DeserializeFromProto(const Ydb::Table::ColumnFamily& proto);
    NKikimrSchemeOp::TFamilyDescription SerializeToProto() const;
    void SerializeToProto(NKikimrSchemeOp::TFamilyDescription& proto) const;

    TConclusion<NKikimrSchemeOp::TOlapColumn::TSerializer> GetSerializer() const;
    TConclusionStatus SetSerializer(const NArrow::NSerialization::TSerializerContainer& serializer);

    static NKikimrSchemeOp::TFamilyDescription GetDefaultColumnFamily();
};
}
