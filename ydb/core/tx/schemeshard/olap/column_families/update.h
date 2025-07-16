#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/accessor/abstract/request.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapColumnFamlilyDiff {
private:
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(std::optional<NKikimrSchemeOp::EColumnCodec>, Codec);
    YDB_ACCESSOR_DEF(std::optional<i32>, CodecLevel);
    YDB_ACCESSOR_DEF(NArrow::NAccessor::TRequestedConstructorContainer, AccessorConstructor);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& diffColumnFamily, IErrorCollector& errors);
};

class TOlapColumnFamlilyAdd {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, SerializerContainer);
    YDB_ACCESSOR_DEF(NArrow::NAccessor::TRequestedConstructorContainer, AccessorConstructor);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& columnFamily, IErrorCollector& errors);
    void ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily);
    void Serialize(NKikimrSchemeOp::TFamilyDescription& columnSchema) const;
    bool ApplyDiff(const TOlapColumnFamlilyDiff& diffColumn, IErrorCollector& errors);
};

class TOlapColumnFamiliesUpdate {
private:
    YDB_READONLY_DEF(TVector<TOlapColumnFamlilyAdd>, AddColumnFamilies);
    YDB_READONLY_DEF(TVector<TOlapColumnFamlilyDiff>, AlterColumnFamily);

public:
    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors);
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
};

}
