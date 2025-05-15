#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/request.h>
#include <ydb/core/formats/arrow/dictionary/diff.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/defaults/common/scalar.h>
#include <ydb/core/tx/schemeshard/olap/column_families/schema.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapColumnDiff {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    YDB_READONLY_DEF(NArrow::NDictionary::TEncodingDiff, DictionaryEncoding);
    YDB_READONLY_DEF(std::optional<TString>, StorageId);
    YDB_READONLY_DEF(std::optional<TString>, DefaultValue);
    YDB_READONLY_DEF(NArrow::NAccessor::TRequestedConstructorContainer, AccessorConstructor);
    YDB_READONLY_DEF(std::optional<TString>, ColumnFamilyName);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDiff& columnSchema, IErrorCollector& errors);
};

class TOlapColumnBase {
private:
    YDB_READONLY_DEF(std::optional<ui32>, KeyOrder);
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(TString, TypeName);
    YDB_READONLY_DEF(NScheme::TTypeInfo, Type);
    YDB_READONLY_DEF(TString, StorageId);
    YDB_FLAG_ACCESSOR(NotNull, false);
    YDB_ACCESSOR_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    YDB_READONLY_DEF(std::optional<NArrow::NDictionary::TEncodingSettings>, DictionaryEncoding);
    YDB_READONLY_DEF(NOlap::TColumnDefaultScalarValue, DefaultValue);
    YDB_ACCESSOR_DEF(NArrow::NAccessor::TConstructorContainer, AccessorConstructor);
    YDB_READONLY_PROTECT(std::optional<ui32>, ColumnFamilyId, std::nullopt);

    bool ApplyColumnFamilySettings(const TOlapColumnFamily& columnFamilies, IErrorCollector& errors);

public:
    TOlapColumnBase(const std::optional<ui32>& keyOrder, const std::optional<ui32> columnFamilyId = {})
        : KeyOrder(keyOrder)
        , ColumnFamilyId(columnFamilyId)
    {
    }
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors);
    void ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema);
    void Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const;
    bool ApplyDiff(const TOlapColumnDiff& diffColumn, IErrorCollector& errors);
    bool ApplyColumnFamily(const TOlapColumnFamily& columnFamilies, IErrorCollector& errors);
    bool ApplyColumnFamily(const TOlapColumnFamiliesDescription& columnFamilies, IErrorCollector& errors);
    bool ApplyDiff(const TOlapColumnDiff& diffColumn, const TOlapColumnFamiliesDescription& columnFamilies, IErrorCollector& errors);
    bool IsKeyColumn() const {
        return !!KeyOrder;
    }
    static bool IsAllowedType(ui32 typeId);
    static bool IsAllowedPkType(ui32 typeId);
    static bool IsAllowedPgType(ui32 pgTypeId);
};

class TOlapColumnAdd: public TOlapColumnBase {
private:
    using TBase = TOlapColumnBase;
    YDB_READONLY_DEF(std::optional<TString>, ColumnFamilyName);

public:
    TOlapColumnAdd(const std::optional<ui32>& keyOrder)
        : TBase(keyOrder)
    {
    }
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors);
    void ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema);
};

class TOlapColumnsUpdate {
private:
    YDB_READONLY_DEF(TVector<TOlapColumnAdd>, AddColumns);
    YDB_READONLY_DEF(TSet<TString>, DropColumns);
    YDB_READONLY_DEF(TVector<TOlapColumnDiff>, AlterColumns);
public:
    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys = false);
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
};

}
