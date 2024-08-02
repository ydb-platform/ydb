#pragma once
#include <ydb/core/formats/arrow/dictionary/diff.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/formats/arrow/accessor/abstract/request.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/engines/scheme/defaults/common/scalar.h>

namespace NKikimr::NSchemeShard {

class TOlapColumnDiff {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    YDB_READONLY_DEF(NArrow::NDictionary::TEncodingDiff, DictionaryEncoding);
    YDB_READONLY_DEF(std::optional<TString>, StorageId);
    YDB_READONLY_DEF(std::optional<TString>, DefaultValue);
    YDB_READONLY_DEF(NArrow::NAccessor::TRequestedConstructorContainer, AccessorConstructor);
public:
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDiff& columnSchema, IErrorCollector& errors) {
        Name = columnSchema.GetName();
        if (!!columnSchema.GetStorageId()) {
            StorageId = columnSchema.GetStorageId();
        }
        if (!Name) {
            errors.AddError("empty field name");
            return false;
        }
        if (columnSchema.HasDefaultValue()) {
            DefaultValue = columnSchema.GetDefaultValue();
        }
        if (columnSchema.HasDataAccessorConstructor()) {
            if (!AccessorConstructor.DeserializeFromProto(columnSchema.GetDataAccessorConstructor())) {
                errors.AddError("cannot parse accessor constructor from proto");
                return false;
            }
        }
        if (columnSchema.HasSerializer()) {
            if (!Serializer.DeserializeFromProto(columnSchema.GetSerializer())) {
                errors.AddError("cannot parse serializer diff from proto");
                return false;
            }
        }
        if (!DictionaryEncoding.DeserializeFromProto(columnSchema.GetDictionaryEncoding())) {
            errors.AddError("cannot parse dictionary encoding diff from proto");
            return false;
        }
        return true;
    }
};

class TOlapColumnAdd {
private:
    YDB_READONLY_DEF(std::optional<ui32>, KeyOrder);
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(TString, TypeName);
    YDB_READONLY_DEF(NScheme::TTypeInfo, Type);
    YDB_READONLY_DEF(TString, StorageId);
    YDB_FLAG_ACCESSOR(NotNull, false);
    YDB_READONLY_DEF(std::optional<NArrow::NSerialization::TSerializerContainer>, Serializer);
    YDB_READONLY_DEF(std::optional<NArrow::NDictionary::TEncodingSettings>, DictionaryEncoding);
    YDB_READONLY_DEF(NOlap::TColumnDefaultScalarValue, DefaultValue);
    YDB_READONLY_DEF(NArrow::NAccessor::TConstructorContainer, AccessorConstructor);
public:
    TOlapColumnAdd(const std::optional<ui32>& keyOrder)
        : KeyOrder(keyOrder) {

    }
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors);
    void ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema);
    void Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const;
    bool ApplyDiff(const TOlapColumnDiff& diffColumn, IErrorCollector& errors);
    bool IsKeyColumn() const {
        return !!KeyOrder;
    }
    static bool IsAllowedType(ui32 typeId);
    static bool IsAllowedPkType(ui32 typeId);
    static bool IsAllowedPgType(ui32 pgTypeId);
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
