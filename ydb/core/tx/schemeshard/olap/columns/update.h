#pragma once
#include <ydb/core/formats/arrow/dictionary/diff.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NKikimr::NSchemeShard {

class TOlapColumnDiff {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    YDB_READONLY_DEF(NArrow::NDictionary::TEncodingDiff, DictionaryEncoding);
public:
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDiff& columnSchema, IErrorCollector& errors) {
        Name = columnSchema.GetName();
        if (!Name) {
            errors.AddError("empty field name");
            return false;
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
    YDB_FLAG_ACCESSOR(NotNull, false);
    YDB_READONLY_DEF(std::optional<NArrow::NSerialization::TSerializerContainer>, Serializer);
    YDB_READONLY_DEF(std::optional<NArrow::NDictionary::TEncodingSettings>, DictionaryEncoding);
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
    static bool IsAllowedFirstPkType(ui32 typeId);
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
