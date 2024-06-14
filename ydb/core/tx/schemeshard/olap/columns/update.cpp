#include "update.h"
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr::NSchemeShard {

    bool TOlapColumnAdd::ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors) {
        if (!columnSchema.GetName()) {
            errors.AddError("Columns cannot have an empty name");
            return false;
        }
        Name = columnSchema.GetName();
        NotNullFlag = columnSchema.GetNotNull();
        TypeName = columnSchema.GetType();
        StorageId = columnSchema.GetStorageId();
        if (columnSchema.HasSerializer()) {
            NArrow::NSerialization::TSerializerContainer serializer;
            if (!serializer.DeserializeFromProto(columnSchema.GetSerializer())) {
                errors.AddError("Cannot parse serializer info");
                return false;
            }
            Serializer = serializer;
        }
        if (columnSchema.HasDictionaryEncoding()) {
            auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnSchema.GetDictionaryEncoding());
            if (!settings) {
                errors.AddError("Cannot parse dictionary compression info: " + settings.GetErrorMessage());
                return false;
            }
            DictionaryEncoding = *settings;
        }

        if (columnSchema.HasTypeId()) {
            errors.AddError(TStringBuilder() << "Cannot set TypeId for column '" << Name << ", use Type");
            return false;
        }

        if (!columnSchema.HasType()) {
            errors.AddError(TStringBuilder() << "Missing Type for column '" << Name);
            return false;
        }

        if (const auto& typeName = NMiniKQL::AdaptLegacyYqlType(TypeName); typeName.StartsWith("pg")) {
            const auto typeDesc = NPg::TypeDescFromPgTypeName(typeName);
            if (!(typeDesc && TOlapColumnAdd::IsAllowedPgType(NPg::PgTypeIdFromTypeDesc(typeDesc)))) {
                errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
                return false;
            }
            Type = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
        } else {
            Y_ABORT_UNLESS(AppData()->TypeRegistry);
            const NScheme::IType* type = AppData()->TypeRegistry->GetType(typeName);
            if (!type) {
                errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
                return false;
            }
            if (!NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
                errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
                return false;;
            }
            Type = NScheme::TTypeInfo(type->GetTypeId());
            if (!IsAllowedType(type->GetTypeId())){
                errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
                return false;
            }
        }
        const auto arrowTypeStatus = NArrow::GetArrowType(Type).status();
        if (!arrowTypeStatus.ok()) {
            errors.AddError(TStringBuilder() << "Column '" << Name << "': " << arrowTypeStatus.ToString());
            return false;
        }
        return true;
    }

    void TOlapColumnAdd::ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema) {
        Name = columnSchema.GetName();
        TypeName = columnSchema.GetType();
        StorageId = columnSchema.GetStorageId();

        if (columnSchema.HasTypeInfo()) {
            Type = NScheme::TypeInfoModFromProtoColumnType(
                columnSchema.GetTypeId(), &columnSchema.GetTypeInfo())
                .TypeInfo;
        } else {
            Type = NScheme::TypeInfoModFromProtoColumnType(
                columnSchema.GetTypeId(), nullptr)
                .TypeInfo;
        }
        if (columnSchema.HasSerializer()) {
            NArrow::NSerialization::TSerializerContainer serializer;
            AFL_VERIFY(serializer.DeserializeFromProto(columnSchema.GetSerializer()));
            Serializer = serializer;
        } else if (columnSchema.HasCompression()) {
            NArrow::NSerialization::TSerializerContainer serializer;
            serializer.DeserializeFromProto(columnSchema.GetCompression()).Validate();
            Serializer = serializer;
        }
        if (columnSchema.HasDictionaryEncoding()) {
            auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnSchema.GetDictionaryEncoding());
            Y_ABORT_UNLESS(settings.IsSuccess());
            DictionaryEncoding = *settings;
        }
        NotNullFlag = columnSchema.GetNotNull();
    }

    void TOlapColumnAdd::Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const {
        columnSchema.SetName(Name);
        columnSchema.SetType(TypeName);
        columnSchema.SetNotNull(NotNullFlag);
        columnSchema.SetStorageId(StorageId);
        if (Serializer) {
            Serializer->SerializeToProto(*columnSchema.MutableSerializer());
        }
        if (DictionaryEncoding) {
            *columnSchema.MutableDictionaryEncoding() = DictionaryEncoding->SerializeToProto();
        }

        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(Type, "");
        columnSchema.SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *columnSchema.MutableTypeInfo() = *columnType.TypeInfo;
        }
    }

    bool TOlapColumnAdd::ApplyDiff(const TOlapColumnDiff& diffColumn, IErrorCollector& errors) {
        Y_ABORT_UNLESS(GetName() == diffColumn.GetName());
        if (diffColumn.GetStorageId()) {
            StorageId = *diffColumn.GetStorageId();
        }
        if (diffColumn.GetSerializer()) {
            Serializer = diffColumn.GetSerializer();
        }
        {
            auto result = diffColumn.GetDictionaryEncoding().Apply(DictionaryEncoding);
            if (!result) {
                errors.AddError("Cannot merge dictionary encoding info: " + result.GetErrorMessage());
                return false;
            }
        }
        return true;
    }

    bool TOlapColumnAdd::IsAllowedType(ui32 typeId) {
        if (!NScheme::NTypeIds::IsYqlType(typeId)) {
            return false;
        }

        switch (typeId) {
            case NYql::NProto::Bool:
            case NYql::NProto::Interval:
            case NYql::NProto::DyNumber:
                return false;
            default:
                break;
        }
        return true;
    }

    bool TOlapColumnAdd::IsAllowedPgType(ui32 pgTypeId) {
        switch (pgTypeId) {
            case INT2OID:
            case INT4OID:
            case INT8OID:
            case FLOAT4OID:
            case FLOAT8OID:
                return true;
            default:
                break;
        }
        return false;
    }

    bool TOlapColumnAdd::IsAllowedPkType(ui32 typeId) {
        switch (typeId) {
            case NYql::NProto::Int8:
            case NYql::NProto::Uint8: // Byte
            case NYql::NProto::Int16:
            case NYql::NProto::Uint16:
            case NYql::NProto::Int32:
            case NYql::NProto::Uint32:
            case NYql::NProto::Int64:
            case NYql::NProto::Uint64:
            case NYql::NProto::String:
            case NYql::NProto::Utf8:
            case NYql::NProto::Date:
            case NYql::NProto::Datetime:
            case NYql::NProto::Timestamp:
            case NYql::NProto::Date32:
            case NYql::NProto::Datetime64:
            case NYql::NProto::Timestamp64:
            case NYql::NProto::Interval64:
            case NYql::NProto::Decimal:
                return true;
            default:
                return false;
        }
    }

    bool TOlapColumnsUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        for (const auto& column : alterRequest.GetDropColumns()) {
            if (!DropColumns.emplace(column.GetName()).second) {
                errors.AddError(NKikimrScheme::StatusInvalidParameter, "Duplicated column for drop");
                return false;
            }
        }
        TSet<TString> addColumnNames;
        for (auto& columnSchema : alterRequest.GetAddColumns()) {
            TOlapColumnAdd column({});
            if (!column.ParseFromRequest(columnSchema, errors)) {
                return false;
            }
            if (addColumnNames.contains(column.GetName())) {
                errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "column '" << column.GetName() << "' duplication for add");
                return false;
            }
            addColumnNames.emplace(column.GetName());
            AddColumns.emplace_back(std::move(column));
        }

        TSet<TString> alterColumnNames;
        for (auto& columnSchemaDiff : alterRequest.GetAlterColumns()) {
            TOlapColumnDiff columnDiff;
            if (!columnDiff.ParseFromRequest(columnSchemaDiff, errors)) {
                return false;
            }
            if (addColumnNames.contains(columnDiff.GetName())) {
                errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "column '" << columnDiff.GetName() << "' have to be either add or update");
                return false;
            }
            if (alterColumnNames.contains(columnDiff.GetName())) {
                errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "column '" << columnDiff.GetName() << "' duplication for update");
                return false;
            }
            alterColumnNames.emplace(columnDiff.GetName());
            AlterColumns.emplace_back(std::move(columnDiff));
        }
        return true;
    }

    bool TOlapColumnsUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
        TMap<TString, ui32> keyColumnNames;
        for (auto&& pkKey : tableSchema.GetKeyColumnNames()) {
            if (!keyColumnNames.emplace(pkKey, keyColumnNames.size()).second) {
                errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Duplicate key column '" << pkKey << "'");
                return false;
            }
        }

        TSet<TString> columnNames;
        for (auto& columnSchema : tableSchema.GetColumns()) {
            std::optional<ui32> keyOrder;
            {
                auto it = keyColumnNames.find(columnSchema.GetName());
                if (it != keyColumnNames.end()) {
                    keyOrder = it->second;
                }
            }

            TOlapColumnAdd column(keyOrder);
            if (!column.ParseFromRequest(columnSchema, errors)) {
                return false;
            }
            if (column.IsKeyColumn()) {
                if (!TOlapColumnAdd::IsAllowedPkType(column.GetType().GetTypeId())) {
                    errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder()
                        << "Type '" << column.GetTypeName() << "' specified for column '" << column.GetName()
                        << "' is not supported as primary key");
                    return false;
                }
            }
            if (columnNames.contains(column.GetName())) {
                errors.AddError(NKikimrScheme::StatusMultipleModifications, TStringBuilder() << "Duplicate column '" << column.GetName() << "'");
                return false;
            }
            if (!allowNullKeys) {
                if (keyColumnNames.contains(column.GetName()) && !column.IsNotNull()) {
                    errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Nullable key column '" << column.GetName() << "'");
                    return false;
                }
            }
            columnNames.emplace(column.GetName());
            AddColumns.emplace_back(std::move(column));
        }

        return true;
    }

}
