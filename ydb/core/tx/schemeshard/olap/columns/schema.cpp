#include "schema.h"
#include <ydb/library/accessor/validator.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>

namespace NKikimr::NSchemeShard {

void TOlapColumnSchema::Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const {
    TBase::Serialize(columnSchema);
    columnSchema.SetId(Id);
}

void TOlapColumnSchema::ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema) {
    TBase::ParseFromLocalDB(columnSchema);
    Id = columnSchema.GetId();
}

bool TOlapColumnsDescription::ApplyUpdate(const TOlapColumnsUpdate& schemaUpdate, IErrorCollector& errors, ui32& nextEntityId) {
    if (Columns.empty() && schemaUpdate.GetAddColumns().empty()) {
        errors.AddError(NKikimrScheme::StatusSchemeError, "No add columns specified");
        return false;
    }

    const bool hasColumnsBefore = KeyColumnIds.size();
    std::map<ui32, ui32> orderedKeyColumnIds;
    for (auto&& column : schemaUpdate.GetAddColumns()) {
        if (ColumnsByName.contains(column.GetName())) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "column '" << column.GetName() << "' already exists");
            return false;
        }
        if (hasColumnsBefore) {
            if (column.IsNotNull()) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "Cannot add new not null column currently (not supported yet)");
                return false;
            }
            if (column.GetKeyOrder()) {
                errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "column '" << column.GetName() << "' is pk column. its impossible to modify pk");
                return false;
            }
        }
        TOlapColumnSchema newColumn(column, nextEntityId++);
        if (newColumn.GetKeyOrder()) {
            Y_ABORT_UNLESS(orderedKeyColumnIds.emplace(*newColumn.GetKeyOrder(), newColumn.GetId()).second);
        }

        Y_ABORT_UNLESS(ColumnsByName.emplace(newColumn.GetName(), newColumn.GetId()).second);
        Y_ABORT_UNLESS(Columns.emplace(newColumn.GetId(), std::move(newColumn)).second);
    }

    for (auto&& columnDiff : schemaUpdate.GetAlterColumns()) {
        auto it = ColumnsByName.find(columnDiff.GetName());
        if (it == ColumnsByName.end()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "column '" << columnDiff.GetName() << "' not exists for altering");
            return false;
        } else {
            auto itColumn = Columns.find(it->second);
            Y_ABORT_UNLESS(itColumn != Columns.end());
            TOlapColumnSchema& newColumn = itColumn->second;
            if (!newColumn.ApplyDiff(columnDiff, errors)) {
                return false;
            }
        }
    }

    if (KeyColumnIds.empty()) {
        auto it = orderedKeyColumnIds.begin();
        for (ui32 i = 0; i < orderedKeyColumnIds.size(); ++i, ++it) {
            KeyColumnIds.emplace_back(it->second);
            Y_ABORT_UNLESS(i == it->first);
        }
        if (KeyColumnIds.empty()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "No primary key specified");
            return false;
        }
    }

    for (const auto& columnName : schemaUpdate.GetDropColumns()) {
        auto columnInfo = GetByName(columnName);
        if (!columnInfo) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Unknown column for drop: " << columnName);
            return false;
        }

        if (columnInfo->IsKeyColumn()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Cannot remove pk column: " << columnName);
            return false;
        }
        ColumnsByName.erase(columnName);
        Columns.erase(columnInfo->GetId());
    }

    return true;
}

void TOlapColumnsDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    TMap<TString, ui32> keyIndexes;
    ui32 idx = 0;
    for (auto&& kName : tableSchema.GetKeyColumnNames()) {
        keyIndexes[kName] = idx++;
    }

    TVector<ui32> keyIds;
    keyIds.resize(tableSchema.GetKeyColumnNames().size(), 0);
    for (const auto& columnSchema : tableSchema.GetColumns()) {
        std::optional<ui32> keyOrder;
        if (keyIndexes.contains(columnSchema.GetName())) {
            keyOrder = keyIndexes.at(columnSchema.GetName());
        }

        TOlapColumnSchema column(keyOrder);
        column.ParseFromLocalDB(columnSchema);
        if (keyOrder) {
            Y_ABORT_UNLESS(*keyOrder < keyIds.size());
            keyIds[*keyOrder] = column.GetId();
        }

        Y_ABORT_UNLESS(ColumnsByName.emplace(column.GetName(), column.GetId()).second);
        Y_ABORT_UNLESS(Columns.emplace(column.GetId(), std::move(column)).second);
    }
    KeyColumnIds.swap(keyIds);
}

void TOlapColumnsDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    for (const auto& column : Columns) {
        column.second.Serialize(*tableSchema.AddColumns());
    }

    for (auto&& cId : KeyColumnIds) {
        auto column = GetById(cId);
        Y_ABORT_UNLESS(!!column);
        *tableSchema.AddKeyColumnNames() = column->GetName();
    }
}

bool TOlapColumnsDescription::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

    ui32 lastColumnId = 0;
    THashSet<ui32> usedColumns;
    for (const auto& colProto : opSchema.GetColumns()) {
        if (colProto.GetName().empty()) {
            errors.AddError("Columns cannot have an empty name");
            return false;
        }
        const TString& colName = colProto.GetName();
        auto* col = GetByName(colName);
        if (!col) {
            errors.AddError("Column '" + colName + "' does not match schema preset");
            return false;
        }
        if (colProto.HasId() && colProto.GetId() != col->GetId()) {
            errors.AddError("Column '" + colName + "' has id " + colProto.GetId() + " that does not match schema preset");
            return false;
        }

        if (!usedColumns.insert(col->GetId()).second) {
            errors.AddError("Column '" + colName + "' is specified multiple times");
            return false;
        }
        if (col->GetId() < lastColumnId) {
            errors.AddError("Column order does not match schema preset");
            return false;
        }
        lastColumnId = col->GetId();

        if (colProto.HasTypeId()) {
            errors.AddError("Cannot set TypeId for column '" + colName + "', use Type");
            return false;
        }
        if (!colProto.HasType()) {
            errors.AddError("Missing Type for column '" + colName + "'");
            return false;
        }

        NScheme::TTypeInfo typeInfo;
        if (const auto& typeName = NMiniKQL::AdaptLegacyYqlType(colProto.GetType()); typeName.StartsWith("pg")) {
            const auto typeDesc = NPg::TypeDescFromPgTypeName(typeName);
            if (!(typeDesc && TOlapColumnAdd::IsAllowedPgType(NPg::PgTypeIdFromTypeDesc(typeDesc)))) {
                errors.AddError("Type '" + colProto.GetType() + "' specified for column '" + colName + "' is not supported");
                return false;
            }
            typeInfo = NScheme::TTypeInfo(typeDesc);
        } else if (const auto decimalType = NScheme::TDecimalType::ParseTypeName(typeName)) {
            typeInfo = NScheme::TTypeInfo(*decimalType);
        } else {
            const NScheme::IType* type = typeRegistry->GetType(typeName);
            if (!type || !TOlapColumnAdd::IsAllowedType(type->GetTypeId())) {
                errors.AddError("Type '" + colProto.GetType() + "' specified for column '" + colName + "' is not supported");
                return false;
            }
            typeInfo = NScheme::TTypeInfo(type->GetTypeId());
        }

        if (typeInfo != col->GetType()) {
            errors.AddError("Type '" + TypeName(typeInfo) + "' specified for column '" + colName + "' does not match schema preset type '" + TypeName(col->GetType()) + "'");
            return false;
        }
    }

    for (auto& pr : Columns) {
        if (!usedColumns.contains(pr.second.GetId())) {
            errors.AddError("Specified schema is missing some schema preset columns");
            return false;
        }
    }

    TVector<ui32> keyColumnIds;
    for (const TString& keyName : opSchema.GetKeyColumnNames()) {
        auto* col = GetByName(keyName);
        if (!col) {
            errors.AddError("Unknown key column '" + keyName + "'");
            return false;
        }
        keyColumnIds.push_back(col->GetId());
    }
    if (keyColumnIds != KeyColumnIds) {
        errors.AddError("Specified schema key columns not matching schema preset");
        return false;
    }
    return true;
}

const NKikimr::NSchemeShard::TOlapColumnSchema* TOlapColumnsDescription::GetByIdVerified(const ui32 id) const noexcept {
    return TValidator::CheckNotNull(GetById(id));
}

}
