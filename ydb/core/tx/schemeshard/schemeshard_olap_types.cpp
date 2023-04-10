#include "schemeshard_olap_types.h"
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>

namespace NKikimr::NSchemeShard {

    void TOlapColumnSchema::Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const {
        columnSchema.SetId(Id);
        columnSchema.SetName(Name);
        columnSchema.SetType(TypeName);
        columnSchema.SetNotNull(NotNullFlag);

        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(Type, "");
        columnSchema.SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *columnSchema.MutableTypeInfo() = *columnType.TypeInfo;
        }
    }

    void TOlapColumnSchema::ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema) {
        Id = columnSchema.GetId();
        Name = columnSchema.GetName();
        TypeName = columnSchema.GetType();

        if (columnSchema.HasTypeInfo()) {
            Type = NScheme::TypeInfoModFromProtoColumnType(
                       columnSchema.GetTypeId(), &columnSchema.GetTypeInfo())
                       .TypeInfo;
        } else {
            Type = NScheme::TypeInfoModFromProtoColumnType(
                       columnSchema.GetTypeId(), nullptr)
                       .TypeInfo;
        }
        NotNullFlag = columnSchema.GetNotNull();
    }

    bool TOlapColumnSchema::ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors) {
        if (!columnSchema.GetName()) {
            errors.AddError("Columns cannot have an empty name");
            return false;
        }
        Name = columnSchema.GetName();
        NotNullFlag = columnSchema.GetNotNull();
        TypeName = columnSchema.GetType();

        if (columnSchema.HasTypeId()) {
            errors.AddError(TStringBuilder() << "Cannot set TypeId for column '" << Name << ", use Type");
            return false;
        }

        if (!columnSchema.HasType()) {
            errors.AddError(TStringBuilder() << "Missing Type for column '" << Name);
            return false;
        }

        auto typeName = NMiniKQL::AdaptLegacyYqlType(TypeName);
        Y_VERIFY(AppData()->TypeRegistry);
        const NScheme::IType* type =
            AppData()->TypeRegistry->GetType(typeName);
        if (!type) {
            errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
            return false;
        }
        if (!NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
            errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
            return false;;
        }
        Type = NScheme::TTypeInfo(type->GetTypeId());
        if (!TOlapSchema::IsAllowedType(type->GetTypeId())){
            errors.AddError(TStringBuilder() << "Type '" << typeName << "' specified for column '" << Name << "' is not supported");
            return false;
        }
        return true;
    }


    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
        if (tableSchema.HasEngine()) {
            Engine = tableSchema.GetEngine();
        }

        TSet<TString> keyColumnNames;
        for (auto&& pkKey : tableSchema.GetKeyColumnNames()) {
            if (keyColumnNames.contains(pkKey)) {
                errors.AddError(Sprintf("Duplicate key column '%s'", pkKey.c_str()));
                return false;
            }
            keyColumnNames.emplace(pkKey);
            KeyColumnNames.emplace_back(pkKey);
        }

        TSet<TString> columnNames;
        for (auto& columnSchema : tableSchema.GetColumns()) {
            TOlapColumnSchema column;
            if (!column.ParseFromRequest(columnSchema, errors)) {
                return false;
            }
            if (columnNames.contains(column.GetName())) {
                errors.AddError(Sprintf("Duplicate column '%s'", column.GetName().c_str()));
                return false;
            }
            if (!allowNullKeys) {
                if (keyColumnNames.contains(column.GetName()) && !column.IsNotNull()) {
                    errors.AddError(Sprintf("Nullable key column '%s'", column.GetName().c_str()));
                    return false;
                }
            }
            columnNames.emplace(column.GetName());
            Columns.emplace_back(std::move(column));
        }
        return true;
    }

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTable& alterRequest, IErrorCollector& errors) {
        TSet<TString> columnNames;
        for (auto& columnSchema : alterRequest.GetAlterSchema().GetColumns()) {
            TOlapColumnSchema column;
            if (!column.ParseFromRequest(columnSchema, errors)) {
                return false;
            }
            if (columnNames.contains(column.GetName())) {
                errors.AddError(Sprintf("Duplicate column '%s'", column.GetName().c_str()));
                return false;
            }
            if (column.IsNotNull()) {
                errors.AddError("Not null updates not supported");
                return false;
            }
            columnNames.emplace(column.GetName());
            Columns.emplace_back(std::move(column));
        }
        return true;
    }

    bool TOlapSchema::Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors) {
        if (Columns.empty() && schemaUpdate.GetColumns().empty()) {
            errors.AddError("No columns specified");
            return false;
        }

        if (KeyColumnIds.empty()) {
            if (schemaUpdate.GetKeyColumnNames().empty()) {
                errors.AddError("No primary key specified");
                return false;
            }
        } else {
            if (!schemaUpdate.GetKeyColumnNames().empty()) {
                errors.AddError("No primary key updates supported");
                return false;
            }
        }

        TMap<TString, ui32> keyIndexes;
        for (ui32 i = 0; i < schemaUpdate.GetKeyColumnNames().size(); ++i) {
            keyIndexes[schemaUpdate.GetKeyColumnNames()[i]] = i;
        }

        if (!HasEngine()) {
            Engine = schemaUpdate.GetEngineDef(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);
        } else {
            if (schemaUpdate.HasEngine()) {
                errors.AddError("No engine updates supported");
                return false;
            }
        }

        for (auto&& column : schemaUpdate.GetColumns()) {
            if (ColumnsByName.contains(column.GetName())) {
                errors.AddError("No special column updates supported");
                return false;
            }
            TOlapColumnSchema newColumn = column;
            newColumn.SetId(NextColumnId);
            ++NextColumnId;

            if (keyIndexes.contains(newColumn.GetName())) {
                auto keyOrder = keyIndexes.at(newColumn.GetName());
                if (keyOrder == 0) {
                    if (!IsAllowedFirstPkType(newColumn.GetType().GetTypeId())) {
                        errors.AddError(TStringBuilder()
                                        << "Type '" << newColumn.GetType().GetTypeId() << "' specified for column '" << newColumn.GetName()
                                        << "' is not supported in first PK position");
                        return false;
                    }
                }
                newColumn.SetKeyOrder(keyOrder);
            }
            ColumnsByName[newColumn.GetName()] = newColumn.GetId();
            Columns[newColumn.GetId()] = std::move(newColumn);
        }

        if (KeyColumnIds.empty()) {
            TVector<ui32> keyColumnIds;
            keyColumnIds.reserve(schemaUpdate.GetKeyColumnNames().size());
            for (auto&& columnName : schemaUpdate.GetKeyColumnNames()) {
                auto it = ColumnsByName.find(columnName);
                if (it == ColumnsByName.end()) {
                    errors.AddError("Invalid key column " + columnName);
                    return false;
                }
                keyColumnIds.push_back(it->second);
            }
            KeyColumnIds.swap(keyColumnIds);
        }
        return true;
    }

    void TOlapSchema::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
        NextColumnId = tableSchema.GetNextColumnId();
        Version = tableSchema.GetVersion();
        Y_VERIFY(tableSchema.HasEngine());
        Engine = tableSchema.GetEngine();

        TMap<TString, ui32> keyIndexes;
        ui32 idx = 0;
        for (auto&& kName : tableSchema.GetKeyColumnNames()) {
            keyIndexes[kName] = idx++;
        }

        TVector<ui32> keyIds;
        keyIds.resize(tableSchema.GetKeyColumnNames().size(), 0);
        for (const auto& columnSchema : tableSchema.GetColumns()) {
            TOlapColumnSchema column;
            column.ParseFromLocalDB(columnSchema);

            if (keyIndexes.contains(column.GetName())) {
                auto keyOrder = keyIndexes.at(column.GetName());
                column.SetKeyOrder(keyOrder);
                Y_VERIFY(keyOrder < keyIds.size());
                keyIds[keyOrder] = column.GetId();
            }
            ColumnsByName[column.GetName()] = column.GetId();
            Columns[column.GetId()] = std::move(column);
        }
        KeyColumnIds.swap(keyIds);
    }

    void TOlapSchema::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
        tableSchema.SetNextColumnId(NextColumnId);
        tableSchema.SetVersion(Version);

        Y_VERIFY(HasEngine());
        tableSchema.SetEngine(GetEngineUnsafe());

        for (const auto& column : Columns) {
            column.second.Serialize(*tableSchema.AddColumns());
        }

        for (auto&& cId : KeyColumnIds) {
            auto column = GetColumnById(cId);
            Y_VERIFY(!!column);
            *tableSchema.AddKeyColumnNames() = column->GetName();
        }
    }

    bool TOlapSchema::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

        ui32 lastColumnId = 0;
        THashSet<ui32> usedColumns;
        for (const auto& colProto : opSchema.GetColumns()) {
            if (colProto.GetName().empty()) {
                errors.AddError("Columns cannot have an empty name");
                return false;
            }
            const TString& colName = colProto.GetName();
            auto* col = GetColumnByName(colName);
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

            auto typeName = NMiniKQL::AdaptLegacyYqlType(colProto.GetType());
            const NScheme::IType* type = typeRegistry->GetType(typeName);
            if (!type || !IsAllowedType(type->GetTypeId())) {
                errors.AddError("Type '" + colProto.GetType() + "' specified for column '" + colName + "' is not supported");
                return false;
            }
            NScheme::TTypeInfo typeInfo(type->GetTypeId());

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
            auto* col = GetColumnByName(keyName);
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

        if (opSchema.GetEngine() != Engine) {
            errors.AddError("Specified schema engine does not match schema preset");
            return false;
        }
        return true;
    }

    bool TOlapSchema::IsAllowedType(ui32 typeId) {
        if (!NScheme::NTypeIds::IsYqlType(typeId)) {
            return false;
        }

        switch (typeId) {
            case NYql::NProto::Bool:
            case NYql::NProto::Interval:
            case NYql::NProto::Decimal:
            case NYql::NProto::DyNumber:
                return false;
            default:
                break;
        }
        return true;
    }

    bool TOlapSchema::IsAllowedFirstPkType(ui32 typeId) {
        switch (typeId) {
            case NYql::NProto::Uint8: // Byte
            case NYql::NProto::Int32:
            case NYql::NProto::Uint32:
            case NYql::NProto::Int64:
            case NYql::NProto::Uint64:
            case NYql::NProto::String:
            case NYql::NProto::Utf8:
            case NYql::NProto::Date:
            case NYql::NProto::Datetime:
            case NYql::NProto::Timestamp:
                return true;
            case NYql::NProto::Interval:
            case NYql::NProto::Decimal:
            case NYql::NProto::DyNumber:
            case NYql::NProto::Yson:
            case NYql::NProto::Json:
            case NYql::NProto::JsonDocument:
            case NYql::NProto::Float:
            case NYql::NProto::Double:
            case NYql::NProto::Bool:
                return false;
            default:
                break;
        }
        return false;
    }

    void TOlapStoreSchemaPreset::ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) {
        Y_VERIFY(presetProto.HasId());
        Y_VERIFY(presetProto.HasName());
        Y_VERIFY(presetProto.HasSchema());
        Id = presetProto.GetId();
        Name = presetProto.GetName();
        TOlapSchema::Parse(presetProto.GetSchema());
    }

    void TOlapStoreSchemaPreset::Serialize(NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) const {
        presetProto.SetId(Id);
        presetProto.SetName(Name);
        TOlapSchema::Serialize(*presetProto.MutableSchema());
    }

    bool TOlapStoreSchemaPreset::ParseFromRequest(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto, IErrorCollector& errors) {
        if (!presetProto.GetName()) {
            errors.AddError("Schema preset name cannot be empty");
            return false;
        }
        Name = presetProto.GetName();
        return true;
    }
}
