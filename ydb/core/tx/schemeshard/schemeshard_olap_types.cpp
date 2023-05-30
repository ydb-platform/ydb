#include "schemeshard_olap_types.h"
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

    bool TOlapColumnAdd::ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors) {
        if (!columnSchema.GetName()) {
            errors.AddError("Columns cannot have an empty name");
            return false;
        }
        Name = columnSchema.GetName();
        NotNullFlag = columnSchema.GetNotNull();
        TypeName = columnSchema.GetType();
        if (columnSchema.HasCompression()) {
            auto compression = NArrow::TCompression::BuildFromProto(columnSchema.GetCompression());
            if (!compression) {
                errors.AddError("Cannot parse compression info: " + compression.GetErrorMessage());
                return false;
            }
            Compression = *compression;
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

    void TOlapColumnAdd::ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema) {
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
        if (columnSchema.HasCompression()) {
            auto compression = NArrow::TCompression::BuildFromProto(columnSchema.GetCompression());
            Y_VERIFY(compression.IsSuccess(), "%s", compression.GetErrorMessage().data());
            Compression = *compression;
        }
        if (columnSchema.HasDictionaryEncoding()) {
            auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnSchema.GetDictionaryEncoding());
            Y_VERIFY(settings.IsSuccess());
            DictionaryEncoding = *settings;
        }
        NotNullFlag = columnSchema.GetNotNull();
    }

    void TOlapColumnAdd::Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const {
        columnSchema.SetName(Name);
        columnSchema.SetType(TypeName);
        columnSchema.SetNotNull(NotNullFlag);
        if (Compression) {
            *columnSchema.MutableCompression() = Compression->SerializeToProto();
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
        Y_VERIFY(GetName() == diffColumn.GetName());
        {
            auto result = diffColumn.GetCompression().Apply(Compression);
            if (!result) {
                errors.AddError("Cannot merge compression info: " + result.GetErrorMessage());
                return false;
            }
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

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
        if (tableSchema.HasEngine()) {
            Engine = tableSchema.GetEngine();
        }

        TMap<TString, ui32> keyColumnNames;
        for (auto&& pkKey : tableSchema.GetKeyColumnNames()) {
            if (!keyColumnNames.emplace(pkKey, keyColumnNames.size()).second) {
                errors.AddError(Sprintf("Duplicate key column '%s'", pkKey.c_str()));
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
            if (column.GetKeyOrder() && *column.GetKeyOrder() == 0) {
                if (!TOlapSchema::IsAllowedFirstPkType(column.GetType().GetTypeId())) {
                    errors.AddError(TStringBuilder()
                        << "Type '" << column.GetType().GetTypeId() << "' specified for column '" << column.GetName()
                        << "' is not supported in first PK position");
                    return false;
                }
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
            AddColumns.emplace_back(std::move(column));
        }
        return true;
    }

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        TSet<TString> addColumnNames;
        if (alterRequest.DropColumnsSize()) {
            errors.AddError("Drop columns method not supported for tablestore and table");
            return false;
        }

        for (auto& columnSchema : alterRequest.GetAddColumns()) {
            TOlapColumnAdd column({});
            if (!column.ParseFromRequest(columnSchema, errors)) {
                return false;
            }
            if (addColumnNames.contains(column.GetName())) {
                errors.AddError(Sprintf("column '%s' duplication for add", column.GetName().c_str()));
                return false;
            }
            addColumnNames.emplace(column.GetName());
            AddColumns.emplace_back(std::move(column));
        }

        TSet<TString> alterColumnNames;
        for (auto& columnSchemaDiff: alterRequest.GetAlterColumns()) {
            TOlapColumnDiff columnDiff;
            if (!columnDiff.ParseFromRequest(columnSchemaDiff, errors)) {
                return false;
            }
            if (addColumnNames.contains(columnDiff.GetName())) {
                errors.AddError(Sprintf("column '%s' have to be either add or update", columnDiff.GetName().c_str()));
                return false;
            }
            if (alterColumnNames.contains(columnDiff.GetName())) {
                errors.AddError(Sprintf("column '%s' duplication for update", columnDiff.GetName().c_str()));
                return false;
            }
            alterColumnNames.emplace(columnDiff.GetName());
            AlterColumns.emplace_back(std::move(columnDiff));
        }
        return true;
    }

    bool TOlapSchema::Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors) {
        if (Columns.empty() && schemaUpdate.GetAddColumns().empty()) {
            errors.AddError("No add columns specified");
            return false;
        }

        if (!HasEngine()) {
            Engine = schemaUpdate.GetEngineDef(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);
        } else {
            if (schemaUpdate.HasEngine()) {
                errors.AddError("No engine updates supported");
                return false;
            }
        }

        const bool hasColumnsBefore = KeyColumnIds.size();
        std::map<ui32, ui32> orderedKeyColumnIds;
        for (auto&& column : schemaUpdate.GetAddColumns()) {
            if (ColumnsByName.contains(column.GetName())) {
                errors.AddError(Sprintf("column '%s' already exists", column.GetName().data()));
                return false;
            }
            if (hasColumnsBefore) {
                if (column.IsNotNull()) {
                    errors.AddError("Cannot add new not null column currently (not supported yet)");
                    return false;
                }
                if (column.GetKeyOrder()) {
                    errors.AddError(Sprintf("column '%s' is pk column. its impossible to modify pk", column.GetName().data()));
                    return false;
                }
            }
            TOlapColumnSchema newColumn(column, NextColumnId++);
            if (newColumn.GetKeyOrder()) {
                Y_VERIFY(orderedKeyColumnIds.emplace(*newColumn.GetKeyOrder(), newColumn.GetId()).second);
            }

            Y_VERIFY(ColumnsByName.emplace(newColumn.GetName(), newColumn.GetId()).second);
            Y_VERIFY(Columns.emplace(newColumn.GetId(), std::move(newColumn)).second);
        }

        for (auto&& columnDiff : schemaUpdate.GetAlterColumns()) {
            auto it = ColumnsByName.find(columnDiff.GetName());
            if (it == ColumnsByName.end()) {
                errors.AddError(Sprintf("column '%s' not exists for altering", columnDiff.GetName().data()));
                return false;
            } else {
                auto itColumn = Columns.find(it->second);
                Y_VERIFY(itColumn != Columns.end());
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
                Y_VERIFY(i == it->first);
            }
            if (KeyColumnIds.empty()) {
                errors.AddError("No primary key specified");
                return false;
            }
        }
        ++Version;
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
            std::optional<ui32> keyOrder;
            if (keyIndexes.contains(columnSchema.GetName())) {
                keyOrder = keyIndexes.at(columnSchema.GetName());
            }

            TOlapColumnSchema column(keyOrder);
            column.ParseFromLocalDB(columnSchema);
            if (keyOrder) {
                Y_VERIFY(*keyOrder < keyIds.size());
                keyIds[*keyOrder] = column.GetId();
            }

            Y_VERIFY(ColumnsByName.emplace(column.GetName(), column.GetId()).second);
            Y_VERIFY(Columns.emplace(column.GetId(), std::move(column)).second);
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
        if (presetProto.HasId()) {
            errors.AddError("Schema preset id cannot be specified explicitly");
            return false;
        }
        if (!presetProto.GetName()) {
            errors.AddError("Schema preset name cannot be empty");
            return false;
        }
        Name = presetProto.GetName();
        return true;
    }
}
