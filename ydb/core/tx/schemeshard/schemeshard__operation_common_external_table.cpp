#include "schemeshard__operation_common_external_table.h"

#include <ydb/core/scheme/scheme_types_proto.h>

#include <utility>

namespace NKikimr::NSchemeShard::NExternalTable {

constexpr uint32_t MAX_FIELD_SIZE = 1000;
constexpr uint32_t MAX_PROTOBUF_SIZE = 2 * 1024 * 1024; // 2 MiB

bool ValidateSourceType(const TString& sourceType, TString& errStr) {
    // Only object storage supported today
    if (sourceType != "ObjectStorage") {
        errStr = "Only ObjectStorage source type supported but got " + sourceType;
        return false;
    }
    return true;
}

bool ValidateLocation(const TString& location, TString& errStr) {
    if (!location) {
        errStr = "Location must not be empty";
        return false;
    }
    if (location.Size() > MAX_FIELD_SIZE) {
        errStr = Sprintf("Maximum length of location must be less or equal equal to %u but got %lu", MAX_FIELD_SIZE, location.Size());
        return false;
    }
    return true;
}

bool ValidateContent(const TString& content, TString& errStr) {
    if (content.Size() > MAX_PROTOBUF_SIZE) {
        errStr = Sprintf("Maximum size of content must be less or equal equal to %u but got %lu", MAX_PROTOBUF_SIZE, content.Size());
        return false;
    }
    return true;
}

bool ValidateDataSourcePath(const TString& dataSourcePath, TString& errStr) {
    if (!dataSourcePath) {
        errStr = "Data source path must not be empty";
        return false;
    }
    return true;
}

bool Validate(const TString& sourceType, const NKikimrSchemeOp::TExternalTableDescription& desc, TString& errStr) {
    return ValidateSourceType(sourceType, errStr)
        && ValidateLocation(desc.GetLocation(), errStr)
        && ValidateContent(desc.GetContent(), errStr)
        && ValidateDataSourcePath(desc.GetDataSourcePath(), errStr);
}

std::pair<TExternalTableInfo::TPtr, TMaybe<TString>> CreateExternalTable(
    const TString& sourceType,
    const NKikimrSchemeOp::TExternalTableDescription& desc,
    const NExternalSource::IExternalSourceFactory::TPtr& factory,
    ui64 alterVersion) {
    TString errStr;

    if (!desc.ColumnsSize()) {
        errStr = "The schema must have at least one column";
        return std::make_pair(nullptr, errStr);
    }

    TExternalTableInfo::TPtr externalTableInfo = new TExternalTableInfo;
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

    if (desc.GetSourceType() != "General") {
        errStr = "Only general data source has been supported as request";
        return std::make_pair(nullptr, errStr);
    }

    externalTableInfo->DataSourcePath = desc.GetDataSourcePath();
    externalTableInfo->Location = desc.GetLocation();
    externalTableInfo->AlterVersion = alterVersion;
    externalTableInfo->SourceType = sourceType;

    NKikimrExternalSources::TSchema schema;
    uint64_t nextColumnId = 1;
    for (const auto& col : desc.GetColumns()) {
        TString colName = col.GetName();

        if (!colName) {
            errStr = "Columns cannot have an empty name";
            return std::make_pair(nullptr, errStr);
        }

        if (col.HasTypeId()) {
            errStr = TStringBuilder() << "Cannot set TypeId for column '" << colName << "', use Type";
            return std::make_pair(nullptr, errStr);
        }

        if (!col.HasType()) {
            errStr = TStringBuilder() << "Missing Type for column '" << colName << "'";
            return std::make_pair(nullptr, errStr);
        }

        auto typeName = NMiniKQL::AdaptLegacyYqlType(col.GetType());

        NScheme::TTypeInfo typeInfo;
        if (!GetTypeInfo(typeRegistry->GetType(typeName), col.GetTypeInfo(), typeName, colName, typeInfo, errStr)) {
            return std::make_pair(nullptr, errStr);
        }

        ui32 colId = col.HasId() ? col.GetId() : nextColumnId;
        if (externalTableInfo->Columns.contains(colId)) {
            errStr = Sprintf("Duplicate column id: %" PRIu32, colId);
            return std::make_pair(nullptr, errStr);
        }

        nextColumnId = colId + 1 > nextColumnId ? colId + 1 : nextColumnId;

        TTableInfo::TColumn& column = externalTableInfo->Columns[colId];
        column = TTableInfo::TColumn(colName, colId, typeInfo, typeInfo.GetPgTypeMod(typeName), col.GetNotNull());

        auto& schemaColumn= *schema.add_column();
        schemaColumn.set_name(colName);
        NScheme::ProtoFromTypeInfo(typeInfo, *schemaColumn.mutable_type(), col.GetNotNull());
    }

    try {
        NKikimrExternalSources::TGeneral general;
        general.ParseFromStringOrThrow(desc.GetContent());
        const auto source = factory->GetOrCreate(sourceType);
        if (!source->HasExternalTable()) {
            errStr = TStringBuilder{} << "External table isn't supported for " << sourceType;
            return std::make_pair(nullptr, errStr);
        }
        externalTableInfo->Content = source->Pack(schema, general);
    } catch (...) {
        errStr = CurrentExceptionMessage();
        return std::make_pair(nullptr, errStr);
    }

    return std::make_pair(externalTableInfo, Nothing());
}


} // namespace NKikimr::NSchemeShard::NExternalDataSource
