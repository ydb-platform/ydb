#include "connection.h"

#include <ydb/core/external_sources/external_service.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NKqp {
TString SerializeWasmServiceConnection(const NYql::TKikimrTableMetadata& metadata) {
    const auto settings = NExternalSource::ParseServiceConnection(metadata.GetResolvedExternalDataSource().GetConnectionDescription());
    NJson::TJsonValue value;
    Y_ENSURE(NJson::ReadJsonTree(settings.Serialize(), &value));
    value["schema_owner"] = metadata.PathId.OwnerId();
    value["schema_id"] = metadata.PathId.TableId();
    value["schema_version"] = metadata.SchemaVersion;
    return NJson::WriteJson(value, false);
}

void ValidateWasmServiceConnection(const TString& settings, const NYql::TKikimrTableMetadata& metadata) {
    NJson::TJsonValue value;
    Y_ENSURE(NJson::ReadJsonTree(settings, &value) && value.IsMap(), "Invalid external service connection snapshot");
    Y_ENSURE(value["schema_owner"].GetUIntegerSafe() == metadata.PathId.OwnerId() &&
                 value["schema_id"].GetUIntegerSafe() == metadata.PathId.TableId() &&
                 value["schema_version"].GetUIntegerSafe() == metadata.SchemaVersion &&
                 NExternalSource::TServiceConnectionSettings::Deserialize(settings).Serialize() ==
                     NExternalSource::ParseServiceConnection(metadata.GetResolvedExternalDataSource().GetConnectionDescription()).Serialize(),
        "External service connection changed while preparing query; retry the query");
}
}   // namespace NKikimr::NKqp
