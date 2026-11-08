#include "schemeshard__operation_common_external_data_source.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/tx/tiering/tier/s3_uri.h>

#include <util/generic/guid.h>

#include <utility>

namespace NKikimr::NSchemeShard::NExternalDataSource {

constexpr uint32_t MAX_FIELD_SIZE    = 1000;
constexpr uint32_t MAX_PROTOBUF_SIZE = 2 * 1024 * 1024; // 2 MiB

TString GetStoragePoolName(const TString& database, const TPathId& pathId) {
    return TStringBuilder() << database << ":eds-" << pathId.OwnerId << '-' << pathId.LocalPathId;
}

TString GetStoragePoolAlias(const NKikimrSchemeOp::TExternalDataSourceProperties& properties) {
    const auto& values = properties.GetProperties();
    const auto it = values.find("storage_pool_name");
    return it == values.end() ? TString{} : it->second;
}

TString PrepareStoragePool(const NKikimrSchemeOp::TExternalDataSourceDescription& desc, TExternalDataSourceInfo& source,
                            const TPath& parent, const TProposeContext& context, const TString& owner) {
    if (!desc.GetProperties().GetProperties().contains("storage_pool_name")) {
        return {};
    }
    if (!AppData()->FeatureFlags.GetEnableExternalDataSourceStoragePools()) {
        return "External data source storage pools are disabled";
    }
    if (!AppData()->FeatureFlags.GetEnableSchemaSecrets()) {
        return "Schema secrets are required for external data source storage pools";
    }
    const auto alias = GetStoragePoolAlias(desc.GetProperties());
    if (alias.empty() || alias.size() > 255 || IsPathPartContainsOnlyDots(alias) || PathPartBrokenAt(alias) != alias.end()) {
        return "Invalid STORAGE_POOL_NAME: expected a nonempty path component of up to 255 characters";
    }
    if (desc.GetSourceType() != "ObjectStorage" || !desc.GetAuth().HasAws()) {
        return "STORAGE_POOL_NAME requires ObjectStorage with AWS authentication";
    }
    if (owner.empty() || (context.UserToken && context.UserToken->GetUserSID() != owner)) {
        return "Storage pool credentials require the external data source owner to match the creating principal";
    }
    const auto database = parent.GetDomainPathString();
    const auto& auth = desc.GetAuth().GetAws();
    for (const auto* secret : { &auth.GetAwsAccessKeyIdSecretName(), &auth.GetAwsSecretAccessKeySecretName() }) {
        TString error;
        if (!secret->StartsWith(database + '/') || *secret != CanonizePath(*secret) ||
                !CheckDbPath(*secret, TString(ExtractDomain(database)), error)) {
            return "Storage pool credentials must reference absolute scheme secret paths inside the database";
        }
    }
    const auto location = NColumnShard::NTiers::TS3Uri::ParseUri(desc.GetLocation());
    if (!location.IsSuccess() || !location->GetScheme() || location->GetBucket().empty() || location->GetHost().empty() ||
            location->GetFolder() || desc.GetLocation().Contains('?') || desc.GetLocation().Contains('#') || desc.GetLocation().Contains('@')) {
        return "STORAGE_POOL_NAME requires an HTTP(S) bucket-root LOCATION";
    }
    const auto domain = parent.GetPathIdForDomain();
    if (context.SS->IsServerlessDomain(parent) || context.SS->IsServerlessDomainGlobal(domain, parent.DomainInfo())) {
        return "STORAGE_POOL_NAME requires a database with dedicated storage";
    }
    ui32 managedPools = 0;
    for (const auto& [id, eds] : context.SS->ExternalDataSources) {
        const auto otherAlias = GetStoragePoolAlias(eds->Properties);
        if (otherAlias.empty() || TPath::Init(id, context.SS).GetPathIdForDomain() != domain) {
            continue;
        }
        if (otherAlias == alias) {
            return "STORAGE_POOL_NAME is already in use in this database";
        }
        ++managedPools;
    }
    if (managedPools >= context.SS->MaxExternalDataSourceStoragePoolsPerDatabase) {
        return "External data source storage pool limit reached for this database";
    }
    TChannelsBindings bindings;
    if (!context.SS->ResolveTabletChannels(0, domain, bindings) || bindings.size() < 3) {
        return "Cannot resolve the database's default BlobDepot tablet channels";
    }
    for (ui32 i = 0; i < 3; ++i) {
        source.Properties.AddStoragePoolChannels(bindings[i].GetStoragePoolName());
    }
    source.Properties.SetStoragePoolObjectPrefix("ydb/" + CreateGuidAsString());
    return {};
}

void RequestStoragePool(const TPath& path, const TExternalDataSourceInfo& source,
                        TOperationId operationId, TOperationContext& context) {
    const auto database = path.GetDomainPathString();
    auto request = MakeHolder<TEvBlobStorage::TEvExternalDataSourcePoolRequest>();
    auto* create = &request->Record;
    create->SetSubTxId(operationId.GetSubTxId());
    auto* group = create->MutableGroup();
    const auto& id = path->PathId;
    group->SetName(TStringBuilder() << "eds-" << id.OwnerId << '-' << id.LocalPathId);
    group->SetDatabase(database);
    group->SetStoragePoolName(GetStoragePoolName(database, id));

    Y_ABORT_UNLESS(source.Properties.StoragePoolChannelsSize() == 3);
    for (ui32 i = 0; i < 3; ++i) {
        auto* profile = group->AddChannelProfiles();
        profile->SetStoragePoolName(source.Properties.GetStoragePoolChannels(i));
        profile->SetChannelKind(i < 2 ? NKikimrBlobDepot::TChannelKind::System : NKikimrBlobDepot::TChannelKind::Data);
        profile->SetCount(1);
    }
    auto* backend = group->MutableS3BackendSettings();
    backend->MutableSyncMode();
    auto* settings = backend->MutableSettings();
    const auto location = NColumnShard::NTiers::TS3Uri::ParseUri(source.Location);
    location->FillSettings(*settings);
    settings->SetRegion(source.Auth.GetAws().GetAwsRegion());
    settings->SetObjectKeyPattern(source.Properties.GetStoragePoolObjectPrefix());
    auto* secrets = backend->MutableExternalDataSourceCredentials();
    secrets->SetDatabase(database);
    secrets->SetOwnerSid(path->Owner);
    secrets->SetAccessKeySecretPath(source.Auth.GetAws().GetAwsAccessKeyIdSecretName());
    secrets->SetSecretKeySecretPath(source.Auth.GetAws().GetAwsSecretAccessKeySecretName());
    context.OnComplete.BindMsgToPipe(operationId, TTabletId(MakeBSControllerID()),
        TPipeMessageId(operationId.GetSubTxId(), ui64(operationId.GetTxId())), request.Release());
}

bool ValidateLocationAndInstallation(const TString& location,
                                     const TString& installation,
                                     TString& errStr) {
    if (location.size() > MAX_FIELD_SIZE) {
        errStr =
            Sprintf("Maximum length of location must be less or equal equal to %u but got %lu",
                    MAX_FIELD_SIZE,
                    location.size());
        return false;
    }
    if (installation.size() > MAX_FIELD_SIZE) {
        errStr = Sprintf(
            "Maximum length of installation must be less or equal equal to %u but got %lu",
            MAX_FIELD_SIZE,
            installation.size());
        return false;
    }
    return true;
}

bool CheckAuth(const TString& authMethod,
               const TVector<TString>& availableAuthMethods,
               TString& errStr) {
    if (Find(availableAuthMethods, authMethod) == availableAuthMethods.end()) {
        errStr = TStringBuilder{} << authMethod << " isn't supported for this source type";
        return false;
    }

    return true;
}

bool ValidateProperties(const NKikimrSchemeOp::TExternalDataSourceProperties& properties,
                        TString& errStr) {
    if (properties.HasStoragePoolError() || properties.StoragePoolChannelsSize() || properties.HasStoragePoolObjectPrefix()) {
        errStr = "Storage pool state is managed by SchemeShard";
        return false;
    }
    if (properties.ByteSizeLong() > MAX_PROTOBUF_SIZE) {
        errStr =
            Sprintf("Maximum size of properties must be less or equal equal to %u but got %lu",
                    MAX_PROTOBUF_SIZE,
                    properties.ByteSizeLong());
        return false;
    }
    return true;
}

bool ValidateAuth(const NKikimrSchemeOp::TAuth& auth,
                  const NExternalSource::IExternalSource::TPtr& source,
                  TString& errStr) {
    if (auth.ByteSizeLong() > MAX_PROTOBUF_SIZE) {
        errStr = Sprintf(
            "Maximum size of authorization information must be less or equal equal to %u but got %lu",
            MAX_PROTOBUF_SIZE,
            auth.ByteSizeLong());
        return false;
    }
    const auto availableAuthMethods = source->GetAuthMethods();
    switch (auth.identity_case()) {
        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET: {
            errStr = "Authorization method isn't specified";
            return false;
        }
        case NKikimrSchemeOp::TAuth::kServiceAccount:
            return CheckAuth("SERVICE_ACCOUNT", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kMdbBasic:
            return CheckAuth("MDB_BASIC", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kBasic:
            return CheckAuth("BASIC", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kAws:
            return CheckAuth("AWS", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kToken:
            return CheckAuth("TOKEN", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kIam:
            return CheckAuth("IAM", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kNone:
            return CheckAuth("NONE", availableAuthMethods, errStr);
    }
    return false;
}

bool Validate(const NKikimrSchemeOp::TExternalDataSourceDescription& desc,
              const NExternalSource::IExternalSourceFactory::TPtr& factory,
              TString& errStr) {

    if (!factory) {
        errStr = "Internal error. External source factory is not set, please contact internal support";
        return false;
    }

    const auto databaseType = NYql::DatabaseTypeFromString(desc.GetSourceType());
    if (!databaseType) {
        errStr = TStringBuilder() << "Unknown source type: " << desc.GetSourceType();
        return false;
    }

    try {
        const auto source = factory->GetOrCreate(*databaseType);
        source->ValidateExternalDataSource(desc.SerializeAsString());
        return ValidateLocationAndInstallation(desc.GetLocation(),
                                               desc.GetInstallation(),
                                               errStr) &&
               ValidateAuth(desc.GetAuth(), source, errStr) &&
               ValidateProperties(desc.GetProperties(), errStr);
    } catch (...) {
        errStr = CurrentExceptionMessage();
        return false;
    }
}

TExternalDataSourceInfo::TPtr CreateExternalDataSource(const NKikimrSchemeOp::TExternalDataSourceDescription& desc, ui64 alterVersion) {
    auto externalDataSourceInfo = MakeIntrusive<TExternalDataSourceInfo>();
    externalDataSourceInfo->SourceType = desc.GetSourceType();
    externalDataSourceInfo->Location = desc.GetLocation();
    externalDataSourceInfo->Installation = desc.GetInstallation();
    externalDataSourceInfo->AlterVersion = alterVersion;
    externalDataSourceInfo->Auth = desc.GetAuth();
    externalDataSourceInfo->Properties = desc.GetProperties();
    return externalDataSourceInfo;
}

} // namespace NKikimr::NSchemeShard::NExternalDataSource
