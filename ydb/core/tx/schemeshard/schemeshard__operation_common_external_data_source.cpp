#include "schemeshard__operation_common_external_data_source.h"

#include <utility>

namespace NKikimr::NSchemeShard::NExternalDataSource {

constexpr uint32_t MAX_FIELD_SIZE    = 1000;
constexpr uint32_t MAX_PROTOBUF_SIZE = 2 * 1024 * 1024; // 2 MiB

bool ValidateLocationAndInstallation(const TString& location,
                                     const TString& installation,
                                     TString& errStr) {
    if (location.Size() > MAX_FIELD_SIZE) {
        errStr =
            Sprintf("Maximum length of location must be less or equal equal to %u but got %lu",
                    MAX_FIELD_SIZE,
                    location.Size());
        return false;
    }
    if (installation.Size() > MAX_FIELD_SIZE) {
        errStr = Sprintf(
            "Maximum length of installation must be less or equal equal to %u but got %lu",
            MAX_FIELD_SIZE,
            installation.Size());
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
        case NKikimrSchemeOp::TAuth::kNone:
            return CheckAuth("NONE", availableAuthMethods, errStr);
    }
    return false;
}

bool Validate(const NKikimrSchemeOp::TExternalDataSourceDescription& desc,
              const NExternalSource::IExternalSourceFactory::TPtr& factory,
              TString& errStr) {
    try {
        const auto source = factory->GetOrCreate(desc.GetSourceType());
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

TExternalDataSourceInfo::TPtr CreateExternalDataSource(
    const NKikimrSchemeOp::TExternalDataSourceDescription& desc, ui64 alterVersion) {
    TExternalDataSourceInfo::TPtr externalDataSoureInfo = new TExternalDataSourceInfo;
    externalDataSoureInfo->SourceType                   = desc.GetSourceType();
    externalDataSoureInfo->Location                     = desc.GetLocation();
    externalDataSoureInfo->Installation                 = desc.GetInstallation();
    externalDataSoureInfo->AlterVersion                 = alterVersion;
    externalDataSoureInfo->Auth.CopyFrom(desc.GetAuth());
    externalDataSoureInfo->Properties.CopyFrom(desc.GetProperties());
    return externalDataSoureInfo;
}

} // namespace NKikimr::NSchemeShard::NExternalDataSource
