#include "schemeshard__operation_common_external_data_source.h"

#include <utility>

namespace NKikimr::NSchemeShard::NExternalDataSource {

constexpr uint32_t MAX_FIELD_SIZE    = 1000;
constexpr uint32_t MAX_PROTOBUF_SIZE = 2 * 1024 * 1024; // 2 MiB

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

namespace {

bool CheckSecretIsNotDelegation(const TString& secretName, TStringBuf usage, TSchemeShard* ss, TString& errStr) {
    if (!secretName.StartsWith('/')) {
        return true; // a secret of the metadata provider
    }
    const TPath path = TPath::Resolve(secretName, ss);
    if (!path.IsResolved() || !path.Base()->IsSecret()) {
        return true;
    }
    const auto it = ss->Secrets.find(path.Base()->PathId);
    if (it == ss->Secrets.end() || !it->second || !it->second->Description.HasIamDelegation()) {
        return true;
    }
    errStr = TStringBuilder() << "Secret " << secretName << " is an IAM delegation secret: its value is an IAM token of the delegated"
        << " service account and cannot be used as " << usage << ", reference it with AUTH_METHOD = \"TOKEN\"";
    return false;
}

} // namespace

bool ValidateSecretsUsage(const NKikimrSchemeOp::TAuth& auth, TSchemeShard* ss, TString& errStr) {
    switch (auth.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount:
            return CheckSecretIsNotDelegation(auth.GetServiceAccount().GetSecretName(), "a service account key signature", ss, errStr);
        case NKikimrSchemeOp::TAuth::kMdbBasic:
            return CheckSecretIsNotDelegation(auth.GetMdbBasic().GetServiceAccountSecretName(), "a service account key signature", ss, errStr)
                && CheckSecretIsNotDelegation(auth.GetMdbBasic().GetPasswordSecretName(), "a password", ss, errStr);
        case NKikimrSchemeOp::TAuth::kBasic:
            return CheckSecretIsNotDelegation(auth.GetBasic().GetPasswordSecretName(), "a password", ss, errStr);
        case NKikimrSchemeOp::TAuth::kAws:
            return CheckSecretIsNotDelegation(auth.GetAws().GetAwsAccessKeyIdSecretName(), "an AWS access key id", ss, errStr)
                && CheckSecretIsNotDelegation(auth.GetAws().GetAwsSecretAccessKeySecretName(), "an AWS secret access key", ss, errStr);
        case NKikimrSchemeOp::TAuth::kToken:
        case NKikimrSchemeOp::TAuth::kIam:
        case NKikimrSchemeOp::TAuth::kNone:
        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return true;
    }
    return true;
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
