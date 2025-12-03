#include "secret_settings.h"
#include "sql_expression.h"
#include <yql/essentials/utils/yql_paths.h>

namespace NSQLTranslationV1 {

using namespace NYql;

namespace {
inline TString GetMixingSecretTypesError(const TString& name, const TString& path) {
    return TStringBuilder() << "Usage secrets of different types is not allowed: "
                            << to_upper(name) << " and " << to_upper(path) << " are set";
}
} // namespace

bool VerifyAndAdjustSecretSettings(
    std::map<TString, TNodePtr>& out,
    TContext& ctx,
    const TVector<TSecretSettingsNames>& secretSettings,
    TStringBuf tablePathPrefix)
{
    for (const auto& settings : secretSettings) {
        auto nameIt = out.find(settings.Name);
        auto pathIt = out.find(settings.Path);
        if (nameIt != out.end() && pathIt != out.end()) {
            ctx.Error() << to_upper(settings.Name) << " and " << to_upper(settings.Path) << " are mutually exclusive";
            return false;
        }

        if (pathIt != out.end()) {
            pathIt->second = BuildLiteralRawString(pathIt->second->GetPos(),
                                                   BuildTablePath(tablePathPrefix, pathIt->second->GetLiteralValue()));
        }
    }

    return true;
}

void AdjustSecretPaths(
    std::map<TString, TDeferredAtom>& out,
    const TVector<TSecretSettingsNames>& secretSettings,
    TStringBuf tablePathPrefix)
{
    for (const auto& setting : secretSettings) {
        auto pathIt = out.find(setting.Path);
        if (pathIt != out.end() && pathIt->second.HasNode()) {
            if (auto literal = pathIt->second.GetLiteral()) {
                pathIt->second = TDeferredAtom(pathIt->second.Build()->GetPos(),
                                               BuildTablePath(tablePathPrefix, *literal));
                continue;
            }
        }
    }
}

TExternalDataSourceAuthFields::TExternalDataSourceAuthFields(
    const THashSet<TString>& mandatoryFields,
    const TVector<TSecretSettingsNames>& secretsFields)
    : MandatoryFields_(mandatoryFields)
    , SecretsFields_(secretsFields)
{
    AllPossibleFields_.reserve(MandatoryFields_.size() + 2 * SecretsFields_.size());
    AllPossibleFields_.insert(begin(MandatoryFields_), end(MandatoryFields_));
    for (const auto& fields : SecretsFields_) {
        AllPossibleFields_.insert(fields.Name);
        AllPossibleFields_.insert(fields.Path);
    }
}

bool TExternalDataSourceAuthFields::CheckMandatoryFields(
    TStringBuf authField,
    const std::map<TString, TDeferredAtom>& result) const {
    if (MandatoryFields_.contains(authField) && !result.contains(TString{authField})) {
        return false;
    }

    return true;
}

bool TExternalDataSourceAuthFields::CheckSecretsFields(
    const std::map<TString, TDeferredAtom>& result,
    TString& errMessage) const {
    const TString* nameField = nullptr;
    const TString* pathField = nullptr;
    size_t nameFieldsCnt = 0;
    size_t pathFieldsCnt = 0;
    for (const auto& fields : SecretsFields_) {
        int presentedFields = 0;
        if (result.contains(fields.Name)) {
            nameField = &fields.Name;
            ++nameFieldsCnt;
            ++presentedFields;
        }
        if (result.contains(fields.Path)) {
            pathField = &fields.Path;
            ++presentedFields;
            ++pathFieldsCnt;
        }
        if (presentedFields == 0) {
            errMessage = TStringBuilder() << "A value must be provided for either " << to_upper(fields.Name)
                                          << " or " << to_upper(fields.Path);
            return false;
        } else if (presentedFields == 2) {
            errMessage = GetMixingSecretTypesError(fields.Name, fields.Path);
            return false;
        } else {
            Y_DEBUG_ABORT_UNLESS(presentedFields == 1);
        }
    }

    if (nameFieldsCnt > 0 && pathFieldsCnt > 0) {
        Y_ENSURE(nameField && pathField);
        errMessage = GetMixingSecretTypesError(*nameField, *pathField);
        return false;
    }

    return true;
}

bool TExternalDataSourceAuthFields::CheckAllPossibleFields(
    TStringBuf authField,
    const std::map<TString, TDeferredAtom>& result) const {
    if (!AllPossibleFields_.contains(authField) && result.contains(TString{authField})) {
        return false;
    }
    return true;
}

bool ValidateExternalDataSourceAuthMethod(const std::map<TString, TDeferredAtom>& result, TContext& ctx) {
    const static TSet<TStringBuf> AllAuthFields = [] {
        TSet<TStringBuf> settings = {
            "service_account_id",
            "login",
            "aws_region",
        };
        for (const auto& names : EDS_SECRETS_SETTINGS) {
            settings.insert(names.Name);
            settings.insert(names.Path);
        }

        return settings;
    }();
    const static TMap<TStringBuf, TExternalDataSourceAuthFields> AuthMethodFields{
        {"NONE", TExternalDataSourceAuthFields{}},
        {"SERVICE_ACCOUNT", TExternalDataSourceAuthFields(
                                {"service_account_id"},
                                {
                                    TSecretSettingsNames("service_account_secret"),
                                })},
        {"BASIC", TExternalDataSourceAuthFields(
                      {"login"},
                      {
                          TSecretSettingsNames("password_secret"),
                      })},
        {"AWS", TExternalDataSourceAuthFields(
                    {"aws_region"},
                    {TSecretSettingsNames("aws_access_key_id_secret"),
                     TSecretSettingsNames("aws_secret_access_key_secret")})},
        {"MDB_BASIC", TExternalDataSourceAuthFields(
                          {"service_account_id", "login"},
                          {
                              TSecretSettingsNames("service_account_secret"),
                              TSecretSettingsNames("password_secret"),
                          })},
        {"TOKEN", TExternalDataSourceAuthFields(
                      {},
                      {
                          TSecretSettingsNames("token_secret"),
                      })}};

    auto authMethodIt = result.find("auth_method");
    if (authMethodIt == result.end() || authMethodIt->second.GetLiteral() == nullptr) {
        ctx.Error() << "AUTH_METHOD requires key";
        return false;
    }

    const auto& authMethod = *authMethodIt->second.GetLiteral();
    auto it = AuthMethodFields.find(authMethod);
    if (it == AuthMethodFields.end()) {
        ctx.Error() << "Unknown AUTH_METHOD = " << authMethod;
        return false;
    }

    const auto& currentAuthFields = it->second;
    for (const auto& authField : AllAuthFields) {
        if (!currentAuthFields.CheckMandatoryFields(authField, result)) {
            ctx.Error() << to_upper(TString{authField}) << " requires key";
            return false;
        }
    }

    for (const auto& authField : AllAuthFields) {
        TString errMessage;
        if (!currentAuthFields.CheckSecretsFields(result, errMessage)) {
            ctx.Error() << errMessage;
            return false;
        }

        if (!currentAuthFields.CheckAllPossibleFields(authField, result)) {
            ctx.Error() << to_upper(TString{authField}) << " key is not supported for AUTH_METHOD = " << authMethod;
            return false;
        }
    }

    return true;
}

} // namespace NSQLTranslationV1
