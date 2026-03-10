#pragma once
#include "node.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <map>

namespace NSQLTranslationV1 {

struct TSecretSettingsNames {
    const TString Name;
    const TString Path;

    explicit TSecretSettingsNames(const TString& prefix)
        : Name(prefix + "_name")
        , Path(prefix + "_path")
    {
    }
};

static const TVector<TSecretSettingsNames> EDS_SECRETS_SETTINGS = {
    TSecretSettingsNames("token_secret"),
    TSecretSettingsNames("password_secret"),
    TSecretSettingsNames("service_account_secret"),
    TSecretSettingsNames("aws_access_key_id_secret"),
    TSecretSettingsNames("aws_secret_access_key_secret"),
};

static const TVector<TSecretSettingsNames> REPLICATION_AND_TRANSFER_SECRETS_SETTINGS = {
    TSecretSettingsNames("token_secret"),
    TSecretSettingsNames("password_secret"),
    TSecretSettingsNames("initial_token_secret"),
};

/**
 * Verifies that paired secrets settings, passed in @nameAndPathSettingsNames param, are mutually exclusive
 * Adds @tablePathPrefix value to secret path if path is not absolute
 */
bool VerifyAndAdjustSecretSettings(
    std::map<TString, TNodePtr>& out,
    TContext& ctx,
    const TVector<TSecretSettingsNames>& secretSettings,
    TStringBuf tablePathPrefix);

/**
 * Adds @tablePathPrefix value to secret path if path is not absolute
 */
void AdjustSecretPaths(
    std::map<TString, TDeferredAtom>& out,
    const TVector<TSecretSettingsNames>& secretSettings,
    TStringBuf tablePathPrefix);

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
class TExternalDataSourceAuthFields final {
public:
    TExternalDataSourceAuthFields() = default;
    TExternalDataSourceAuthFields(const THashSet<TString>& mandatoryFields, const TVector<TSecretSettingsNames>& secretsFields);

    bool CheckMandatoryFields(TStringBuf authField, const std::map<TString, TDeferredAtom>& result) const;

    /*
     * Checks that names and paths are not mixed
     */
    bool CheckSecretsFields(const std::map<TString, TDeferredAtom>& result, TString& errMessage) const;

    bool CheckAllPossibleFields(TStringBuf authField, const std::map<TString, TDeferredAtom>& result) const;

private:
    const THashSet<TString> MandatoryFields_;
    const TVector<TSecretSettingsNames> SecretsFields_;
    THashSet<TString> AllPossibleFields_;
};

bool ValidateExternalDataSourceAuthMethod(const std::map<TString, TDeferredAtom>& result, TContext& ctx);

} // namespace NSQLTranslationV1
