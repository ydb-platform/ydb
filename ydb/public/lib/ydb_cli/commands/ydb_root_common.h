#pragma once

#include <ydb/public/lib/ydb_cli/common/profile_manager.h>
#include <ydb/public/lib/ydb_cli/common/root.h>

namespace NYdb {
namespace NConsoleClient {

struct TClientSettings {
    // Whether to use secure connection or not
    TMaybe<bool> EnableSsl;
    // Whether to use access token in auth options or not
    TMaybe<bool> UseAccessToken;
    // Whether to use default token file in auth options or not
    TMaybe<bool> UseDefaultTokenFile;
    // Whether to use IAM authentication (Yandex.Cloud) or not
    TMaybe<bool> UseIamAuth;
    // Whether to use static credentials (user/password) or not
    TMaybe<bool> UseStaticCredentials;
    // Whether to use OAuth 2.0 token exchange credentials or not
    TMaybe<bool> UseOauth2TokenExchange;
    // Whether to use export to YT command or not
    TMaybe<bool> UseExportToYt;
    // Whether to mention user account in --help command or not
    TMaybe<bool> MentionUserAccount;
    // Name of a directory in user home directory to save profile config
    TString YdbDir;
};

class TClientCommandRootCommon : public TClientCommandRootBase {
public:
    TClientCommandRootCommon(const TString& name, const TClientSettings& settings);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    void ParseAddress(TConfig& config) override;
    void ParseCredentials(TConfig& config) override;
    void Validate(TConfig& config) override;
    int Run(TConfig& config) override;
protected:
    virtual void FillConfig(TConfig& config);
    virtual void SetCredentialsGetter(TConfig& config);

private:
    void ValidateSettings();
    bool GetCredentialsFromProfile(std::shared_ptr<IProfile> profile, TConfig& config, bool explicitOption);

    void ParseProfile();
    void ParseDatabase(TConfig& config);
    void ParseIamEndpoint(TConfig& config);
    void ParseCaCerts(TConfig& config) override;
    void GetAddressFromString(TConfig& config, TString* result = nullptr);
    bool ParseProtocolNoConfig(TString& message);
    void GetCaCerts(TConfig& config);
    bool TryGetParamFromProfile(const TString& name, std::shared_ptr<IProfile> profile, bool explicitOption,
                                std::function<bool(const TString&, const TString&, bool)> callback);

    TString Database;

    ui32 VerbosityLevel = 0;
    bool IsVerbose() const {
        return VerbosityLevel > 0;
    }

    TString ProfileName;
    TString ProfileFile;
    std::shared_ptr<IProfile> Profile;
    std::shared_ptr<IProfileManager> ProfileManager;

    TString UserName;
    TString PasswordFile;
    bool DoNotAskForPassword = false;

    bool UseMetadataCredentials = false;
    TString YCToken;
    TString YCTokenFile;
    TString SaKeyFile;
    TString IamEndpoint;
    const TClientSettings& Settings;
    TVector<TString> MisuseErrors;

    TString Oauth2KeyFile;

    bool IsAddressSet = false;
    bool IsDatabaseSet = false;
    bool IsIamEndpointSet = false;
    bool IsCaCertsFileSet = false;
    bool IsAuthSet = false;
};

}
}
