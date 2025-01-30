#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/profile_manager.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandConfig : public TClientCommandTree {
public:
    TCommandConfig();
    virtual void Config(TConfig& config) override;
};

class TCommandConnectionInfo : public TClientCommand {
public:
    TCommandConnectionInfo();

    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    static void PrintInfo(TConfig& config);
    static void PrintVerboseInfo(TConfig& config);
};

class TCommandProfile : public TClientCommandTree {
public:
    TCommandProfile();
};

class TCommandProfileCommon : public TClientCommand {
public:
    TCommandProfileCommon(const TString& name, const std::initializer_list<TString>& aliases =
            std::initializer_list<TString>(), const TString& description = TString());
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;

protected:
    void ValidateAuth();
    bool AnyProfileOptionInCommandLine();
    void ConfigureProfile(const TString& profileName, std::shared_ptr<IProfileManager> profileManager,
                     TConfig& config, bool interactive, bool cmdLine);

    TString ProfileName, Endpoint, Database, TokenFile, Oauth2KeyFile, YcTokenFile, SaKeyFile,
            IamTokenFile, IamEndpoint, User, PasswordFile, CaCertsFile;

    bool UseMetadataCredentials = false;
    bool AnonymousAuth = false;

private:
    void SetupProfileSetting(const TString& name, const TString& value, bool existingProfile, const TString &profileName,
                             std::shared_ptr<IProfile> profile, bool interactive, bool cmdLine);
    void SetupProfileAuthentication(bool existingProfile, const TString& profileName, std::shared_ptr<IProfile> profile,
                                    TConfig& config, bool interactive, bool cmdLine);
    bool SetAuthFromCommandLine(std::shared_ptr<IProfile> profile);
    void GetOptionsFromStdin();
    void ParseUrl(const TString& url);
};

class TCommandInit : public TCommandProfileCommon {
public:
    TCommandInit();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandCreateProfile : public TCommandProfileCommon {
public:
    TCommandCreateProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    bool Interactive;
};

class TCommandDeleteProfile : public TClientCommand {
public:
    TCommandDeleteProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString ProfileName;
    bool Force = false;
};

class TCommandActivateProfile : public TClientCommand {
public:
    TCommandActivateProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString ProfileName;
};

class TCommandDeactivateProfile : public TClientCommand {
public:
    TCommandDeactivateProfile();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandListProfiles : public TClientCommand {
public:
    TCommandListProfiles();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    bool WithContent = false;
};

class TCommandGetProfile : public TClientCommand {
public:
    TCommandGetProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString ProfileName;
};

class TCommandUpdateProfile : public TCommandProfileCommon {
public:
    TCommandUpdateProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
private:
    void ValidateNoOptions();
    void DropNoOptions(std::shared_ptr<IProfile> profile);
    bool NoEndpoint = false;
    bool NoDatabase = false;
    bool NoAuth = false;
    bool NoIamEndpoint = false;
    bool NoCaCertsFile = false;
};

class TCommandReplaceProfile : public TCommandProfileCommon {
public:
    TCommandReplaceProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
private:
    bool Force = false;
};

}
}
