#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/profile_manager.h>

namespace NYdb {
namespace NConsoleClient {

std::shared_ptr<IProfileManager> CreateYdbProfileManager(const TString& ydbDir);

class TCommandConfig : public TClientCommandTree {
public:
    TCommandConfig();
    virtual void Config(TConfig& config) override;
};

class TCommandProfile : public TClientCommandTree {
public:
    TCommandProfile();
};

class TCommandInit : public TClientCommand {
public:
    TCommandInit();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandProfileCommon : public TClientCommand {
public:
    TCommandProfileCommon(const TString& name, const std::initializer_list<TString>& aliases =
            std::initializer_list<TString>(), const TString& description = TString());
    virtual void Config(TConfig& config) override;

protected:
    void ValidateAuth(TConfig& config);
    bool AnyProfileOptionInCommandLine(TConfig& config);
    TString ProfileName;
    bool UseMetadataCredentials = false;
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
    void ValidateNoOptions(TConfig& config);
    void DropNoOptions(std::shared_ptr<IProfile> profile);
    bool NoEndpoint = false;
    bool NoDatabase = false;
    bool NoAuth = false;
    bool NoIamEndpoint = false;
};

class TCommandReplaceProfile : public TCommandProfileCommon {
public:
    TCommandReplaceProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

}
}
