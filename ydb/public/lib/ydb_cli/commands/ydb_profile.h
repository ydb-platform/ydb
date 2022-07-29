#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/profile_manager.h>

namespace NYdb {
namespace NConsoleClient {

std::shared_ptr<IProfileManager> CreateYdbProfileManager(const TString& ydbDir);

class TCommandConfig : public TClientCommandTree {
public:
    TCommandConfig();
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

class TCommandCreateProfile : public TClientCommand {
public:
    TCommandCreateProfile();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString ProfileName;
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

}
}
