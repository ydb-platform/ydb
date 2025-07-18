#pragma once

#include "init.h"
#include "init_impl.h"

namespace NKikimr::NConfig {

class TEnvMock
    : public IEnv
{
public:
    TString HostName() const override {
        return SavedHostName;
    }

    TString FQDNHostName() const override {
        return SavedFQDNHostName;
    }

    TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty = true) const override {
        Y_UNUSED(allowEmpty);
        if (auto it = SavedFiles.find({filePath, fileName}); it != SavedFiles.end()) {
            return it->second;
        }
        return ""; // TODO throw
    }

    void Sleep(const TDuration&) const override {
        return;
    }

    TString SavedHostName;
    TString SavedFQDNHostName;
    TMap<std::pair<TString, TString>, TString> SavedFiles;
};

class TEnvRecorder
    : public IEnv
{
    IEnv& Impl;
    mutable TEnvMock Mock;
public:
    TEnvRecorder(IEnv& impl)
        : Impl(impl)
    {}

    TString HostName() const override {
        Mock.SavedHostName = Impl.HostName();
        return Mock.SavedHostName;
    }

    TString FQDNHostName() const override {
        Mock.SavedFQDNHostName = Impl.FQDNHostName();
        return Mock.SavedFQDNHostName;
    }

    TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty = true) const override {
        auto& File = Mock.SavedFiles[{filePath, fileName}];
        File = Impl.ReadFromFile(filePath, fileName, allowEmpty);
        return File;
    }

    void Sleep(const TDuration& dur) const override {
        return Impl.Sleep(dur);
    }

    TEnvMock GetMock() const {
        return Mock;
    }
};

class TDynConfigClientMock
    : public IDynConfigClient
{
public:
    std::shared_ptr<IConfigurationResult> GetConfig(
        const TGrpcSslSettings& gs,
        const TVector<TString>& addrs,
        const TDynConfigSettings& settings,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        Y_UNUSED(gs, addrs, settings, env, logger); // TODO validate unchanged
        return SavedResult;
    }

    std::shared_ptr<IConfigurationResult> SavedResult;
};

class TDynConfigClientRecorder
    : public IDynConfigClient
{
    IDynConfigClient& Impl;
    mutable TDynConfigClientMock Mock;
public:
    TDynConfigClientRecorder(IDynConfigClient& impl)
        : Impl(impl)
    {}

    std::shared_ptr<IConfigurationResult> GetConfig(
        const TGrpcSslSettings& gs,
        const TVector<TString>& addrs,
        const TDynConfigSettings& settings,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        Mock.SavedResult = Impl.GetConfig(gs, addrs, settings, env, logger);
        return Mock.SavedResult;
    }

    TDynConfigClientMock GetMock() const {
        return Mock;
    }
};

class TNodeBrokerClientMock
    : public INodeBrokerClient
{
public:
    std::shared_ptr<INodeRegistrationResult> RegisterDynamicNode(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const TNodeRegistrationSettings& regSettings,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        Y_UNUSED(grpcSettings, addrs, regSettings, env, logger);
        return SavedResult;
    }

    std::shared_ptr<INodeRegistrationResult> SavedResult;
};

class TNodeBrokerClientRecorder
    : public INodeBrokerClient
{
    INodeBrokerClient& Impl;
    mutable TNodeBrokerClientMock Mock;
public:
    TNodeBrokerClientRecorder(INodeBrokerClient& impl)
        : Impl(impl)
    {}

    std::shared_ptr<INodeRegistrationResult> RegisterDynamicNode(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const TNodeRegistrationSettings& regSettings,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        Mock.SavedResult = Impl.RegisterDynamicNode(grpcSettings, addrs, regSettings, env, logger);
        return Mock.SavedResult;
    }

    TNodeBrokerClientMock GetMock() const {
        return Mock;
    }
};

class TProtoConfigFileProviderMock
    : public IProtoConfigFileProvider
{
public:
    void AddConfigFile(TString optName, TString description) override {
        SavedOpts[optName] = MakeSimpleShared<TFileConfigOptions>(TFileConfigOptions{.Description = description});
    }

    void RegisterCliOptions(NLastGetopt::TOpts& opts) const override {
        for (const auto& [name, opt] : SavedOpts) {
            opts.AddLongOption(name, opt->Description)
                .OptionalArgument("PATH")
                .StoreResult(&opt->ParsedOption);
        }
    }

    TString GetProtoFromFile(const TString& path, IErrorCollector& errorCollector) const override {
        Y_UNUSED(path, errorCollector, SavedFiles);
        return SavedFiles.at(path);
    }

    bool Has(TString optName) override {
        if (auto* opt = SavedOpts.FindPtr(optName)) {
            return !!((*opt)->ParsedOption);
        }
        return false;
    }

    TString Get(TString optName) override {
        if (auto* opt = SavedOpts.FindPtr(optName); opt && (*opt)->ParsedOption) {
            return (*opt)->ParsedOption.GetRef();
        }
        ythrow yexception() << "option " << optName.Quote() << " undefined";
    }

    TMap<TString, TSimpleSharedPtr<TFileConfigOptions>> SavedOpts;
    TMap<TString, TString> SavedFiles;
};


class TProtoConfigFileProviderRecorder
    : public IProtoConfigFileProvider
{
    IProtoConfigFileProvider& Impl;
    mutable TProtoConfigFileProviderMock Mock;
public:
    TProtoConfigFileProviderRecorder(IProtoConfigFileProvider& impl)
        : Impl(impl)
    {}

    void AddConfigFile(TString optName, TString description) override {
        Mock.AddConfigFile(optName, description);
        Impl.AddConfigFile(optName, description);
    }

    void RegisterCliOptions(NLastGetopt::TOpts& opts) const override {
        Impl.RegisterCliOptions(opts);
    }

    TString GetProtoFromFile(const TString& path, IErrorCollector& errorCollector) const override {
        Mock.SavedFiles[path] = Impl.GetProtoFromFile(path, errorCollector);
        return Mock.SavedFiles[path];
    }

    bool Has(TString optName) override {
        return Impl.Has(optName);
    }

    TString Get(TString optName) override {
        return Impl.Get(optName);
    }

    TProtoConfigFileProviderMock GetMock() const {
        return Mock;
    }
};

class TConfigClientMock
    : public IConfigClient
{
public:
    std::shared_ptr<IStorageConfigResult> FetchConfig(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        Y_UNUSED(grpcSettings, addrs, env, logger);
        return SavedResult;
    }

    std::shared_ptr<IStorageConfigResult> SavedResult;
};

class TConfigClientRecorder
    : public IConfigClient
{
    IConfigClient& Impl;
    mutable TConfigClientMock Mock;
public:
    TConfigClientRecorder(IConfigClient& impl)
        : Impl(impl)
    {}

    std::shared_ptr<IStorageConfigResult> FetchConfig(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        Mock.SavedResult = Impl.FetchConfig(grpcSettings, addrs, env, logger);
        return Mock.SavedResult;
    }

    TConfigClientMock GetMock() const {
        return Mock;
    }
};

} // namespace NKikimr::NConfig
