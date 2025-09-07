#include "init.h"

namespace {

class TNullStream : public IOutputStream {
    void DoWrite(const void*, size_t) override {}
};

TNullStream NullStream;

} // anonymous namespace

namespace NKikimr::NConfig {

class TNoopInitLogger
    : public IInitLogger
{
public:
    IOutputStream& Out() const noexcept override {
        return NullStream;
    }

    IOutputStream& Err() const noexcept override {
        return NullStream;
    }
};

class TNoopDynConfigClient
    : public IDynConfigClient
{
public:
    std::shared_ptr<IConfigurationResult> GetConfig(
        const TGrpcSslSettings&,
        const TVector<TString>&,
        const TDynConfigSettings&,
        const IEnv&,
        IInitLogger&) const override
    {
        return nullptr;
    }
};

class TNoopNodeBrokerClient
    : public INodeBrokerClient
{
public:
    std::shared_ptr<INodeRegistrationResult> RegisterDynamicNode(
        const TGrpcSslSettings&,
        const TVector<TString>&,
        const TNodeRegistrationSettings&,
        const IEnv&,
        IInitLogger&) const override
    {
        return nullptr;
    }
};

class TNoopMemLogInitializer
    : public IMemLogInitializer
{
public:
    void Init(const NKikimrConfig::TMemoryLogConfig&) const override {}
};

class TNoopConfigClient
    : public IConfigClient
{
public:
    std::shared_ptr<IStorageConfigResult> FetchConfig(
        const TGrpcSslSettings&,
        const TVector<TString>&,
        const IEnv&,
        IInitLogger&) const override
    {
        return nullptr;
    }
};

std::unique_ptr<IMemLogInitializer> MakeNoopMemLogInitializer() {
    return std::make_unique<TNoopMemLogInitializer>();
}

std::unique_ptr<INodeBrokerClient> MakeNoopNodeBrokerClient() {
    return std::make_unique<TNoopNodeBrokerClient>();
}

std::unique_ptr<IDynConfigClient> MakeNoopDynConfigClient() {
    return std::make_unique<TNoopDynConfigClient>();
}

std::unique_ptr<IConfigClient> MakeNoopConfigClient() {
    return std::make_unique<TNoopConfigClient>();
}

std::unique_ptr<IInitLogger> MakeNoopInitLogger() {
    return std::make_unique<TNoopInitLogger>();
}

} // namespace NKikimr::NConfig
