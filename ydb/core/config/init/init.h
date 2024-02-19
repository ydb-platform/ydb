#pragma once

#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <ydb/core/base/location.h>
#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/cms/console/config_item_info.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/base/path.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/system/hostname.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/generic/maybe.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>

#include <filesystem>

namespace fs = std::filesystem;

// ====
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/lib/ydb_cli/common/common.h>
#include <ydb/library/aclib/aclib.h>
using namespace NYdb::NConsoleClient;
// ===

extern TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig();
extern TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig();

namespace NKikimr::NConfig {

class IEnv {
public:
    virtual ~IEnv() {}
    virtual TString HostName() const = 0;
    virtual TString FQDNHostName() const = 0;
};

class IErrorCollector {
public:
    virtual ~IErrorCollector() {}
    virtual void Fatal(TString error) = 0;
};

class IProtoConfigFileProvider {
public:
    virtual ~IProtoConfigFileProvider() {}
    virtual void AddConfigFile(TString optName, TString description) = 0;
    virtual void RegisterCliOptions(NLastGetopt::TOpts& opts) const = 0;
    virtual TString GetProtoFromFile(const TString& path, IErrorCollector& errorCollector) const = 0;
    virtual bool Has(TString optName) = 0;
    virtual TString Get(TString optName) = 0;
};

class IConfigUpdateTracer {
public:
    virtual ~IConfigUpdateTracer() {}
    virtual void Add(ui32 kind, TConfigItemInfo::TUpdate) = 0;
    virtual THashMap<ui32, TConfigItemInfo> Dump() const = 0;
};

class IInitialConfigurator {
public:
    virtual ~IInitialConfigurator() {};
    virtual void RegisterCliOptions(NLastGetopt::TOpts& opts) = 0;
    virtual void ValidateOptions(const NLastGetopt::TOpts& opts, const NLastGetopt::TOptsParseResult& parseResult) = 0;
    virtual void Parse(const TVector<TString>& freeArgs) = 0;
};

std::unique_ptr<IConfigUpdateTracer> MakeDefaultConfigUpdateTracer();
std::unique_ptr<IProtoConfigFileProvider> MakeDefaultProtoConfigFileProvider();
std::unique_ptr<IEnv> MakeDefaultEnv();
std::unique_ptr<IErrorCollector> MakeDefaultErrorCollector();
std::unique_ptr<IInitialConfigurator> MakeDefaultInitialConfigurator(
        NConfig::IErrorCollector& errorCollector,
        NConfig::IProtoConfigFileProvider& protoConfigFileProvider,
        NConfig::IConfigUpdateTracer& configUpdateTracer,
        NConfig::IEnv& env);

class TInitialConfigurator {
public:
    TInitialConfigurator(
        NConfig::IErrorCollector& errorCollector,
        NConfig::IProtoConfigFileProvider& protoConfigFileProvider,
        NConfig::IConfigUpdateTracer& configUpdateTracer,
        NConfig::IEnv& env)
            : Impl(MakeDefaultInitialConfigurator(errorCollector, protoConfigFileProvider, configUpdateTracer, env))
    {}

    void RegisterCliOptions(NLastGetopt::TOpts& opts) {
        Impl->RegisterCliOptions(opts);
    }

    void ValidateOptions(const NLastGetopt::TOpts& opts, const NLastGetopt::TOptsParseResult& parseResult) {
        Impl->ValidateOptions(opts, parseResult);
    }

    void Parse(const TVector<TString>& freeArgs) {
        Impl->Parse(freeArgs);
    }

private:
    std::unique_ptr<IInitialConfigurator> Impl;
};

} // namespace NKikimr::NConfig
