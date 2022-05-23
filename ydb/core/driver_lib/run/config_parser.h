#pragma once

#include "config.h"
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/messagebus/session_config.h>
#include <library/cpp/messagebus/queue_config.h>

namespace NKikimr {

class TRunCommandConfigParser {
public:
    TRunCommandConfigParser(TKikimrRunConfig& config);
    virtual ~TRunCommandConfigParser();

    virtual void ParseRunOpts(int argc, char **argv);

    virtual void SetupGlobalOpts(NLastGetopt::TOpts& opts);
    virtual void ParseGlobalOpts(const NLastGetopt::TOptsParseResult& res);
    virtual void SetupLastGetOptForConfigFiles(NLastGetopt::TOpts& opts);
    virtual void ParseConfigFiles(const NLastGetopt::TOptsParseResult& res);
    virtual void ApplyParsedOptions();

protected:
    struct TGlobalOpts {
        bool StartTcp; // interconnect config
        bool SysLog; // log settings
        ui32 LogLevel; // log settings
        ui32 LogSamplingLevel; // log settings
        ui32 LogSamplingRate; // log settings
        TString LogFormat;// log settings
        TString ClusterName; // log settings
        TString UDFsDir;  // directory to recurcively load UDFs from
        TVector<TString> UDFsPaths; // fine tunned UDFs list to load

        TGlobalOpts();
    };

    struct TRunOpts {
        ui32 NodeId;
        bool StartBusProxy;
        ui32 BusProxyPort;
        NBus::TBusQueueConfig ProxyBusQueueConfig;
        NBus::TBusServerSessionConfig ProxyBusSessionConfig;
        TVector<ui64> ProxyBindToProxy;
        ui32 MonitoringPort;
        TString MonitoringAddress;
        TString MonitoringCertificateFile;
        ui32 MonitoringThreads;
        TString RestartsCountFile;
        bool StartTracingBusProxy;
        TString TracePath;
        size_t CompileInflightLimit; // MiniKQLCompileService

        TRunOpts();
    };

    TKikimrRunConfig& Config;
    TGlobalOpts       GlobalOpts;
    TRunOpts          RunOpts;
};

}
