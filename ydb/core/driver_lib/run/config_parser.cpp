#include "config_parser.h"

#include <ydb/library/actors/core/log_settings.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/protos/alloc.pb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/compile_service_config.pb.h>

#include <ydb/core/config/init/dummy.h>

#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/system/hostname.h>
#include <util/string/printf.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {

TRunCommandConfigParser::TGlobalOpts::TGlobalOpts()
    : StartTcp(false)
    , SysLog(false)
    , LogLevel(NActors::NLog::PRI_WARN)
    , LogSamplingLevel(NActors::NLog::PRI_DEBUG)
    , LogSamplingRate(0)
{}

TRunCommandConfigParser::TRunOpts::TRunOpts()
    : NodeId(0)
    , StartBusProxy(false)
    , BusProxyPort(NMsgBusProxy::TProtocol::DefaultPort)
    , MonitoringPort(0)
    , MonitoringAddress()
    , MonitoringThreads(10)
    , RestartsCountFile("")
    , StartTracingBusProxy(true)
    , CompileInflightLimit(100000)
{}


TRunCommandConfigParser::TRunCommandConfigParser(TKikimrRunConfig& config)
    : Config(config)
{}

TRunCommandConfigParser::~TRunCommandConfigParser()
{}

void TRunCommandConfigParser::SetupLastGetOptForConfigFiles(NLastGetopt::TOpts& opts) {
    // todo: file configs must be paired with url to request basic values
    opts.AddLongOption('u', "url-base", "url base to request configs from").OptionalArgument("URL");
    opts.AddLongOption("sys-file", "actor system config file (use dummy config by default)").OptionalArgument("PATH");
    opts.AddLongOption("naming-file", "static nameservice config file").OptionalArgument("PATH");
    opts.AddLongOption("domains-file", "domain config file").OptionalArgument("PATH").Required();
    opts.AddLongOption("bs-file", "blobstorage config file").OptionalArgument("PATH");
    opts.AddLongOption("log-file", "log config file").OptionalArgument("PATH");
    opts.AddLongOption("ic-file", "interconnect config file").OptionalArgument("PATH");
    opts.AddLongOption("channels-file", "tablet channel profile config file").OptionalArgument("PATH").Required();
    opts.AddLongOption("vdisk-file", "vdisk kind config file").OptionalArgument("PATH");
    opts.AddLongOption("drivemodel-file", "drive model config file").OptionalArgument("PATH");
    opts.AddLongOption("kqp-file", "Kikimr Query Processor config file").OptionalArgument("PATH");
    opts.AddLongOption("incrhuge-file", "incremental huge blob keeper config file").OptionalArgument("PATH");
    opts.AddLongOption("memorylog-file", "set buffer size for memory log").OptionalArgument("PATH");
    opts.AddLongOption("grpc-file", "gRPC config file").OptionalArgument("PATH");
    opts.AddLongOption("grpc-port", "enable gRPC server on port").RequiredArgument("PORT");
    opts.AddLongOption("grpcs-port", "enable gRPC SSL server on port").RequiredArgument("PORT");
    opts.AddLongOption("kafka-port", "enable kafka proxy server on port").OptionalArgument("PORT");
    opts.AddLongOption("grpc-public-host", "set public gRPC host for discovery").RequiredArgument("HOST");
    opts.AddLongOption("grpc-public-port", "set public gRPC port for discovery").RequiredArgument("PORT");
    opts.AddLongOption("grpcs-public-port", "set public gRPC SSL port for discovery").RequiredArgument("PORT");
    opts.AddLongOption("pq-file", "PQ config file").OptionalArgument("PATH");
    opts.AddLongOption("pqcd-file", "PQCD config file").OptionalArgument("PATH");
    opts.AddLongOption("netclassifier-file", "NetClassifier config file").OptionalArgument("PATH");
    opts.AddLongOption("auth-file", "authorization config file").OptionalArgument("PATH");
    opts.AddLongOption("auth-token-file", "authorization token config file").OptionalArgument("PATH");
    opts.AddLongOption("key-file", "encryption key config file").OptionalArgument("PATH");
    opts.AddLongOption("sqs-file", "SQS config file").OptionalArgument("PATH");
    opts.AddLongOption("http-proxy-file", "Http proxy config file").OptionalArgument("PATH");
    opts.AddLongOption("alloc-file", "Allocator config file").OptionalArgument("PATH");
    opts.AddLongOption("yql-file", "Yql Analytics config file").OptionalArgument("PATH");
    opts.AddLongOption("fq-file", "Federated Query config file").OptionalArgument("PATH");
    opts.AddLongOption("pdisk-key-file", "pdisk encryption key config file").OptionalArgument("PATH");
    opts.AddLongOption("public-http-file", "Public HTTP config file").OptionalArgument("PATH");
}

void TRunCommandConfigParser::ParseConfigFiles(const NLastGetopt::TOptsParseResult& res) {
    if (res.Has("sys-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("sys-file"), Config.AppConfig.MutableActorSystemConfig()));
    } else {
        auto sysConfig = DummyActorSystemConfig();
        Config.AppConfig.MutableActorSystemConfig()->CopyFrom(*sysConfig);
    }

    if (res.Has("naming-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("naming-file"), Config.AppConfig.MutableNameserviceConfig()));
    }

    if (res.Has("domains-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("domains-file"), Config.AppConfig.MutableDomainsConfig()));
    }

    if (res.Has("bs-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("bs-file"), Config.AppConfig.MutableBlobStorageConfig()));
    }

    if (res.Has("log-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("log-file"), Config.AppConfig.MutableLogConfig()));
    }

    if (res.Has("ic-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("ic-file"), Config.AppConfig.MutableInterconnectConfig()));
    }

    if (res.Has("channels-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("channels-file"), Config.AppConfig.MutableChannelProfileConfig()));
    }

    if (res.Has("bootstrap-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("bootstrap-file"), Config.AppConfig.MutableBootstrapConfig()));
    }

    if (res.Has("vdisk-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("vdisk-file"), Config.AppConfig.MutableVDiskConfig()));
    }

    if (res.Has("drivemodel-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("drivemodel-file"), Config.AppConfig.MutableDriveModelConfig()));
    }

    if (res.Has("kqp-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("kqp-file"), Config.AppConfig.MutableKQPConfig()));
    }

    if (res.Has("incrhuge-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("incrhuge-file"), Config.AppConfig.MutableIncrHugeConfig()));
    }

    if (res.Has("grpc-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("grpc-file"), Config.AppConfig.MutableGRpcConfig()));
    }

    if (res.Has("feature-flags-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("feature-flags-file"), Config.AppConfig.MutableFeatureFlags(), true));
    }

    if (res.Has("sqs-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("sqs-file"), Config.AppConfig.MutableSqsConfig()));
    }

    if (res.Has("http-proxy-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("http-proxy-file"), Config.AppConfig.MutableHttpProxyConfig()));
    }

    if (res.Has("grpc-port")) {
        auto& conf = *Config.AppConfig.MutableGRpcConfig();
        conf.SetStartGRpcProxy(true);
        conf.SetPort(FromString<ui16>(res.Get("grpc-port")));
    }

    if (res.Has("grpcs-port")) {
        auto& conf = *Config.AppConfig.MutableGRpcConfig();
        conf.SetStartGRpcProxy(true);
        conf.SetSslPort(FromString<ui16>(res.Get("grpcs-port")));
    }

    if (res.Has("kafka-port")) {
        auto& conf = *Config.AppConfig.MutableKafkaProxyConfig();
        conf.SetListeningPort(FromString<ui16>(res.Get("kafka-port")));
    }

    if (res.Has("grpc-public-host")) {
        auto& conf = *Config.AppConfig.MutableGRpcConfig();
        conf.SetPublicHost(res.Get("grpc-public-host"));
    }

    if (res.Has("grpc-public-port")) {
        auto& conf = *Config.AppConfig.MutableGRpcConfig();
        conf.SetPublicPort(FromString<ui16>(res.Get("grpc-public-port")));
    }

    if (res.Has("grpcs-public-port")) {
        auto& conf = *Config.AppConfig.MutableGRpcConfig();
        conf.SetPublicSslPort(FromString<ui16>(res.Get("grpcs-public-port")));
    }

    if (res.Has("pq-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("pq-file"), Config.AppConfig.MutablePQConfig()));
    }

    if (res.Has("pqcd-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("pqcd-file"), Config.AppConfig.MutablePQClusterDiscoveryConfig()));
    }

    if (res.Has("netclassifier-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("netclassifier-file"), Config.AppConfig.MutableNetClassifierConfig()));
    }

    if (res.Has("auth-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("auth-file"), Config.AppConfig.MutableAuthConfig()));
    }

    if (res.Has("auth-token-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("auth-token-file"), Config.AppConfig.MutableAuthConfig()));
    }

    if (res.Has("key-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("key-file"), Config.AppConfig.MutableKeyConfig()));
    }

    if (res.Has("pdisk-key-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("pdisk-key-file"), Config.AppConfig.MutablePDiskKeyConfig()));
    }

    if (res.Has("alloc-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("alloc-file"), Config.AppConfig.MutableAllocatorConfig()));
    } else {
        auto allocConfig = DummyAllocatorConfig();
        Config.AppConfig.MutableAllocatorConfig()->CopyFrom(*allocConfig);
    }

    if (res.Has("fq-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("fq-file"), Config.AppConfig.MutableFederatedQueryConfig()));
    }

    if (res.Has("public-http-file")) {
        Y_ABORT_UNLESS(ParsePBFromFile(res.Get("public-http-file"), Config.AppConfig.MutablePublicHttpConfig()));
    }
}

void TRunCommandConfigParser::SetupGlobalOpts(NLastGetopt::TOpts& opts) {

    opts.AddLongOption("cluster-name", "which cluster this node belongs to")
        .DefaultValue("unknown").OptionalArgument("STR").StoreResult(&GlobalOpts.ClusterName);
    opts.AddLongOption("log-level", "default logging level").OptionalArgument("1-7")
        .DefaultValue(ToString(GlobalOpts.LogLevel)).StoreResult(&GlobalOpts.LogLevel);
    opts.AddLongOption("log-sampling-level", "sample logs equal to or above this level").OptionalArgument("1-7")
        .DefaultValue(ToString(GlobalOpts.LogSamplingLevel)).StoreResult(&GlobalOpts.LogSamplingLevel);
    opts.AddLongOption("log-sampling-rate",
                       "log only each Nth message with priority matching sampling level; 0 turns log sampling off")
        .OptionalArgument(Sprintf("0,%" PRIu32, Max<ui32>()))
        .DefaultValue(ToString(GlobalOpts.LogSamplingRate)).StoreResult(&GlobalOpts.LogSamplingRate);
    opts.AddLongOption("log-format", "log format to use; short skips the priority and timestamp")
        .DefaultValue("full").OptionalArgument("full|short|json").StoreResult(&GlobalOpts.LogFormat);
    opts.AddLongOption("syslog", "send to syslog instead of stderr").NoArgument();
    opts.AddLongOption("tcp", "start tcp interconnect").NoArgument();
    opts.AddLongOption("udf", "Load shared library with UDF by given path").AppendTo(&GlobalOpts.UDFsPaths);
    opts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory").StoreResult(&GlobalOpts.UDFsDir);
}


void TRunCommandConfigParser::ParseGlobalOpts(const NLastGetopt::TOptsParseResult& res) {
    if (res.Has("tcp"))
        GlobalOpts.StartTcp = true;

    if (res.Has("syslog"))
        GlobalOpts.SysLog = true;
}

void TRunCommandConfigParser::ParseRunOpts(int argc, char **argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();

    opts.AddLongOption('n', "node", "Node ID")
            .RequiredArgument(Sprintf("NUM (0,%d]", NActors::TActorId::MaxNodeId)).Required().StoreResult(&RunOpts.NodeId);
    opts.AddLongOption("proxy", "Bind to proxy(-ies)").RequiredArgument("ADDR").AppendTo(&RunOpts.ProxyBindToProxy);
    opts.AddLongOption("mon-port", "Monitoring port").OptionalArgument("NUM").StoreResult(&RunOpts.MonitoringPort);
    opts.AddLongOption("mon-address", "Monitoring address").OptionalArgument("ADDR").StoreResult(&RunOpts.MonitoringAddress);
    opts.AddLongOption("mon-cert", "Monitoring certificate (https)").OptionalArgument("PATH").StoreResult(&RunOpts.MonitoringCertificateFile);
    opts.AddLongOption("mon-threads", "Monitoring http server threads").RequiredArgument("NUM").StoreResult(&RunOpts.MonitoringThreads);

    SetupLastGetOptForConfigFiles(opts);
    opts.AddLongOption("bootstrap-file", "Bootstrap config file").OptionalArgument("PATH");
    opts.AddLongOption("feature-flags-file", "File with feature flags to turn new features on/off").OptionalArgument("PATH");
    opts.AddLongOption('r', "restarts-count-file", "State for restarts monitoring counter,\nuse empty string to disable\n")
            .OptionalArgument("PATH").DefaultValue(RunOpts.RestartsCountFile).StoreResult(&RunOpts.RestartsCountFile);
    opts.AddLongOption("compile-inflight-limit", "Limit on parallel programs compilation").OptionalArgument("NUM").StoreResult(&RunOpts.CompileInflightLimit);
    opts.AddHelpOption('h');
    opts.SetFreeArgTitle(0, "mbus", "- use to start message bus proxy and override it's settings");
    opts.SetFreeArgsMin(0);
    opts.ArgPermutation_ = REQUIRE_ORDER;

    TOptsParseResult res(&opts, argc, argv);
    ParseConfigFiles(res);

    if ((res.GetFreeArgCount() > 0) && (res.GetFreeArgs().at(0) == "mbus")) {
        size_t freeArgsPos = res.GetFreeArgsPos();
        argc -= freeArgsPos;
        argv += freeArgsPos;
        TOpts msgBusOpts = TOpts::Default();
        SetMsgBusDefaults(RunOpts.ProxyBusSessionConfig, RunOpts.ProxyBusQueueConfig);
        msgBusOpts.AddLongOption("port", "Message bus proxy port").OptionalArgument("NUM")
                .DefaultValue(ToString(RunOpts.BusProxyPort)).StoreResult(&RunOpts.BusProxyPort);
        msgBusOpts.AddLongOption("trace-path", "Path for trace files").Optional().RequiredArgument("PATH").StoreResult(&RunOpts.TracePath);
        msgBusOpts.AddHelpOption('h');
        RunOpts.ProxyBusQueueConfig.ConfigureLastGetopt(msgBusOpts, "");
        RunOpts.ProxyBusSessionConfig.ConfigureLastGetopt(msgBusOpts, "");
        TOptsParseResult mbusRes(&msgBusOpts, argc, argv);
        Y_UNUSED(mbusRes);
        RunOpts.StartTracingBusProxy = !RunOpts.TracePath.empty();
        RunOpts.StartBusProxy = true;
    }

    Y_ABORT_UNLESS(RunOpts.NodeId > 0 && RunOpts.NodeId <= NActors::TActorId::MaxNodeId);
}

void TRunCommandConfigParser::ApplyParsedOptions() {
    // apply global options
    Config.AppConfig.MutableInterconnectConfig()->SetStartTcp(GlobalOpts.StartTcp);
    auto logConfig = Config.AppConfig.MutableLogConfig();
    logConfig->SetSysLog(GlobalOpts.SysLog);
    logConfig->SetDefaultLevel(GlobalOpts.LogLevel);
    logConfig->SetDefaultSamplingLevel(GlobalOpts.LogSamplingLevel);
    logConfig->SetDefaultSamplingRate(GlobalOpts.LogSamplingRate);
    logConfig->SetFormat(GlobalOpts.LogFormat);
    logConfig->SetClusterName(GlobalOpts.ClusterName);

    // apply options affecting UDF paths
    Config.AppConfig.SetUDFsDir(GlobalOpts.UDFsDir);
    for (const auto& path : GlobalOpts.UDFsPaths) {
        Config.AppConfig.AddUDFsPaths(path);
    }

    // apply run options
    Config.NodeId = RunOpts.NodeId;
    auto messageBusConfig = Config.AppConfig.MutableMessageBusConfig();

    messageBusConfig->SetStartBusProxy(RunOpts.StartBusProxy);
    messageBusConfig->SetBusProxyPort(RunOpts.BusProxyPort);

    auto queueConfig = messageBusConfig->MutableProxyBusQueueConfig();
    queueConfig->SetName(RunOpts.ProxyBusQueueConfig.Name);
    queueConfig->SetNumWorkers(RunOpts.ProxyBusQueueConfig.NumWorkers);

    auto sessionConfig = messageBusConfig->MutableProxyBusSessionConfig();

    // TODO use macro from messagebus header file
    sessionConfig->SetName(RunOpts.ProxyBusSessionConfig.Name);
    sessionConfig->SetNumRetries(RunOpts.ProxyBusSessionConfig.NumRetries);
    sessionConfig->SetRetryInterval(RunOpts.ProxyBusSessionConfig.RetryInterval);
    sessionConfig->SetReconnectWhenIdle(RunOpts.ProxyBusSessionConfig.ReconnectWhenIdle);
    sessionConfig->SetMaxInFlight(RunOpts.ProxyBusSessionConfig.MaxInFlight);
    sessionConfig->SetPerConnectionMaxInFlight(RunOpts.ProxyBusSessionConfig.PerConnectionMaxInFlight);
    sessionConfig->SetPerConnectionMaxInFlightBySize(RunOpts.ProxyBusSessionConfig.PerConnectionMaxInFlightBySize);
    sessionConfig->SetMaxInFlightBySize(RunOpts.ProxyBusSessionConfig.MaxInFlightBySize);
    sessionConfig->SetTotalTimeout(RunOpts.ProxyBusSessionConfig.TotalTimeout);
    sessionConfig->SetSendTimeout(RunOpts.ProxyBusSessionConfig.SendTimeout);
    sessionConfig->SetConnectTimeout(RunOpts.ProxyBusSessionConfig.ConnectTimeout);
    sessionConfig->SetDefaultBufferSize(RunOpts.ProxyBusSessionConfig.DefaultBufferSize);
    sessionConfig->SetMaxBufferSize(RunOpts.ProxyBusSessionConfig.MaxBufferSize);
    sessionConfig->SetSocketRecvBufferSize(RunOpts.ProxyBusSessionConfig.SocketRecvBufferSize);
    sessionConfig->SetSocketSendBufferSize(RunOpts.ProxyBusSessionConfig.SocketSendBufferSize);
    sessionConfig->SetSocketToS(RunOpts.ProxyBusSessionConfig.SocketToS);
    sessionConfig->SetSendThreshold(RunOpts.ProxyBusSessionConfig.SendThreshold);
    sessionConfig->SetCork(RunOpts.ProxyBusSessionConfig.Cork.MilliSeconds());
    sessionConfig->SetMaxMessageSize(RunOpts.ProxyBusSessionConfig.MaxMessageSize);
    sessionConfig->SetTcpNoDelay(RunOpts.ProxyBusSessionConfig.TcpNoDelay);
    sessionConfig->SetTcpCork(RunOpts.ProxyBusSessionConfig.TcpCork);
    sessionConfig->SetExecuteOnMessageInWorkerPool(RunOpts.ProxyBusSessionConfig.ExecuteOnMessageInWorkerPool);
    sessionConfig->SetExecuteOnReplyInWorkerPool(RunOpts.ProxyBusSessionConfig.ExecuteOnReplyInWorkerPool);
    sessionConfig->SetListenPort(RunOpts.ProxyBusSessionConfig.ListenPort);

    for (auto proxy : RunOpts.ProxyBindToProxy) {
        messageBusConfig->AddProxyBindToProxy(proxy);
    }
    messageBusConfig->SetStartTracingBusProxy(RunOpts.StartTracingBusProxy);
    messageBusConfig->SetTracePath(RunOpts.TracePath);

    Config.AppConfig.MutableBootstrapConfig()->MutableCompileServiceConfig()->SetInflightLimit(RunOpts.CompileInflightLimit);
    Config.AppConfig.MutableMonitoringConfig()->SetMonitoringPort(RunOpts.MonitoringPort);
    Config.AppConfig.MutableMonitoringConfig()->SetMonitoringAddress(RunOpts.MonitoringAddress);
    Config.AppConfig.MutableMonitoringConfig()->SetMonitoringThreads(RunOpts.MonitoringThreads);
    Config.AppConfig.MutableMonitoringConfig()->SetMonitoringCertificate(TUnbufferedFileInput(RunOpts.MonitoringCertificateFile).ReadAll());
    Config.AppConfig.MutableRestartsCountConfig()->SetRestartsCountFile(RunOpts.RestartsCountFile);
}

} // namespace NKikimr
