#include "application.h"
#include "utils.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>

#include <ydb/core/base/backtrace.h>
#include <ydb/core/blob_depot/mon_main.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/public/udf/udf_static_registry.h>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#endif

namespace NKikimrRun {

namespace {

#ifdef PROFILE_MEMORY_ALLOCATIONS
void InterruptHandler(int) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cout << colors.Red() << "Execution interrupted, finishing profile memory allocations..." << colors.Default() << Endl;
    TMainBase::FinishProfileMemoryAllocations();

    abort();
}
#endif

}  // anonymous namespace

TMainBase::TMainBase() {
#ifdef PROFILE_MEMORY_ALLOCATIONS
    signal(SIGINT, &InterruptHandler);
#endif
}

#ifdef PROFILE_MEMORY_ALLOCATIONS
void TMainBase::FinishProfileMemoryAllocations() {
    if (ProfileAllocationsOutput) {
        NAllocProfiler::StopAllocationSampling(*ProfileAllocationsOutput);
    } else {
        TString output;
        TStringOutput stream(output);
        NAllocProfiler::StopAllocationSampling(stream);

        Cout << CoutColors.Red() << "Warning: profile memory allocations output is not specified, please use flag `--profile-output` for writing profile info (dump size " << NKikimr::NBlobDepot::FormatByteSize(output.size()) << ")" << CoutColors.Default() << Endl;
    }
}
#endif

void TMainBase::RegisterKikimrOptions(NLastGetopt::TOpts& options, TServerSettings& settings) {
    options.AddLongOption("threads", "User pool size for each node (also scaled system, batch and IC pools in proportion: system / user / batch / IC = 1 / 10 / 1 / 1)")
        .RequiredArgument("uint")
        .StoreMappedResultT<ui32>(&UserPoolSize, [](ui32 threadsCount) {
            if (threadsCount < 1) {
                ythrow yexception() << "Number of threads less than one";
            }
            return threadsCount;
        });

    options.AddLongOption('u', "udf", "Load shared library with UDF by given path")
        .RequiredArgument("file")
        .EmplaceTo(&UdfsPaths);

    options.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory")
        .RequiredArgument("directory")
        .StoreResult(&UdfsDirectory);

    options.AddLongOption("exclude-linked-udfs", "Exclude linked udfs when same udf passed from -u or --udfs-dir")
        .NoArgument()
        .SetFlag(&ExcludeLinkedUdfs);

    options.AddLongOption("log-file", "File with execution logs (writes in stderr if empty)")
        .RequiredArgument("file")
        .StoreResult(&settings.LogOutputFile)
        .Handler1([](const NLastGetopt::TOptsParser* option) {
            if (const TString& file = option->CurVal()) {
                std::remove(file.c_str());
            }
        });

    RegisterLogOptions(options);

    options.AddLongOption("profile-output", "File with profile memory allocations output (use '-' to write in stdout)")
        .RequiredArgument("file")
        .StoreMappedResultT<TString>(&ProfileAllocationsOutput, &GetDefaultOutput);

    options.AddLongOption('M', "monitoring", "Embedded UI ports range start (start on random free port by default), if used will be run as daemon")
        .OptionalArgument("uint")
        .Handler1([&settings](const NLastGetopt::TOptsParser* option) {
            settings.MonitoringEnabled = true;
            if (const char* portStr = option->CurVal()) {
                settings.FirstMonitoringPort = FromString(portStr);
            }
        });

    options.AddLongOption('G', "grpc", "gRPC ports range start (start on random free port by default), if used will be run as daemon")
        .OptionalArgument("uint")
        .Handler1([&settings](const NLastGetopt::TOptsParser* option) {
            settings.GrpcEnabled = true;
            if (const char* portStr = option->CurVal()) {
                settings.FirstGrpcPort = FromString(portStr);
            }
        });

    options.AddLongOption("domain", "Test cluster domain name")
        .RequiredArgument("name")
        .DefaultValue(settings.DomainName)
        .StoreResult(&settings.DomainName);

    TChoices<std::function<void()>> backtrace({
        {"heavy", &NKikimr::EnableYDBBacktraceFormat},
        {"light", []() { SetFormatBackTraceFn(FormatBackTrace); }}
    });
    options.AddLongOption("backtrace", "Default backtrace format function")
        .RequiredArgument("backtrace-type")
        .DefaultValue("heavy")
        .Choices(backtrace.GetChoices())
        .Handler1([backtrace](const NLastGetopt::TOptsParser* option) {
            TString choice(option->CurValOrDef());
            backtrace(choice)();
        });
}

void TMainBase::RegisterLogOptions(NLastGetopt::TOpts& options) {
    const auto allowedPriorities = ", allowed log priorities: trace, debug, info, notice, warn, error, crit, alert, emerg";
    options.AddLongOption("log-default", TStringBuilder() << "Default log priority" << allowedPriorities)
        .RequiredArgument("priority")
        .StoreMappedResultT<TString>(&DefaultLogPriority, GetLogPrioritiesMap("log-default"));

    options.AddLongOption("log", TStringBuilder() << "Component log priority in format <component>=<priority> (e. g. KQP_YQL=trace)" << allowedPriorities)
        .RequiredArgument("component=priority")
        .Handler1([this, logPriority = GetLogPrioritiesMap("log")](const NLastGetopt::TOptsParser* option) {
            TStringBuf component;
            TStringBuf priority;
            TStringBuf(option->CurVal()).Split('=', component, priority);
            if (component.empty() || priority.empty()) {
                ythrow yexception() << "Incorrect log setting, expected form component=priority, e. g. KQP_YQL=trace";
            }

            const auto service = GetLogService(TString(component));
            if (!LogPriorities.emplace(service, logPriority(TString(priority))).second) {
                ythrow yexception() << "Got duplicated log service name: " << component;
            }
        });

    const auto addLogOption = [&](const TString& name, const TString& help, std::optional<NActors::NLog::EPriority>* target) {
        options.AddLongOption(name, TStringBuilder() << help << allowedPriorities)
            .RequiredArgument("priority")
            .StoreMappedResultT<TString>(target, GetLogPrioritiesMap(name));
    };

    addLogOption("log-fq", "FQ components log priority", &FqLogPriority);
    addLogOption("log-kqp", "KQP components log priority", &KqpLogPriority);
    addLogOption("log-runtime", "DQ and MKQL components log priority", &RuntimeLogPriority);
    addLogOption("log-tablets", "Log priority for all tablet services (HIVE / SS / DS / CS and etc.)", &TabletsLogPriority);
    addLogOption("log-blob-storage", "Blob storage components log priority", &BsLogPriority);
    addLogOption("log-server-io", "Server IO components log priority (http / grpc / viewer and etc.)", &ServerIoLogPriority);
}

IOutputStream* TMainBase::GetDefaultOutput(const TString& file) {
    if (file == "-") {
        return &Cout;
    }
    if (file) {
        FileHolders.emplace_back(new TFileOutput(file));
        return FileHolders.back().get();
    }
    return nullptr;
}

TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> TMainBase::CreateFunctionRegistry() const {
    if (!UdfsDirectory.empty() || !UdfsPaths.empty()) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Fetching udfs..." << colors.Default() << Endl;
    }

    auto paths = UdfsPaths;
    NKikimr::NMiniKQL::FindUdfsInDir(UdfsDirectory, &paths);
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, paths)->Clone();

    if (ExcludeLinkedUdfs) {
        for (const auto& wrapper : NYql::NUdf::GetStaticUdfModuleWrapperList()) {
            auto [name, ptr] = wrapper();
            if (!functionRegistry->IsLoadedUdfModule(name)) {
                functionRegistry->AddModule(TString(NKikimr::NMiniKQL::StaticModulePrefix) + name, name, std::move(ptr));
            }
        }
    } else {
        NKikimr::NMiniKQL::FillStaticModules(*functionRegistry);
    }

    return functionRegistry;
}

void TMainBase::ReplaceYqlTokenTemplate(TString& text) const {
    const TString tokenVariableName = TStringBuilder() << "${" << YQL_TOKEN_VARIABLE << "}";
    if (YqlToken) {
        SubstGlobal(text, tokenVariableName, YqlToken);
    } else if (text.Contains(tokenVariableName)) {
        ythrow yexception() << "Failed to replace ${" << YQL_TOKEN_VARIABLE << "} template, please specify " << YQL_TOKEN_VARIABLE << " environment variable";
    }
}

void TMainBase::SetupActorSystemConfig(NKikimrConfig::TAppConfig& config) const {
    if (!UserPoolSize) {
        return;
    }

    auto& asConfig = *config.MutableActorSystemConfig();

    if (!asConfig.HasScheduler()) {
        auto& scheduler = *asConfig.MutableScheduler();
        scheduler.SetResolution(64);
        scheduler.SetSpinThreshold(0);
        scheduler.SetProgressThreshold(10000);
    }

    asConfig.ClearExecutor();

    const auto addExecutor = [&asConfig](const TString& name, ui64 threads, NKikimrConfig::TActorSystemConfig::TExecutor::EType type, std::optional<ui64> spinThreshold = std::nullopt) {
        auto& executor = *asConfig.AddExecutor();
        executor.SetName(name);
        executor.SetThreads(threads);
        executor.SetType(type);
        if (spinThreshold) {
            executor.SetSpinThreshold(*spinThreshold);
        }
        return executor;
    };

    const auto divideThreads = [](ui64 threads, ui64 divisor) {
        if (threads < 1) {
            ythrow yexception() << "Threads must be greater than 0";
        }
        return (threads - 1) / divisor + 1;
    };

    addExecutor("System", divideThreads(*UserPoolSize, 10), NKikimrConfig::TActorSystemConfig::TExecutor::BASIC, 10);
    asConfig.SetSysExecutor(0);

    addExecutor("User", *UserPoolSize, NKikimrConfig::TActorSystemConfig::TExecutor::BASIC, 1);
    asConfig.SetUserExecutor(1);

    addExecutor("Batch", divideThreads(*UserPoolSize, 10), NKikimrConfig::TActorSystemConfig::TExecutor::BASIC, 1);
    asConfig.SetBatchExecutor(2);

    addExecutor("IO", 1, NKikimrConfig::TActorSystemConfig::TExecutor::IO);
    asConfig.SetIoExecutor(3);

    addExecutor("IC", divideThreads(*UserPoolSize, 10), NKikimrConfig::TActorSystemConfig::TExecutor::BASIC, 10)
        .SetTimePerMailboxMicroSecs(100);
    auto& serviceExecutors = *asConfig.MutableServiceExecutor();
    serviceExecutors.Clear();
    auto& serviceExecutor = *serviceExecutors.Add();
    serviceExecutor.SetServiceName("Interconnect");
    serviceExecutor.SetExecutorId(4);
}

void TMainBase::SetupLogsConfig(NKikimrConfig::TLogConfig& config) const {
    if (DefaultLogPriority) {
        config.SetDefaultLevel(*DefaultLogPriority);
    }

    const auto setupLogPriority = [&config](const std::unordered_set<TString>& prefixes, std::optional<NActors::NLog::EPriority> priority) {
        if (!priority) {
            return;
        }

        std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> logPriorities;

        const auto descriptor = NKikimrServices::EServiceKikimr_descriptor();
        for (int i = 0; i < descriptor->value_count(); ++i) {
            const auto service = static_cast<NKikimrServices::EServiceKikimr>(descriptor->value(i)->number());
            const auto& serviceStr = NKikimrServices::EServiceKikimr_Name(service);

            for (const auto& prefix : prefixes) {
                if (serviceStr.StartsWith(prefix)) {
                    logPriorities.emplace(service, *priority);
                    break;
                }
            }
        }

        ModifyLogPriorities(logPriorities, config);
    };

    setupLogPriority({
        "PUBLIC_HTTP", "DB_POOL", "STREAMS", "YQL_", "YQ_", "FQ_"
    }, FqLogPriority);

    setupLogPriority({
        "KQP_", "METADATA_"
    }, KqpLogPriority);

    setupLogPriority({
        "MINIKQL_ENGINE", "KQP_TASKS_RUNNER", "KQP_COMPUTE", "DQ_TASK_RUNNER", "OBJECT_STORAGE_INFERENCINATOR", "SSA_GRAPH_EXECUTION"
    }, RuntimeLogPriority);

    setupLogPriority({
        "TABLET_", "TX_", "OPS_", "PIPE_", "KEYVALUE", "PERSQUEUE", "PQ_", "BOARD_", "QUOTER_", "DATASHARD_", "CMS", "KESUS_",
        "CONFIGS_", "SCHEME_", "SEQUENCE", "COLUMNSHARD_", "REPLICATION_", "TRANSFER", "NAMESERVICE", "DATA_INTEGRITY", "MEMORY_",
        "GROUPED_MEMORY_LIMITER", "STATISTICS", "OBJECTS_MONITORING", "ARROW_HELPER", "EXT_INDEX", "DISCOVERY", "BLOB_CACHE",
        "DISCOVERY_CACHE", "BG_TASKS", "GENERAL_CACHE", "LOCAL_YDB_PROXY", "CHANGE_EXCHANGE", "SYSTEM_VIEWS", "BUILD_INDEX",
        "EXPORT", "IMPORT", "S3_WRAPPER", "CONTINUOUS_BACKUP", "NET_CLASSIFIER", "KESYS_PROXY_REQUEST", "NODE_BROKER", "FLAT_TX_SCHEMESHARD",
        "TENANT_SLOT_BROKER", "SQS", "CHOOSE_PROXY", "LB_CONFIG_MANAGER", "LONG_TX_SERVICE", "SAUSAGE_BIO", "RESOURCE_BROKER",
        "STATESTORAGE", "HIVE", "LOCAL", "BOOTSTRAPPER", "TENANT_POOL", "LABELS_MAINTAINER", "GRAPH", "SCHEMESHARD_DESCRIBE",
    }, TabletsLogPriority);

    setupLogPriority({
        "BRIDGE", "DS_PROXY_NODE_MON", "BLOBSTORAGE", "BSCONFIG", "BS_", "BLOB_"
    }, BsLogPriority);

    setupLogPriority({
        "TOKEN_", "KAFKA_PROXY", "YDB_SDK", "TICKET_PARSER", "BLACKBOX_VALIDATOR", "LDAP_AUTH_PROVIDER", "RPC_REQUEST", "HEALTH",
        "DB_METADATA_CACHE", "HTTP_PROXY", "PGWIRE", "LOCAL_PGWIRE", "PGYDB", "AUDIT_LOG_WRITER", "METERING_WRITER", "VIEWER",
        "MSGBUS_", "GRPC_"
    }, ServerIoLogPriority);

    ModifyLogPriorities(LogPriorities, config);
}

}  // namespace NKikimrRun
