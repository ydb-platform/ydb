#include "application.h"
#include "utils.h"

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>

#include <ydb/core/base/backtrace.h>

namespace NKikimrRun {

void TMainBase::RegisterKikimrOptions(NLastGetopt::TOpts& options, TServerSettings& settings) {
    options.AddLongOption("log-file", "File with execution logs (writes in stderr if empty)")
        .RequiredArgument("file")
        .StoreResult(&settings.LogOutputFile)
        .Handler1([](const NLastGetopt::TOptsParser* option) {
            if (const TString& file = option->CurVal()) {
                std::remove(file.c_str());
            }
        });

    TChoices<NActors::NLog::EPriority> logPriority({
        {"emerg", NActors::NLog::EPriority::PRI_EMERG},
        {"alert", NActors::NLog::EPriority::PRI_ALERT},
        {"crit", NActors::NLog::EPriority::PRI_CRIT},
        {"error", NActors::NLog::EPriority::PRI_ERROR},
        {"warn", NActors::NLog::EPriority::PRI_WARN},
        {"notice", NActors::NLog::EPriority::PRI_NOTICE},
        {"info", NActors::NLog::EPriority::PRI_INFO},
        {"debug", NActors::NLog::EPriority::PRI_DEBUG},
        {"trace", NActors::NLog::EPriority::PRI_TRACE},
    });
    options.AddLongOption("log-default", "Default log priority")
        .RequiredArgument("priority")
        .Choices(logPriority.GetChoices())
        .StoreMappedResultT<TString>(&DefaultLogPriority, logPriority);

    options.AddLongOption("log", "Component log priority in format <component>=<priority> (e. g. KQP_YQL=trace)")
        .RequiredArgument("component priority")
        .Handler1([this, logPriority](const NLastGetopt::TOptsParser* option) {
            TStringBuf component;
            TStringBuf priority;
            TStringBuf(option->CurVal()).Split('=', component, priority);
            if (component.empty() || priority.empty()) {
                ythrow yexception() << "Incorrect log setting, expected form component=priority, e. g. KQP_YQL=trace";
            }

            if (!logPriority.Contains(TString(priority))) {
                ythrow yexception() << "Incorrect log priority: " << priority;
            }

            const auto service = GetLogService(TString(component));
            if (!LogPriorities.emplace(service, logPriority(TString(priority))).second) {
                ythrow yexception() << "Got duplicated log service name: " << component;
            }
        });

    options.AddLongOption('M', "monitoring", "Embedded UI port (use 0 to start on random free port), if used will be run as daemon")
        .RequiredArgument("uint")
        .Handler1([&settings](const NLastGetopt::TOptsParser* option) {
            if (const TString& port = option->CurVal()) {
                settings.MonitoringEnabled = true;
                settings.MonitoringPortOffset = FromString(port);
            }
        });

    options.AddLongOption('G', "grpc", "gRPC port (use 0 to start on random free port), if used will be run as daemon")
        .RequiredArgument("uint")
        .Handler1([&settings](const NLastGetopt::TOptsParser* option) {
            if (const TString& port = option->CurVal()) {
                settings.GrpcEnabled = true;
                settings.GrpcPort = FromString(port);
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

void TMainBase::FillLogConfig(NKikimrConfig::TLogConfig& config) const {
    if (DefaultLogPriority) {
        config.SetDefaultLevel(*DefaultLogPriority);
    }
    ModifyLogPriorities(LogPriorities, config);
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

}  // namespace NKikimrRun
