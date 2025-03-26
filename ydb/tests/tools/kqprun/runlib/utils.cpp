#include "utils.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_reader.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/core/blob_depot/mon_main.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimrRun {

namespace {

void TerminateHandler() {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= terminate() call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    Cerr << colors.Red() << "=======================================" << colors.Default() << Endl;

    abort();
}


void SegmentationFaultHandler(int) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= segmentation fault call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    Cerr << colors.Red() << "==============================================" << colors.Default() << Endl;

    abort();
}

void FloatingPointExceptionHandler(int) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= floating point exception call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    Cerr << colors.Red() << "====================================================" << colors.Default() << Endl;

    abort();
}

}  // nonymous namespace


TRequestResult::TRequestResult()
    : Status(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
{}

TRequestResult::TRequestResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
    : Status(status)
    , Issues(issues)
{}

TRequestResult::TRequestResult(Ydb::StatusIds::StatusCode status, const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues)
    : Status(status)
{
    NYql::IssuesFromMessage(issues, Issues);
}

bool TRequestResult::IsSuccess() const {
    return Status == Ydb::StatusIds::SUCCESS;
}

TString TRequestResult::ToString() const {
    return TStringBuilder() << "Request finished with status: " << Status << "\nIssues:\n" << Issues.ToString() << "\n";
}

TStatsPrinter::TStatsPrinter(NYdb::NConsoleClient::EDataFormat planFormat)
    : PlanFormat(planFormat)
    , StatProcessor(NFq::CreateStatProcessor("stat_full"))
{}

void TStatsPrinter::PrintPlan(const TString& plan, IOutputStream& output) const {
    if (!plan) {
        return;
    }

    NJson::TJsonValue planJson;
    NJson::ReadJsonTree(plan, &planJson, true);
    if (!planJson.GetMapSafe().contains("meta")) {
        return;
    }

    NYdb::NConsoleClient::TQueryPlanPrinter printer(PlanFormat, true, output);
    printer.Print(plan);
}

void TStatsPrinter::PrintInProgressStatistics(const TString& plan, IOutputStream& output) const {
    output << TInstant::Now().ToIsoStringLocal() << " Script in progress statistics" << Endl;

    auto convertedPlan = plan;
    try {
        convertedPlan = StatProcessor->ConvertPlan(plan);
    } catch (const NJson::TJsonException& ex) {
        output << "Error plan conversion: " << ex.what() << Endl;
        return;
    }

    try {
        double cpuUsage = 0.0;
        auto fullStat = StatProcessor->GetQueryStat(convertedPlan, cpuUsage, nullptr);
        auto flatStat = StatProcessor->GetFlatStat(convertedPlan);
        auto publicStat = StatProcessor->GetPublicStat(fullStat);

        output << "\nCPU usage: " << cpuUsage << Endl;
        PrintStatistics(fullStat, flatStat, publicStat, output);
    } catch (const NJson::TJsonException& ex) {
        output << "Error stat conversion: " << ex.what() << Endl;
        return;
    }

    output << "\nPlan visualization:" << Endl;
    PrintPlan(convertedPlan, output);
}

void TStatsPrinter::PrintTimeline(const TString& plan, IOutputStream& output) {
    TPlanVisualizer planVisualizer;
    planVisualizer.LoadPlans(plan);
    output.Write(planVisualizer.PrintSvg());
}

void TStatsPrinter::PrintStatistics(const TString& fullStat, const THashMap<TString, i64>& flatStat, const NFq::TPublicStat& publicStat, IOutputStream& output) {
    output << "\nFlat statistics:" << Endl;
    for (const auto& [propery, value] : flatStat) {
        TString valueString = ToString(value);
        if (propery.Contains("Bytes")) {
            valueString = NKikimr::NBlobDepot::FormatByteSize(value);
        } else if (propery.Contains("TimeUs")) {
            valueString = NFq::FormatDurationUs(value);
        } else if (propery.Contains("TimeMs")) {
            valueString = NFq::FormatDurationMs(value);
        } else {
            valueString = FormatNumber(value);
        }
        output << propery << " = " << valueString << Endl;
    }

    output << "\nPublic statistics:" << Endl;
    if (auto memoryUsageBytes = publicStat.MemoryUsageBytes) {
        output << "MemoryUsage = " << NKikimr::NBlobDepot::FormatByteSize(*memoryUsageBytes) << Endl;
    }
    if (auto cpuUsageUs = publicStat.CpuUsageUs) {
        output << "CpuUsage = " << NFq::FormatDurationUs(*cpuUsageUs) << Endl;
    }
    if (auto inputBytes = publicStat.InputBytes) {
        output << "InputSize = " << NKikimr::NBlobDepot::FormatByteSize(*inputBytes) << Endl;
    }
    if (auto outputBytes = publicStat.OutputBytes) {
        output << "OutputSize = " << NKikimr::NBlobDepot::FormatByteSize(*outputBytes) << Endl;
    }
    if (auto sourceInputRecords = publicStat.SourceInputRecords) {
        output << "SourceInputRecords = " << FormatNumber(*sourceInputRecords) << Endl;
    }
    if (auto sinkOutputRecords = publicStat.SinkOutputRecords) {
        output << "SinkOutputRecords = " << FormatNumber(*sinkOutputRecords) << Endl;
    }
    if (auto runningTasks = publicStat.RunningTasks) {
        output << "RunningTasks = " << FormatNumber(*runningTasks) << Endl;
    }

    output << "\nFull statistics:" << Endl;
    NJson::TJsonValue statsJson;
    NJson::ReadJsonTree(fullStat, &statsJson);
    NJson::WriteJson(&output, &statsJson, true, true, true);
    output << Endl;
}

TString TStatsPrinter::FormatNumber(i64 number) {
    struct TSeparator : public std::numpunct<char> {
        char do_thousands_sep() const final {
            return '.';
        }

        std::string do_grouping() const final {
            return "\03";
        }
    };

    std::ostringstream stream;
    stream.imbue(std::locale(stream.getloc(), new TSeparator()));
    stream << number;
    return stream.str();
}

TString LoadFile(const TString& file) {
    return TFileInput(file).ReadAll();
}

NKikimrServices::EServiceKikimr GetLogService(const TString& serviceName) {
    NKikimrServices::EServiceKikimr service;
    if (!NKikimrServices::EServiceKikimr_Parse(serviceName, &service)) {
        ythrow yexception() << "Invalid kikimr service name " << serviceName;
    }
    return service;
}

void ModifyLogPriorities(std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> logPriorities, NKikimrConfig::TLogConfig& logConfig) {
    for (auto& entry : *logConfig.MutableEntry()) {
        const auto it = logPriorities.find(GetLogService(entry.GetComponent()));
        if (it != logPriorities.end()) {
            entry.SetLevel(it->second);
            logPriorities.erase(it);
        }
    }
    for (const auto& [service, priority] : logPriorities) {
        auto* entry = logConfig.AddEntry();
        entry->SetComponent(NKikimrServices::EServiceKikimr_Name(service));
        entry->SetLevel(priority);
    }
}

void InitLogSettings(const NKikimrConfig::TLogConfig& logConfig, NActors::TTestActorRuntimeBase& runtime) {
    if (logConfig.HasDefaultLevel()) {
        auto priority = NActors::NLog::EPriority(logConfig.GetDefaultLevel());
        auto descriptor = NKikimrServices::EServiceKikimr_descriptor();
        for (int i = 0; i < descriptor->value_count(); ++i) {
            runtime.SetLogPriority(static_cast<NKikimrServices::EServiceKikimr>(descriptor->value(i)->number()), priority);
        }
    }

    for (const auto& setting : logConfig.get_arr_entry()) {
        runtime.SetLogPriority(GetLogService(setting.GetComponent()), NActors::NLog::EPriority(setting.GetLevel()));
    }
}

TChoices<NActors::NLog::EPriority> GetLogPrioritiesMap(const TString& optionName) {
    return TChoices<NActors::NLog::EPriority>({
        {"emerg", NActors::NLog::EPriority::PRI_EMERG},
        {"alert", NActors::NLog::EPriority::PRI_ALERT},
        {"crit", NActors::NLog::EPriority::PRI_CRIT},
        {"error", NActors::NLog::EPriority::PRI_ERROR},
        {"warn", NActors::NLog::EPriority::PRI_WARN},
        {"notice", NActors::NLog::EPriority::PRI_NOTICE},
        {"info", NActors::NLog::EPriority::PRI_INFO},
        {"debug", NActors::NLog::EPriority::PRI_DEBUG},
        {"trace", NActors::NLog::EPriority::PRI_TRACE},
    }, optionName, false);
}

void SetupSignalActions() {
    std::set_terminate(&TerminateHandler);
    signal(SIGSEGV, &SegmentationFaultHandler);
    signal(SIGFPE, &FloatingPointExceptionHandler);
}

void PrintResultSet(EResultOutputFormat format, IOutputStream& output, const Ydb::ResultSet& resultSet) {
    switch (format) {
        case EResultOutputFormat::RowsJson: {
            NYdb::TResultSet result(resultSet);
            NYdb::TResultSetParser parser(result);
            while (parser.TryNextRow()) {
                NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &output);
                writer.SetWriteNanAsString(true);
                NYdb::FormatResultRowJson(parser, result.GetColumnsMeta(), writer, NYdb::EBinaryStringEncoding::Unicode);
                output << Endl;
            }
            break;
        }

        case EResultOutputFormat::FullJson: {
            resultSet.PrintJSON(output);
            output << Endl;
            break;
        }

        case EResultOutputFormat::FullProto: {
            TString resultSetString;
            google::protobuf::TextFormat::Printer printer;
            printer.SetSingleLineMode(false);
            printer.SetUseUtf8StringEscaping(true);
            printer.PrintToString(resultSet, &resultSetString);
            output << resultSetString;
            break;
        }
    }
}

}  // namespace NKikimrRun
