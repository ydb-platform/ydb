#include "kqp_runner.h"
#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_reader.h>

#include <ydb/core/blob_depot/mon_main.h>
#include <ydb/core/fq/libs/compute/common/utils.h>

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/format.h>


namespace NKqpRun {

namespace {

// Function adds thousands separators
// 123456789 -> 123.456.789
TString FormatNumber(i64 number) {
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

void PrintStatistics(const TString& fullStat, const THashMap<TString, i64>& flatStat, const NFq::TPublicStat& publicStat, IOutputStream& output) {
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

}  // anonymous namespace


//// TKqpRunner::TImpl

class TKqpRunner::TImpl {
public:
    enum class EQueryType {
        ScriptQuery,
        YqlScriptQuery,
        AsyncQuery
    };

    explicit TImpl(const TRunnerOptions& options)
        : Options_(options)
        , YdbSetup_(options.YdbSettings)
        , StatProcessor_(NFq::CreateStatProcessor("stat_full"))
        , CerrColors_(NColorizer::AutoColors(Cerr))
        , CoutColors_(NColorizer::AutoColors(Cout))
    {}

    bool ExecuteSchemeQuery(const TRequestOptions& query) const {
        StartSchemeTraceOpt();

        TSchemeMeta meta;
        TRequestResult status = YdbSetup_.SchemeQueryRequest(query, meta);
        TYdbSetup::StopTraceOpt();

        PrintSchemeQueryAst(meta.Ast);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute scheme query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return true;
    }

    bool ExecuteScript(const TRequestOptions& script) {
        StartScriptTraceOpt();

        TRequestResult status = YdbSetup_.ScriptRequest(script, ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to start script execution, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return WaitScriptExecutionOperation();
    }

    bool ExecuteQuery(const TRequestOptions& query, EQueryType queryType) {
        StartScriptTraceOpt();
        StartTime_ = TInstant::Now();

        TString queryTypeStr;
        TQueryMeta meta;
        TRequestResult status;
        switch (queryType) {
        case EQueryType::ScriptQuery:
            queryTypeStr = "Generic";
            status = YdbSetup_.QueryRequest(query, meta, ResultSets_, GetProgressCallback());
            break;

        case EQueryType::YqlScriptQuery:
            queryTypeStr = "Yql script";
            status = YdbSetup_.YqlScriptRequest(query, meta, ResultSets_);
            break;

        case EQueryType::AsyncQuery:
            YdbSetup_.QueryRequestAsync(query);
            return true;
        }

        TYdbSetup::StopTraceOpt();

        PrintScriptAst(meta.Ast);
        PrintScriptProgress(ExecutionMeta_.Plan);
        PrintScriptPlan(meta.Plan);
        PrintScriptFinish(meta, queryTypeStr);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (!status.Issues.Empty()) {
            Cerr << CerrColors_.Red() << "Request finished with issues:" << CerrColors_.Default() << Endl << status.Issues.ToString() << Endl;
        }

        return true;
    }

    void WaitAsyncQueries() const {
        YdbSetup_.WaitAsyncQueries();
    }

    bool FetchScriptResults() {
        TYdbSetup::StopTraceOpt();

        ResultSets_.clear();
        ResultSets_.resize(ExecutionMeta_.ResultSetsCount);
        for (i32 resultSetId = 0; resultSetId < ExecutionMeta_.ResultSetsCount; ++resultSetId) {
            TRequestResult status = YdbSetup_.FetchScriptExecutionResultsRequest(ExecutionOperation_, resultSetId, ResultSets_[resultSetId]);

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to fetch result set with id " << resultSetId << ", reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }
        }

        return true;
    }

    bool ForgetExecutionOperation() {
        TYdbSetup::StopTraceOpt();

        TRequestResult status = YdbSetup_.ForgetScriptExecutionOperationRequest(ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to forget script execution operation, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (!status.Issues.Empty()) {
            Cerr << CerrColors_.Red() << "Forget operation finished with issues:" << CerrColors_.Default() << Endl << status.Issues.ToString() << Endl;
        }

        return true;
    }

    void PrintScriptResults() const {
        if (Options_.ResultOutput) {
            Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Writing script query results..." << CoutColors_.Default() << Endl;
            for (size_t i = 0; i < ResultSets_.size(); ++i) {
                if (ResultSets_.size() > 1) {
                    *Options_.ResultOutput << CoutColors_.Cyan() << "Result set " << i + 1 << ":" << CoutColors_.Default() << Endl;
                }
                PrintScriptResult(ResultSets_[i]);
            }
        }
    }

private:
    bool WaitScriptExecutionOperation() {
        StartTime_ = TInstant::Now();
        ExecutionMeta_ = TExecutionMeta();

        TDuration getOperationPeriod = TDuration::Seconds(1);
        if (auto progressStatsPeriodMs = Options_.YdbSettings.AppConfig.GetQueryServiceConfig().GetProgressStatsPeriodMs()) {
            getOperationPeriod = TDuration::MilliSeconds(progressStatsPeriodMs);
        }

        TRequestResult status;
        while (true) {
            status = YdbSetup_.GetScriptExecutionOperationRequest(ExecutionOperation_, ExecutionMeta_);
            PrintScriptProgress(ExecutionMeta_.Plan);

            if (ExecutionMeta_.Ready) {
                break;
            }

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to get script execution operation, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            Sleep(getOperationPeriod);
        }

        PrintScriptAst(ExecutionMeta_.Ast);
        PrintScriptPlan(ExecutionMeta_.Plan);
        PrintScriptFinish(ExecutionMeta_, "Script");

        if (!status.IsSuccess() || ExecutionMeta_.ExecutionStatus != NYdb::NQuery::EExecStatus::Completed) {
            Cerr << CerrColors_.Red() << "Failed to execute script, invalid final status, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (!status.Issues.Empty()) {
            Cerr << CerrColors_.Red() << "Request finished with issues:" << CerrColors_.Default() << Endl << status.Issues.ToString() << Endl;
        }

        return true;
    }

    void StartSchemeTraceOpt() const {
        if (Options_.TraceOptType == TRunnerOptions::ETraceOptType::All || Options_.TraceOptType == TRunnerOptions::ETraceOptType::Scheme) {
            YdbSetup_.StartTraceOpt();
        }
    }

    void StartScriptTraceOpt() const {
        if (Options_.TraceOptType == TRunnerOptions::ETraceOptType::All || Options_.TraceOptType == TRunnerOptions::ETraceOptType::Script) {
            YdbSetup_.StartTraceOpt();
        }
    }

    void PrintSchemeQueryAst(const TString& ast) const {
        if (Options_.SchemeQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing scheme query ast" << CoutColors_.Default() << Endl;
            Options_.SchemeQueryAstOutput->Write(ast);
        }
    }

    void PrintScriptAst(const TString& ast) const {
        if (Options_.ScriptQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query ast" << CoutColors_.Default() << Endl;
            Options_.ScriptQueryAstOutput->Write(ast);
        }
    }

    void PrintPlan(const TString& plan, IOutputStream* output) const {
        if (!plan) {
            return;
        }

        NJson::TJsonValue planJson;
        NJson::ReadJsonTree(plan, &planJson, true);
        if (!planJson.GetMapSafe().contains("meta")) {
            return;
        }

        NYdb::NConsoleClient::TQueryPlanPrinter printer(Options_.PlanOutputFormat, true, *output);
        printer.Print(plan);
    }

    void PrintScriptPlan(const TString& plan) const {
        if (Options_.ScriptQueryPlanOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query plan" << CoutColors_.Default() << Endl;
            PrintPlan(plan, Options_.ScriptQueryPlanOutput);
        }
    }

    void PrintScriptProgress(const TString& plan) const {
        if (Options_.InProgressStatisticsOutputFile) {
            TFileOutput outputStream(Options_.InProgressStatisticsOutputFile);
            outputStream << TInstant::Now().ToIsoStringLocal() << " Script in progress statistics" << Endl;

            auto convertedPlan = plan;
            try {
                convertedPlan = StatProcessor_->ConvertPlan(plan);
            } catch (const NJson::TJsonException& ex) {
                outputStream << "Error plan conversion: " << ex.what() << Endl;
            }

            try {
                double cpuUsage = 0.0;
                auto fullStat = StatProcessor_->GetQueryStat(convertedPlan, cpuUsage, nullptr);
                auto flatStat = StatProcessor_->GetFlatStat(convertedPlan);
                auto publicStat = StatProcessor_->GetPublicStat(fullStat);

                outputStream << "\nCPU usage: " << cpuUsage << Endl;
                PrintStatistics(fullStat, flatStat, publicStat, outputStream);
            } catch (const NJson::TJsonException& ex) {
                outputStream << "Error stat conversion: " << ex.what() << Endl;
            }

            outputStream << "\nPlan visualization:" << Endl;
            PrintPlan(convertedPlan, &outputStream);

            outputStream.Finish();
        }
    }

    TProgressCallback GetProgressCallback() {
        return [this](const NKikimrKqp::TEvExecuterProgress& executerProgress) mutable {
            const TString& plan = executerProgress.GetQueryPlan();
            ExecutionMeta_.Plan = plan;
            PrintScriptProgress(plan);
        };
    }

    void PrintScriptResult(const Ydb::ResultSet& resultSet) const {
        switch (Options_.ResultOutputFormat) {
        case TRunnerOptions::EResultOutputFormat::RowsJson: {
            NYdb::TResultSet result(resultSet);
            NYdb::TResultSetParser parser(result);
            while (parser.TryNextRow()) {
                NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, Options_.ResultOutput);
                writer.SetWriteNanAsString(true);
                NYdb::FormatResultRowJson(parser, result.GetColumnsMeta(), writer, NYdb::EBinaryStringEncoding::Unicode);
                *Options_.ResultOutput << Endl;
            }
            break;
        }

        case TRunnerOptions::EResultOutputFormat::FullJson:
            resultSet.PrintJSON(*Options_.ResultOutput);
            *Options_.ResultOutput << Endl;
            break;

        case TRunnerOptions::EResultOutputFormat::FullProto:
            TString resultSetString;
            google::protobuf::TextFormat::Printer printer;
            printer.SetSingleLineMode(false);
            printer.SetUseUtf8StringEscaping(true);
            printer.PrintToString(resultSet, &resultSetString);
            *Options_.ResultOutput << resultSetString;
            break;
        }
    }

    void PrintScriptFinish(const TQueryMeta& meta, const TString& queryType) const {
        Cout << CoutColors_.Cyan() << queryType << " request finished.";
        if (meta.TotalDuration) {
            Cout << " Total duration: " << meta.TotalDuration;
        } else {
            Cout << " Estimated duration: " << TInstant::Now() - StartTime_;
        }
        Cout << CoutColors_.Default() << Endl;
    }

private:
    TRunnerOptions Options_;

    TYdbSetup YdbSetup_;
    std::unique_ptr<NFq::IPlanStatProcessor> StatProcessor_;
    NColorizer::TColors CerrColors_;
    NColorizer::TColors CoutColors_;

    TString ExecutionOperation_;
    TExecutionMeta ExecutionMeta_;
    std::vector<Ydb::ResultSet> ResultSets_;
    TInstant StartTime_;
};


//// TKqpRunner

TKqpRunner::TKqpRunner(const TRunnerOptions& options)
    : Impl_(new TImpl(options))
{}

bool TKqpRunner::ExecuteSchemeQuery(const TRequestOptions& query) const {
    return Impl_->ExecuteSchemeQuery(query);
}

bool TKqpRunner::ExecuteScript(const TRequestOptions& script) const {
    return Impl_->ExecuteScript(script);
}

bool TKqpRunner::ExecuteQuery(const TRequestOptions& query) const {
    return Impl_->ExecuteQuery(query, TImpl::EQueryType::ScriptQuery);
}

bool TKqpRunner::ExecuteYqlScript(const TRequestOptions& query) const {
    return Impl_->ExecuteQuery(query, TImpl::EQueryType::YqlScriptQuery);
}

void TKqpRunner::ExecuteQueryAsync(const TRequestOptions& query) const {
    Impl_->ExecuteQuery(query, TImpl::EQueryType::AsyncQuery);
}

void TKqpRunner::WaitAsyncQueries() const {
    Impl_->WaitAsyncQueries();
}

bool TKqpRunner::FetchScriptResults() {
    return Impl_->FetchScriptResults();
}

bool TKqpRunner::ForgetExecutionOperation() {
    return Impl_->ForgetExecutionOperation();
}

void TKqpRunner::PrintScriptResults() const {
    Impl_->PrintScriptResults();
}

}  // namespace NKqpRun
