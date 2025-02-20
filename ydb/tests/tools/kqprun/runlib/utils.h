#pragma once

#include "settings.h"

#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimrRun {

struct TRequestResult {
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TRequestResult();

    TRequestResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);

    TRequestResult(Ydb::StatusIds::StatusCode status, const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues);

    bool IsSuccess() const;

    TString ToString() const;
};

template <typename TResult>
class TChoices {
public:
    explicit TChoices(std::map<TString, TResult> choicesMap, const TString& optionName = "", bool checkRegister = true)
        : ChoicesMap(std::move(choicesMap))
        , OptionName(optionName)
        , CheckRegister(checkRegister)
    {}

    TResult operator()(TString choice) const {
        if (!CheckRegister) {
            std::for_each(choice.begin(), choice.vend(), [](char& c) { c = std::tolower(c); });
        }

        const auto it = ChoicesMap.find(choice);
        if (it == ChoicesMap.end()) {
            auto error = yexception() << "Value '" << choice << "' is not allowed " << (OptionName ? TStringBuilder() << "for option " << OptionName : TStringBuilder()) << ", available variants:\n";
            for (auto it = ChoicesMap.begin(); it != ChoicesMap.end();) {
                error << choice;
                if (++it != ChoicesMap.end()) {
                    error << ", ";
                }
            }
            ythrow error;
        }
        return it->second;
    }

    TVector<TString> GetChoices() const {
        TVector<TString> choices;
        choices.reserve(ChoicesMap.size());
        for (const auto& [choice, _] : ChoicesMap) {
            choices.emplace_back(choice);
        }
        return choices;
    }

    bool Contains(const TString& choice) const {
        return ChoicesMap.contains(choice);
    }

private:
    const std::map<TString, TResult> ChoicesMap;
    const TString OptionName;
    const bool CheckRegister;
};

class TStatsPrinter {
public:
    explicit TStatsPrinter(NYdb::NConsoleClient::EDataFormat planFormat);

    void PrintPlan(const TString& plan, IOutputStream& output) const;

    void PrintInProgressStatistics(const TString& plan, IOutputStream& output) const;

    static void PrintTimeline(const TString& plan, IOutputStream& output);

    static void PrintStatistics(const TString& fullStat, const THashMap<TString, i64>& flatStat, const NFq::TPublicStat& publicStat, IOutputStream& output);

    // Function adds thousands separators
    // 123456789 -> 123.456.789
    static TString FormatNumber(i64 number);

private:
    const NYdb::NConsoleClient::EDataFormat PlanFormat;
    const std::unique_ptr<NFq::IPlanStatProcessor> StatProcessor;
};

TString LoadFile(const TString& file);

NKikimrServices::EServiceKikimr GetLogService(const TString& serviceName);

void ModifyLogPriorities(std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> logPriorities, NKikimrConfig::TLogConfig& logConfig);

void InitLogSettings(const NKikimrConfig::TLogConfig& logConfig, NActors::TTestActorRuntimeBase& runtime);

TChoices<NActors::NLog::EPriority> GetLogPrioritiesMap(const TString& optionName);

void SetupSignalActions();

void PrintResultSet(EResultOutputFormat format, IOutputStream& output, const Ydb::ResultSet& resultSet);

}  // namespace NKikimrRun
