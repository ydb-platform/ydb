#pragma once

#include "command.h"
#include "formats.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandWithResponseHeaders {
protected:
    void PrintResponseHeader(const NYdb::TStatus& status);
    void PrintResponseHeaderPretty(const NYdb::TStatus& status);

    bool ShowHeaders = false;
    static const TString ResponseHeadersHelp;
};

class TCommandWithFormat {
protected:
    void AddInputFormats(TClientCommand::TConfig& config, 
                         const TVector<EOutputFormat>& allowedFormats, EOutputFormat defaultFormat = EOutputFormat::JsonUnicode);
    void AddStdinFormats(TClientCommand::TConfig& config, const TVector<EOutputFormat>& allowedStdinFormats, 
                         const TVector<EOutputFormat>& allowedFramingFormats);
    void AddFormats(TClientCommand::TConfig& config, 
                         const TVector<EOutputFormat>& allowedFormats, EOutputFormat defaultFormat = EOutputFormat::Pretty);
    void AddMessagingFormats(TClientCommand::TConfig& config, const TVector<EMessagingFormat>& allowedFormats);
    void ParseFormats();
    void ParseMessagingFormats();

    // Deprecated
    void AddDeprecatedJsonOption(TClientCommand::TConfig& config,
        const TString& description = "(Deprecated, will be removed soon. Use --format option instead)"
        " Output in json format");

protected:
    EOutputFormat OutputFormat = EOutputFormat::Default;
    EOutputFormat InputFormat = EOutputFormat::Default;
    EOutputFormat FramingFormat = EOutputFormat::Default;
    EOutputFormat StdinFormat = EOutputFormat::Default;
    TVector<EOutputFormat> StdinFormats;
    EMessagingFormat MessagingFormat = EMessagingFormat::SingleMessage;

private:
    TVector<EOutputFormat> AllowedInputFormats;
    TVector<EOutputFormat> AllowedStdinFormats;
    TVector<EOutputFormat> AllowedFramingFormats;
    TVector<EOutputFormat> AllowedFormats;
    TVector<EMessagingFormat> AllowedMessagingFormats;
    bool DeprecatedOptionUsed = false;
    
protected:
    bool IsStdinFormatSet = false;
    bool IsFramingFormatSet = false;
};

class TResultSetPrinter {
public:
    TResultSetPrinter(EOutputFormat format, std::function<bool()> isInterrupted = []() { return false; });
    ~TResultSetPrinter();

    void Print(const TResultSet& resultSet);

    // Should be called between result sets
    void Reset();

private:
    void BeginResultSet();
    void EndResultSet();
    void EndLineBeforeNextResult();

    void PrintPretty(const TResultSet& resultSet);
    void PrintJsonArray(const TResultSet& resultSet, EBinaryStringEncoding encoding);
    void PrintCsv(const TResultSet& resultSet, const char* delim);

    bool FirstPart = true;
    bool PrintedSomething = false;
    EOutputFormat Format;
    std::function<bool()> IsInterrupted;
};

class TQueryPlanPrinter {
public:
    TQueryPlanPrinter(EOutputFormat format, bool analyzeMode = false)
        : Format(format)
        , AnalyzeMode(analyzeMode) {}

    void Print(const TString& plan);

private:
    void PrintPretty(const NJson::TJsonValue& plan);
    void PrintPrettyImpl(const NJson::TJsonValue& plan, TVector<TString>& offsets);
    void PrintJson(const TString& plan);
    TString JsonToString(const NJson::TJsonValue& jsonValue);

    EOutputFormat Format;
    bool AnalyzeMode;
};

}
}
