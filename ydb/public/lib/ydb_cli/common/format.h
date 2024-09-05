#pragma once

#include "command.h"
#include "formats.h"
#include "pretty_table.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

namespace NYdb {

    class TResultSetParquetPrinter;

}

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
                         const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat = EDataFormat::JsonUnicode);
    void AddStdinFormats(TClientCommand::TConfig& config, const TVector<EDataFormat>& allowedStdinFormats, 
                         const TVector<EDataFormat>& allowedFramingFormats);
    void AddOutputFormats(TClientCommand::TConfig& config, 
                         const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat = EDataFormat::Pretty);
    void AddMessagingFormats(TClientCommand::TConfig& config, const TVector<EMessagingFormat>& allowedFormats);
    void ParseFormats();
    void ParseMessagingFormats();

    // Deprecated
    void AddDeprecatedJsonOption(TClientCommand::TConfig& config,
        const TString& description = "(Deprecated, will be removed soon. Use --format option instead)"
        " Output in json format");

protected:
    EDataFormat OutputFormat = EDataFormat::Default;
    EDataFormat InputFormat = EDataFormat::Default;
    EDataFormat FramingFormat = EDataFormat::Default;
    EDataFormat StdinFormat = EDataFormat::Default;
    TVector<EDataFormat> StdinFormats;
    EMessagingFormat MessagingFormat = EMessagingFormat::SingleMessage;

private:
    TVector<EDataFormat> AllowedInputFormats;
    TVector<EDataFormat> AllowedStdinFormats;
    TVector<EDataFormat> AllowedFramingFormats;
    TVector<EDataFormat> AllowedFormats;
    TVector<EMessagingFormat> AllowedMessagingFormats;
    bool DeprecatedOptionUsed = false;
    
protected:
    bool IsStdinFormatSet = false;
    bool IsFramingFormatSet = false;
};

class TResultSetPrinter {
public:
    TResultSetPrinter(EDataFormat format, std::function<bool()> isInterrupted = []() { return false; });
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
    EDataFormat Format;
    std::function<bool()> IsInterrupted;
    std::unique_ptr<TResultSetParquetPrinter> ParquetPrinter;
};

class TQueryPlanPrinter {
public:
    TQueryPlanPrinter(EDataFormat format, bool analyzeMode = false, IOutputStream& output = Cout, size_t maxWidth = 0)
        : Format(format)
        , AnalyzeMode(analyzeMode)
        , Output(output)
        , MaxWidth(maxWidth)
        {}

    void Print(const TString& plan);

private:
    void PrintPretty(const NJson::TJsonValue& plan);
    void PrintPrettyImpl(const NJson::TJsonValue& plan, TVector<TString>& offsets);
    void PrintPrettyTable(const NJson::TJsonValue& plan);
    void PrintPrettyTableImpl(const NJson::TJsonValue& plan, TString& offset, TPrettyTable& table);
    void PrintJson(const TString& plan);
    void PrintSimplifyJson(const NJson::TJsonValue& plan);
    TString JsonToString(const NJson::TJsonValue& jsonValue);

private:
    EDataFormat Format;
    bool AnalyzeMode;
    IOutputStream& Output;
    size_t MaxWidth;
};

}
}
