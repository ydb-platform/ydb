#pragma once

#include "command.h"
#include "formats.h"
#include "pretty_table.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <util/generic/set.h>

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
    // Has both input and output
    bool IsIoCommand();
    virtual bool HasInput();
    virtual bool HasOutput();
};

class TCommandWithInput: virtual public TCommandWithFormat {
protected:
    void AddInputFormats(TClientCommand::TConfig& config, const TVector<EDataFormat>& allowedFormats,
        EDataFormat defaultFormat = EDataFormat::Json);
    void AddLegacyInputFormats(TClientCommand::TConfig& config, const TString& legacyName,
        const TVector<TString>& newNames,
        const TVector<EDataFormat>& allowedFormats);
    void AddLegacyJsonInputFormats(TClientCommand::TConfig& config);
    void AddInputFramingFormats(TClientCommand::TConfig &config, const TVector<EFramingFormat>& allowedFormats,
        EFramingFormat defaultFormat = EFramingFormat::NoFraming);
    void AddInputBinaryStringEncodingFormats(TClientCommand::TConfig &config,
        const TVector<EBinaryStringEncodingFormat>& allowedFormats,
        EBinaryStringEncodingFormat defaultFormat = EBinaryStringEncodingFormat::Unicode);
    void AddInputFileOption(TClientCommand::TConfig& config, bool allowMultiple,
        const TString& description = "File name with input data");
    void ParseInputFormats();

    virtual THashMap<EDataFormat, TString>& GetInputFormatDescriptions();
    virtual bool HasInput() override;

protected:
    TVector<TString> InputFiles;
    bool AllowMultipleInputFiles;
    EDataFormat InputFormat = EDataFormat::Default;
    EFramingFormat InputFramingFormat = EFramingFormat::Default;
    EBinaryStringEncodingFormat InputBinaryStringEncodingFormat = EBinaryStringEncodingFormat::Default;
    EBinaryStringEncoding InputBinaryStringEncoding;

private:
    TVector<EDataFormat> LegacyInputFormats;
    TSet<EDataFormat> AllowedInputFormats;
    TSet<EFramingFormat> AllowedInputFramingFormats;
    TSet<EBinaryStringEncodingFormat> AllowedBinaryStringEncodingFormats;
};

class TCommandWithOutput: virtual public TCommandWithFormat {
protected:
    void AddOutputFormats(TClientCommand::TConfig& config, 
                         const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat = EDataFormat::Pretty);
    void ParseOutputFormats();

    // Deprecated
    void AddDeprecatedJsonOption(TClientCommand::TConfig& config,
        const TString& description = "(Deprecated, will be removed soon. Use --format option instead)"
        " Output in json format");

protected:
    EDataFormat OutputFormat = EDataFormat::Default;

private:
    TVector<EDataFormat> AllowedFormats;
    bool DeprecatedOptionUsed = false;
};

class TCommandWithMessagingFormat {
protected:
    void AddMessagingFormats(TClientCommand::TConfig& config, const TVector<EMessagingFormat>& allowedFormats);
    void ParseMessagingFormats();

protected:
    EMessagingFormat MessagingFormat = EMessagingFormat::SingleMessage;

private:
    TVector<EMessagingFormat> AllowedMessagingFormats;
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
