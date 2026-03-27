#pragma once

#include "interruptable.h"

#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient {

class TExplainGenericQuery final : public TInterruptableCommand {
public:
    explicit TExplainGenericQuery(const TDriver& driver);

    struct TResult {
        TString PlanJson;
        TString Ast;
    };

    TResult Explain(const TString& query, std::optional<TDuration> timeout = std::nullopt, bool analyze = false);

private:
    NQuery::TQueryClient Client;
};

class TExecuteGenericQuery : public TInterruptableCommand {
public:
    explicit TExecuteGenericQuery(const TDriver& driver);

    struct TSettings {
        TString DiagnosticsFile;
        bool ExplainMode = false;
        bool ExplainAnalyzeMode = false;
        bool ExplainAst = false;
        EDataFormat OutputFormat = EDataFormat::Default;
        NQuery::TExecuteQuerySettings Settings;
        std::optional<TParams> Parameters;
        bool AddIndent = false;
    };

    int Execute(const TString& query, const TSettings& execSettings);

protected:
    virtual void OnResultPart(ui64 resultSetIndex, const TResultSet& resultSet);

private:
    NQuery::TAsyncExecuteQueryIterator StartQuery(const TString& query, const TSettings& execSettings);

    int PrintResponse(NQuery::TExecuteQueryIterator& result, const TString& query, const TSettings& execSettings);

private:
    const TDriver Driver;
    NQuery::TQueryClient Client;
};

} // namespace NYdb::NConsoleClient
