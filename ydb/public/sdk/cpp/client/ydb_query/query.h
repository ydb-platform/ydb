#pragma once

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_query/stats.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <library/cpp/threading/future/future.h>

#include <variant>

namespace NYdb::NQuery {

enum class ESyntax {
    Unspecified = 0,
    YqlV1 = 1, // YQL
    Pg = 2, // PostgresQL
};

enum class EExecMode {
    Unspecified = 0,
    Parse = 10,
    Validate = 20,
    Explain = 30,
    Execute = 50,
};

enum class EStatsMode {
    Unspecified = 0,
    None = 10,
    Basic = 20,
    Full = 30,
    Profile = 40,
};

std::optional<EStatsMode> ParseStatsMode(std::string_view statsMode);
std::string_view StatsModeToString(const EStatsMode statsMode);

enum class EExecStatus {
    Unspecified = 0,
    Starting = 10,
    Aborted = 20,
    Canceled = 30,
    Completed = 40,
    Failed = 50,
};

class TExecuteQueryPart;
using TAsyncExecuteQueryPart = NThreading::TFuture<TExecuteQueryPart>;

class TExecuteQueryIterator : public TStatus {
    friend class TExecQueryImpl;
public:
    class TReaderImpl;

    TAsyncExecuteQueryPart ReadNext();

private:
    TExecuteQueryIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl) {}

    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

using TAsyncExecuteQueryIterator = NThreading::TFuture<TExecuteQueryIterator>;

struct TExecuteQuerySettings : public TRequestSettings<TExecuteQuerySettings> {
    FLUENT_SETTING_OPTIONAL(ui32, OutputChunkMaxSize);
    FLUENT_SETTING_DEFAULT(ESyntax, Syntax, ESyntax::YqlV1);
    FLUENT_SETTING_DEFAULT(EExecMode, ExecMode, EExecMode::Execute);
    FLUENT_SETTING_DEFAULT(EStatsMode, StatsMode, EStatsMode::None);
    FLUENT_SETTING_OPTIONAL(bool, ConcurrentResultSets);
};

struct TBeginTxSettings : public TRequestSettings<TBeginTxSettings> {};
struct TCommitTxSettings : public TRequestSettings<TCommitTxSettings> {};
struct TRollbackTxSettings : public TRequestSettings<TRollbackTxSettings> {};



class TCommitTransactionResult : public TStatus {
public:
    TCommitTransactionResult(TStatus&& status);
};

class TBeginTransactionResult;

using TAsyncBeginTransactionResult = NThreading::TFuture<TBeginTransactionResult>;
using TAsyncCommitTransactionResult = NThreading::TFuture<TCommitTransactionResult>;

struct TExecuteScriptSettings : public TOperationRequestSettings<TExecuteScriptSettings> {
    FLUENT_SETTING_DEFAULT(Ydb::Query::Syntax, Syntax, Ydb::Query::SYNTAX_YQL_V1);
    FLUENT_SETTING_DEFAULT(Ydb::Query::ExecMode, ExecMode, Ydb::Query::EXEC_MODE_EXECUTE);
    FLUENT_SETTING_DEFAULT(Ydb::Query::StatsMode, StatsMode, Ydb::Query::STATS_MODE_NONE);
    FLUENT_SETTING(TDuration, ResultsTtl);
};

class TQueryContent {
public:
    TQueryContent() = default;

    TQueryContent(const TString& text, ESyntax syntax)
        : Text(text)
        , Syntax(syntax)
    {}

    TString Text;
    ESyntax Syntax = ESyntax::Unspecified;
};

class TScriptExecutionOperation : public TOperation {
public:
    struct TMetadata {
        TString ExecutionId;
        EExecStatus ExecStatus = EExecStatus::Unspecified;
        EExecMode ExecMode = EExecMode::Unspecified;

        TQueryContent ScriptContent;
        Ydb::TableStats::QueryStats ExecStats;
        TVector<Ydb::Query::ResultSetMeta> ResultSetsMeta;
    };

    using TOperation::TOperation;
    TScriptExecutionOperation(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const {
        return Metadata_;
    }

private:
    TMetadata Metadata_;
};

struct TFetchScriptResultsSettings : public TRequestSettings<TFetchScriptResultsSettings> {
    FLUENT_SETTING(TString, FetchToken);
    FLUENT_SETTING_DEFAULT(ui64, RowsLimit, 1000);
};

class TFetchScriptResultsResult : public TStatus {
public:
    bool HasResultSet() const { return ResultSet_.Defined(); }
    ui64 GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }
    const TString& GetNextFetchToken() const { return NextFetchToken_; }

    explicit TFetchScriptResultsResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TFetchScriptResultsResult(TStatus&& status, TResultSet&& resultSet, i64 resultSetIndex, const TString& nextFetchToken)
        : TStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , NextFetchToken_(nextFetchToken)
    {}

private:
    TMaybe<TResultSet> ResultSet_;
    i64 ResultSetIndex_ = 0;
    TString NextFetchToken_;
};

class TExecuteQueryResult;
using TAsyncFetchScriptResultsResult = NThreading::TFuture<TFetchScriptResultsResult>;
using TAsyncExecuteQueryResult = NThreading::TFuture<TExecuteQueryResult>;

} // namespace NYdb::NQuery
