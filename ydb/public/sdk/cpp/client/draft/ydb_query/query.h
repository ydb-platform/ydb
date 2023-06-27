#pragma once

#include <ydb/public/api/grpc/draft/ydb_query_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_query/stats.h>
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

enum class EExecStatus {
    Unspecified = 0,
    Starting = 10,
    Aborted = 20,
    Canceled = 30,
    Completed = 40,
    Failed = 50,
};

class TExecuteQueryPart : public TStreamPartStatus {
public:
    bool HasResultSet() const { return ResultSet_.Defined(); }
    ui64 GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    const TMaybe<TExecStats>& GetStats() const { return Stats_; }

    TExecuteQueryPart(TStatus&& status, TMaybe<TExecStats>&& queryStats)
        : TStreamPartStatus(std::move(status))
        , Stats_(std::move(queryStats))
    {}

    TExecuteQueryPart(TStatus&& status, TResultSet&& resultSet, i64 resultSetIndex, TMaybe<TExecStats>&& queryStats)
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , Stats_(std::move(queryStats))
    {}

private:
    TMaybe<TResultSet> ResultSet_;
    i64 ResultSetIndex_ = 0;
    TMaybe<TExecStats> Stats_;
};

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
    FLUENT_SETTING_DEFAULT(ESyntax, Syntax, ESyntax::YqlV1);
    FLUENT_SETTING_DEFAULT(EExecMode, ExecMode, EExecMode::Execute);
    FLUENT_SETTING_DEFAULT(EStatsMode, StatsMode, EStatsMode::None);
};

class TExecuteQueryResult : public TStatus {
public:
    const TVector<TResultSet>& GetResultSets() const;
    TResultSet GetResultSet(size_t resultIndex) const;
    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    const TMaybe<TExecStats>& GetStats() const { return Stats_; }

    TExecuteQueryResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TExecuteQueryResult(TStatus&& status, TVector<TResultSet>&& resultSets, TMaybe<TExecStats>&& stats)
        : TStatus(std::move(status))
        , ResultSets_(std::move(resultSets))
        , Stats_(std::move(stats))
    {}

private:
    TVector<TResultSet> ResultSets_;
    TMaybe<TExecStats> Stats_;
};

using TAsyncExecuteQueryResult = NThreading::TFuture<TExecuteQueryResult>;

struct TExecuteScriptSettings : public TOperationRequestSettings<TExecuteScriptSettings> {
    FLUENT_SETTING_DEFAULT(Ydb::Query::ExecMode, ExecMode, Ydb::Query::EXEC_MODE_EXECUTE);
};

class TVersionedScriptId {
public:
    TVersionedScriptId(const TString& id, TMaybe<i64> revision = Nothing())
        : Id_(id)
        , Revision_(revision)
    {}

    const TString& Id() const {
        return Id_;
    }

    TMaybe<i64> Revision() const {
        return Revision_;
    }

    void SetRevision(i64 revision) {
        Revision_ = revision;
    }

private:
    TString Id_;
    TMaybe<i64> Revision_;
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

        // Not greater than one of SavedScriptId or QueryContent is set.
        std::optional<TVersionedScriptId> ScriptId;
        TQueryContent ScriptContent;
        Ydb::TableStats::QueryStats ExecStats;
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
    FLUENT_SETTING_DEFAULT(ui64, RowsOffset, 0);
    FLUENT_SETTING_DEFAULT(ui64, RowsLimit, 1000);
};

class TFetchScriptResultsResult : public TStatus {
public:
    bool HasResultSet() const { return ResultSet_.Defined(); }
    ui64 GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }
    const TString& NextPageToken() const { return NextPageToken_; }

    explicit TFetchScriptResultsResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TFetchScriptResultsResult(TStatus&& status, TResultSet&& resultSet, i64 resultSetIndex, const TString& nextPageToken)
        : TStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , NextPageToken_(nextPageToken)
    {}

private:
    TMaybe<TResultSet> ResultSet_;
    i64 ResultSetIndex_ = 0;
    TString NextPageToken_;
};

using TAsyncFetchScriptResultsResult = NThreading::TFuture<TFetchScriptResultsResult>;

} // namespace NYdb::NQuery
