#pragma once

#include "fwd.h"

#include "stats.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/virtual_timestamp.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb::inline Dev::NQuery {

using TRetryOperationSettings = NYdb::NRetry::TRetryOperationSettings;

//! Query text syntax accepted by Query Service.
enum class ESyntax {
    //! Let the server choose the query syntax.
    Unspecified = 0,
    //! YQL version 1 syntax.
    YqlV1 = 1,
    //! PostgreSQL-compatible syntax.
    Pg = 2,
};

//! Controls how far the server processes a query.
enum class EExecMode {
    //! Let the server choose the execution mode.
    Unspecified = 0,
    //! Parse the query without validating or executing it.
    Parse = 10,
    //! Parse and validate the query without executing it.
    Validate = 20,
    //! Build an execution plan without executing the query.
    Explain = 30,
    //! Execute the query.
    Execute = 50,
};

//! Controls the amount of execution statistics returned by the server.
enum class EStatsMode {
    //! Let the server choose the statistics mode.
    Unspecified = 0,
    //! Do not collect execution statistics.
    None = 10,
    //! Collect aggregated table access statistics.
    Basic = 20,
    //! Add execution statistics and the query plan to basic statistics.
    Full = 30,
    //! Collect detailed task and channel statistics.
    Profile = 40,
};

//! Controls how often a result set schema is included in a response stream.
enum class ESchemaInclusionMode {
    //! Use the server default, which is equivalent to Always.
    Unspecified = 0,
    //! Include the schema in every result set part.
    Always = 1,
    //! Include the schema only in the first part of each result set.
    FirstOnly = 2,
};

//! Parses a lowercase statistics mode name, or returns std::nullopt for an unknown name.
std::optional<EStatsMode> ParseStatsMode(std::string_view statsMode);
//! Returns the lowercase name of a statistics mode.
std::string_view StatsModeToString(const EStatsMode statsMode);

//! Current state of an asynchronously executed script.
enum class EExecStatus {
    //! The execution state is not specified.
    Unspecified = 0,
    //! The script is being prepared for execution.
    Starting = 10,
    //! The script is running.
    Running = 15,
    //! The script was aborted by the server.
    Aborted = 20,
    //! The script was canceled.
    Canceled = 30,
    //! The script completed successfully.
    Completed = 40,
    //! The script execution failed.
    Failed = 50,
};

//! Apache Arrow result format settings.
struct TArrowFormatSettings {
    using TSelf = TArrowFormatSettings;

    //! Compression settings for Arrow record batches.
    struct TCompressionCodec {
        using TSelf = TCompressionCodec;

        //! Supported Arrow record batch compression codecs.
        enum class EType {
            //! Use the server default, which is equivalent to None.
            Unspecified = 0,
            //! Do not compress record batches.
            None = 1,
            //! Use Zstandard compression.
            Zstd = 2,
            //! Use LZ4 frame compression.
            Lz4Frame = 3,
        };

        //! Selects the compression codec; defaults to the server choice.
        FLUENT_SETTING_DEFAULT(EType, Type, EType::Unspecified);
        //! Sets the codec-specific compression level; the codec default is used when unset.
        FLUENT_SETTING_OPTIONAL(int32_t, Level);
    };

    //! Sets compression options for Arrow record batches.
    FLUENT_SETTING_OPTIONAL(TCompressionCodec, CompressionCodec);
};

using TAsyncExecuteQueryPart = NThreading::TFuture<TExecuteQueryPart>;

//! Asynchronous reader for a streaming query response.
class TExecuteQueryIterator : public TStatus {
    friend class TExecQueryImpl;
public:
    class TReaderImpl;

    //! Asynchronously reads the next response part. Read until the returned part reports EOS().
    TAsyncExecuteQueryPart ReadNext();

private:
    TExecuteQueryIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl) {}

    TExecuteQueryIterator(
        std::shared_ptr<TReaderImpl> impl,
        TStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl) {}

    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

using TAsyncExecuteQueryIterator = NThreading::TFuture<TExecuteQueryIterator>;

//! Settings for executing or streaming a query.
struct TExecuteQuerySettings : public TRequestSettings<TExecuteQuerySettings> {
    //! Limits one streamed result part to the specified number of bytes.
    FLUENT_SETTING_OPTIONAL(uint32_t, OutputChunkMaxSize);
    //! Selects the query syntax; defaults to YQL version 1.
    FLUENT_SETTING_DEFAULT(ESyntax, Syntax, ESyntax::YqlV1);
    //! Selects the execution mode; defaults to executing the query.
    FLUENT_SETTING_DEFAULT(EExecMode, ExecMode, EExecMode::Execute);
    //! Selects the statistics detail level; statistics are disabled by default.
    FLUENT_SETTING_DEFAULT(EStatsMode, StatsMode, EStatsMode::None);
    //! Allows parts of different result sets to be interleaved in the response stream.
    FLUENT_SETTING_OPTIONAL(bool, ConcurrentResultSets);
    //! Selects the workload manager resource pool used to execute the query.
    FLUENT_SETTING(std::string, ResourcePool);
    //! Requests periodic statistics at this interval while statistics collection is enabled.
    FLUENT_SETTING_OPTIONAL(std::chrono::milliseconds, StatsCollectPeriod);
    //! Selects how often result set schemas are included; defaults to the server behavior.
    FLUENT_SETTING_DEFAULT(ESchemaInclusionMode, SchemaInclusionMode, ESchemaInclusionMode::Unspecified);
    //! Selects the result set representation; defaults to the server's value format.
    FLUENT_SETTING_DEFAULT(TResultSet::EFormat, Format, TResultSet::EFormat::Unspecified);
    //! Sets Arrow-specific options used when Format is the Arrow representation.
    FLUENT_SETTING_OPTIONAL(TArrowFormatSettings, ArrowFormatSettings);
    //! Sets a per-request retry policy, overriding the client retry settings when present.
    FLUENT_SETTING_OPTIONAL(TRetryOperationSettings, RetrySettings);
};

//! Request settings for beginning a transaction.
struct TBeginTxSettings : public TRequestSettings<TBeginTxSettings> {};
//! Request settings for committing a transaction.
struct TCommitTxSettings : public TRequestSettings<TCommitTxSettings> {};
//! Request settings for rolling back a transaction.
struct TRollbackTxSettings : public TRequestSettings<TRollbackTxSettings> {};
//! Request settings for deleting a session.
struct TDeleteSessionSettings : public TRequestSettings<TDeleteSessionSettings> {
    //! Sets a per-request retry policy, overriding the client retry settings when present.
    FLUENT_SETTING_OPTIONAL(TRetryOperationSettings, RetrySettings);
};



//! Result of committing an interactive transaction.
class TCommitTransactionResult : public TStatus {
public:
    //! Constructs a commit result without a commit timestamp.
    TCommitTransactionResult(TStatus&& status);
    //! Constructs a commit result with an optional commit timestamp.
    TCommitTransactionResult(TStatus&& status, std::optional<NScheme::TVirtualTimestamp>&& commitTimestamp);

    //! Returns the commit timestamp when the successful transaction produced write effects.
    const std::optional<NScheme::TVirtualTimestamp>& GetCommitTimestamp() const { return CommitTimestamp_; }

private:
    std::optional<NScheme::TVirtualTimestamp> CommitTimestamp_;
};

using TAsyncBeginTransactionResult = NThreading::TFuture<TBeginTransactionResult>;
using TAsyncCommitTransactionResult = NThreading::TFuture<TCommitTransactionResult>;

//! Settings for starting asynchronous script execution.
struct TExecuteScriptSettings : public TOperationRequestSettings<TExecuteScriptSettings> {
    //! Selects the script syntax; defaults to YQL version 1.
    FLUENT_SETTING_DEFAULT(ESyntax, Syntax, ESyntax::YqlV1);
    //! Selects the execution mode; defaults to executing the script.
    FLUENT_SETTING_DEFAULT(EExecMode, ExecMode, EExecMode::Execute);
    //! Selects the statistics detail level; statistics are disabled by default.
    FLUENT_SETTING_DEFAULT(EStatsMode, StatsMode, EStatsMode::None);
    //! Sets how long completed script results remain available for fetching.
    FLUENT_SETTING(TDuration, ResultsTtl);
    //! Selects the workload manager resource pool used to execute the script.
    FLUENT_SETTING(std::string, ResourcePool);
    //! Sets a per-request retry policy, overriding the client retry settings when present.
    FLUENT_SETTING_OPTIONAL(TRetryOperationSettings, RetrySettings);
};

//! Query text together with its syntax.
class TQueryContent {
public:
    //! Constructs empty query content with unspecified syntax.
    TQueryContent() = default;

    //! Constructs query content from text and syntax.
    TQueryContent(const std::string& text, ESyntax syntax)
        : Text(text)
        , Syntax(syntax)
    {}

    //! Query text.
    std::string Text;
    //! Syntax used by the query text.
    ESyntax Syntax = ESyntax::Unspecified;
};

//! Metadata describing one script result set.
class TResultSetMeta {
public:
    //! Constructs empty result set metadata.
    TResultSetMeta() = default;

    //! Constructs metadata by copying the result set columns.
    explicit TResultSetMeta(const std::vector<TColumn>& columns, uint64_t rowsCount = 0, bool finished = false)
        : Columns(columns)
        , RowsCount(rowsCount)
        , Finished(finished)
    {}

    //! Constructs metadata by taking ownership of the result set columns.
    explicit TResultSetMeta(std::vector<TColumn>&& columns, uint64_t rowsCount = 0, bool finished = false)
        : Columns(std::move(columns))
        , RowsCount(rowsCount)
        , Finished(finished)
    {}

    //! Result set column descriptions.
    std::vector<TColumn> Columns;
    //! Number of rows currently available in the result set.
    uint64_t RowsCount = 0;
    //! Whether the result set is complete.
    bool Finished = false;
};

//! Long-running operation returned by ExecuteScript().
class TScriptExecutionOperation : public TOperation {
public:
    //! Script execution metadata reported by the server.
    struct TMetadata {
        //! Server-side script execution identifier.
        std::string ExecutionId;
        //! Current script execution state.
        EExecStatus ExecStatus = EExecStatus::Unspecified;
        //! Execution mode used for the script.
        EExecMode ExecMode = EExecMode::Unspecified;

        //! Submitted script text and syntax.
        TQueryContent ScriptContent;
        //! Script execution statistics.
        TExecStats ExecStats;
        //! Metadata for the script's result sets.
        std::vector<TResultSetMeta> ResultSetsMeta;
    };

    //! Inherits constructors for operation states that do not contain script metadata.
    using TOperation::TOperation;
    //! Constructs an operation and extracts script metadata from the wire response.
    TScriptExecutionOperation(TStatus&& status, Ydb::Operations::Operation&& operation);

    //! Returns the script execution metadata.
    const TMetadata& Metadata() const {
        return Metadata_;
    }

private:
    TMetadata Metadata_;
};

//! Settings for fetching one page of script results.
struct TFetchScriptResultsSettings : public TRequestSettings<TFetchScriptResultsSettings> {
    //! Sets the continuation token returned by the previous fetch request.
    FLUENT_SETTING(std::string, FetchToken);
    //! Sets the maximum number of rows to fetch; defaults to 1000.
    FLUENT_SETTING_DEFAULT(uint64_t, RowsLimit, 1000);
    //! Sets a per-request retry policy, overriding the client retry settings when present.
    FLUENT_SETTING_OPTIONAL(TRetryOperationSettings, RetrySettings);
};

//! Result of fetching one page of script results.
class TFetchScriptResultsResult : public TStatus {
public:
    //! Returns whether this response contains a result set.
    bool HasResultSet() const { return ResultSet_.has_value(); }
    //! Returns the index of the result set in this response. HasResultSet() must be true.
    uint64_t GetResultSetIndex() const { return ResultSetIndex_; }
    //! Returns the result set. HasResultSet() must be true.
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    //! Moves the result set out of this response. HasResultSet() must be true.
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }
    //! Returns the continuation token for the next page, or an empty string at the end.
    const std::string& GetNextFetchToken() const { return NextFetchToken_; }

    //! Constructs a fetch result that does not contain a result set.
    explicit TFetchScriptResultsResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    //! Constructs a successful fetch result containing a result set page.
    TFetchScriptResultsResult(TStatus&& status, TResultSet&& resultSet, int64_t resultSetIndex, const std::string& nextFetchToken)
        : TStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , NextFetchToken_(nextFetchToken)
    {}

private:
    std::optional<TResultSet> ResultSet_;
    int64_t ResultSetIndex_ = 0;
    std::string NextFetchToken_;
};

using TAsyncFetchScriptResultsResult = NThreading::TFuture<TFetchScriptResultsResult>;
using TAsyncExecuteQueryResult = NThreading::TFuture<TExecuteQueryResult>;

} // namespace NYdb::NQuery
