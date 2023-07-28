#pragma once

///
/// @file yt/cpp/mapreduce/interface/errors.h
///
/// Errors and exceptions emitted by library.

#include "fwd.h"
#include "common.h"

#include <library/cpp/yson/node/node.h>

#include <util/generic/bt_exception.h>
#include <util/generic/yexception.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Error that is thrown when library detects invalid usage of API.
///
/// For example trying to start operations on empty table list.
class TApiUsageError
    : public TWithBackTrace<yexception>
{ };

///
/// @brief Error that is thrown when request retries continues for too long.
///
/// @see NYT::TRetryConfig
/// @see NYT::IRetryConfigProvider
class TRequestRetriesTimeout
    : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Error returned by YT cluster.
///
/// An object of this class describe error that happened on YT server.
/// Internally each error is a tree. Each node of the tree contains:
///   - integer error code;
///   - text description of error;
///   - attributes describing error context.
///
/// To get text description of an error one should use
/// @ref NYT::TYtError::ShortDescription or @ref NYT::TYtError::FullDescription
///
/// To distinguish between error kinds @ref NYT::TYtError::ContainsErrorCode should be used.
///
/// @see NYT::TErrorResponse
/// @see NYT::TOperationFailedError
class TYtError
{
public:
    /// Constructs error with NYT::NClusterErrorCodes::OK code and empty message.
    TYtError();

    /// Constructs error with NYT::NClusterErrorCodes::Generic code and given message.
    explicit TYtError(const TString& message);

    /// Constructs error with given code and given message.
    TYtError(int code, const TString& message);

    /// Construct error from json representation.
    TYtError(const ::NJson::TJsonValue& value);

    /// Construct error from TNode representation.
    TYtError(const TNode& value);

    ///
    /// @brief Check if error or any of inner errors has given error code.
    ///
    /// Use this method to distinguish kind of error.
    bool ContainsErrorCode(int code) const;

    ///
    /// @brief Get short description of error.
    ///
    /// Short description contain text description of error and all inner errors.
    /// It is human readable but misses some important information (error codes, error attributes).
    ///
    /// Usually it's better to use @ref NYT::TYtError::FullDescription to log errors.
    TString ShortDescription() const;

    ///
    /// @brief Get full description of error.
    ///
    /// Full description contains readable short description
    /// followed by text yson representation of error that contains error codes and attributes.
    TString FullDescription() const;

    ///
    /// @brief Get error code of the topmost error.
    ///
    /// @warning Do not use this method to distinguish between error kinds
    /// @ref NYT::TYtError::ContainsErrorCode should be used instead.
    int GetCode() const;

    ///
    /// @brief Get error text of the topmost error.
    ///
    /// @warning This method should not be used to log errors
    /// since text description of inner errors is going to be lost.
    /// @ref NYT::TYtError::FullDescription should be used instead.
    const TString& GetMessage() const;

    ///
    /// @brief Check if error or any of inner errors contains given text chunk.
    ///
    /// @warning @ref NYT::TYtError::ContainsErrorCode must be used instead of
    /// this method when possible. If there is no suitable error code it's
    /// better to ask yt@ to add one. This method should only be used as workaround.
    bool ContainsText(const TStringBuf& text) const;

    /// @brief Get inner errors.
    const TVector<TYtError>& InnerErrors() const;

    /// Parse error from json string.
    void ParseFrom(const TString& jsonError);

    /// Collect error codes from entire error tree.
    TSet<int> GetAllErrorCodes() const;

    /// Check if error has any attributes.
    bool HasAttributes() const;

    /// Get error attributes.
    const TNode::TMapType& GetAttributes() const;

    /// Get text yson representation of error
    TString GetYsonText() const;

private:
    int Code_;
    TString Message_;
    TVector<TYtError> InnerErrors_;
    TNode::TMapType Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Generic error response returned by server.
///
/// TErrorResponse can be thrown from almost any client method when server responds with error.
///
class TErrorResponse
    : public yexception
{
public:
    TErrorResponse(int httpCode, const TString& requestId);
    TErrorResponse(int httpCode, TYtError error);

    /// Get error object returned by server.
    const TYtError& GetError() const;

    /// Get if (correlation-id) of request that was responded with error.
    TString GetRequestId() const;

    /// Get HTTP code of response.
    int GetHttpCode() const;

    /// Is error parsed from response trailers.
    bool IsFromTrailers() const;

    /// Check if error was caused by transport problems inside YT cluster.
    bool IsTransportError() const;

    /// Check if error was caused by failure to resolve cypress path.
    bool IsResolveError() const;

    /// Check if error was caused by lack of permissions to execute request.
    bool IsAccessDenied() const;

    /// Check if error was caused by failure to lock object because of another transaction is holding lock.
    bool IsConcurrentTransactionLockConflict() const;

    /// Check if error was caused by request quota limit exceeding.
    bool IsRequestRateLimitExceeded() const;

    // YT can't serve request because it is overloaded.
    bool IsRequestQueueSizeLimitExceeded() const;

    /// Check if error was caused by failure to get chunk. Such errors are almost always temporary.
    bool IsChunkUnavailable() const;

    /// Check if error was caused by internal YT timeout.
    bool IsRequestTimedOut() const;

    /// Check if error was caused by trying to work with transaction that was finished or never existed.
    bool IsNoSuchTransaction() const;

    // User reached their limit of concurrently running operations.
    bool IsConcurrentOperationsLimitReached() const;

    /// @deprecated This method must not be used.
    bool IsOk() const;

    void SetRawError(const TString& message);
    void SetError(TYtError error);
    void ParseFromJsonError(const TString& jsonError);
    void SetIsFromTrailers(bool isFromTrailers);

private:
    void Setup();

private:
    int HttpCode_;
    TString RequestId_;
    TYtError Error_;
    bool IsFromTrailers_ = false;
};

////////////////////////////////////////////////////////////////////////////////

/// Info about failed jobs.
///
/// @see NYT::TOperationFailedError
struct TFailedJobInfo
{
    /// Id of a job.
    TJobId JobId;

    /// Error describing job failure.
    TYtError Error;

    /// Stderr of job.
    ///
    /// @note YT doesn't store all job stderrs, check @ref NYT::IOperationClient::GetJobStderr
    /// for list of limitations.
    ///
    /// @see NYT::IOperationClient::GetJobStderr
    TString Stderr;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Error that is thrown when operation watched by library fails.
///
/// This error is thrown from operation starting methods when they are started in sync mode (@ refNYT::TOperationOptions::Wait == true)
/// or from future returned by NYT::IOperation::Watch.
///
/// @see NYT::IOperationClient
class TOperationFailedError
    : public yexception
{
public:
    /// Final state of operation.
    enum EState {
        /// Operation was failed due to some error.
        Failed,
        /// Operation didn't experienced errors, but was aborted by user request or by YT.
        Aborted,
    };

public:
    TOperationFailedError(EState state, TOperationId id, TYtError ytError, TVector<TFailedJobInfo> failedJobInfo);

    /// Get final state of operation.
    EState GetState() const;

    /// Get operation id.
    TOperationId GetOperationId() const;

    /// Return operation error.
    const TYtError& GetError() const;

    /// Return info about failed jobs (if any).
    const TVector<TFailedJobInfo>& GetFailedJobInfo() const;

private:
    EState State_;
    TOperationId OperationId_;
    TYtError Error_;
    TVector<TFailedJobInfo> FailedJobInfo_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
