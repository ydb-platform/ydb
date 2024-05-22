#pragma once

///
/// @file yt/cpp/mapreduce/interface/client.h
///
/// Main header of the C++ YT Wrapper.

///
/// @mainpage C++ library for working with YT
///
/// This library provides possibilities to work with YT as a [MapReduce](https://en.wikipedia.org/wiki/MapReduce) system. It allows:
///   - to read/write tables and files
///   - to run operations
///   - to work with transactions.
///
/// This library provides only basic functions for working with dynamic tables.
/// To access full powers of YT dynamic tables one should use
/// [yt/client](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/client) library.
///
/// Entry points to this library:
///   - @ref NYT::Initialize() initialization function for this library;
///   - @ref NYT::IClient main interface to work with YT cluster;
///   - @ref NYT::CreateClient() function that creates client for particular cluster;
///   - @ref NYT::IOperationClient ancestor of @ref NYT::IClient containing the set of methods to run operations.
///
/// Tutorial on how to use this library can be found [here](https://ytsaurus.tech/docs/en/api/c/description).

#include "fwd.h"

#include "client_method_options.h"
#include "constants.h"
#include "batch_request.h"
#include "cypress.h"
#include "init.h"
#include "io.h"
#include "node.h"
#include "operation.h"

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/compiler.h>

/// Main namespace of YT client
namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// OAuth info (returned by @ref NYT::IClient::WhoAmI).
struct TAuthorizationInfo
{
    /// User's login.
    TString Login;

    /// Realm.
    TString Realm;
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Part of @ref NYT::TCheckPermissionResponse.
///
/// In case when 'Action == ESecurityAction::Deny' because of a 'deny' rule,
/// the "denying" object name and id and "denied" subject name an id may be returned.
struct TCheckPermissionResult
{
    /// Was the access granted or not.
    ESecurityAction Action;

    /// Id of the object whose ACL's "deny" rule forbids the access.
    TMaybe<TGUID> ObjectId;

    ///
    /// @brief Name of the object whose ACL's "deny" rule forbids the access.
    ///
    /// Example is "node //tmp/x/y".
    TMaybe<TString> ObjectName;

    /// Id of the subject for whom the access was denied by a "deny" rule.
    TMaybe<TGUID> SubjectId;

    /// Name of the subject for whom the access was denied by a "deny" rule.
    TMaybe<TString> SubjectName;
};

/// @brief Result of @ref NYT::IClient::CheckPermission command.
///
/// The base part of the response corresponds to the check result for the node itself.
/// `Columns` vector contains check results for the columns (in the same order as in the request).
struct TCheckPermissionResponse
    : public TCheckPermissionResult
{
    /// @brief Results for the table columns access permissions.
    ///
    /// @see [Columnar ACL doc](https://ytsaurus.tech/docs/en/user-guide/storage/columnar-acl)
    TVector<TCheckPermissionResult> Columns;
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Interface representing a lock obtained from @ref NYT::ITransaction::Lock.
///
/// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#start_tx)
class ILock
    : public TThrRefBase
{
public:
    virtual ~ILock() = default;

    /// Get cypress node id of lock itself.
    virtual const TLockId& GetId() const = 0;

    /// Get cypress node id of locked object.
    virtual TNodeId GetLockedNodeId() const = 0;

    ///
    /// @brief Get future that will be set once lock is in "acquired" state.
    ///
    /// Note that future might contain exception if some error occurred
    /// e.g. lock transaction was aborted.
    virtual const ::NThreading::TFuture<void>& GetAcquiredFuture() const = 0;

    ///
    /// @brief Wait until lock is in "acquired" state.
    ///
    /// Throws exception if timeout exceeded or some error occurred
    /// e.g. lock transaction was aborted.
    void Wait(TDuration timeout = TDuration::Max());
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Base class for @ref NYT::IClient and @ref NYT::ITransaction.
///
/// This class contains transactional commands.
class IClientBase
    : public TThrRefBase
    , public ICypressClient
    , public IIOClient
    , public IOperationClient
{
public:
    ///
    /// @brief Start a [transaction] (https://ytsaurus.tech/docs/en/user-guide/storage/transactions.html#master_transactions).
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#start_tx)
    [[nodiscard]] virtual ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options = TStartTransactionOptions()) = 0;

    ///
    /// @brief Change properties of table.
    ///
    /// Allows to:
    ///   - switch table between dynamic/static mode
    ///   - or change table schema
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#alter_table)
    virtual void AlterTable(
        const TYPath& path,
        const TAlterTableOptions& options = TAlterTableOptions()) = 0;

    ///
    /// @brief Create batch request object that allows to execute several light requests in parallel.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#execute_batch)
    virtual TBatchRequestPtr CreateBatchRequest() = 0;

    /// @brief Get root client outside of all transactions.
    virtual IClientPtr GetParentClient() = 0;
};

////////////////////////////////////////////////////////////////////////////////


/// @brief Interface representing a master transaction.
///
/// @see [YT doc](https://ytsaurus.tech/docs/en/user-guide/storage/transactions.html#master_transactions)
class ITransaction
    : virtual public IClientBase
{
public:
    /// Get id of transaction.
    virtual const TTransactionId& GetId() const = 0;

    ///
    /// @brief Try to lock given path.
    ///
    /// Lock will be held until transaction is committed/aborted or @ref NYT::ITransaction::Unlock method is called.
    /// Lock modes:
    ///   - `LM_EXCLUSIVE`: if exclusive lock is taken no other transaction can take exclusive or shared lock.
    ///   - `LM_SHARED`: if shared lock is taken other transactions can take shared lock but not exclusive.
    ///   - `LM_SNAPSHOT`: snapshot lock always succeeds, when snapshot lock is taken current transaction snapshots object.
    ///  It will not see changes that occurred to it in other transactions.
    ///
    /// Exclusive/shared lock can be waitable or not.
    /// If nonwaitable lock cannot be taken exception is thrown.
    /// If waitable lock cannot be taken it is created in pending state and client can wait until it actually taken.
    /// Check @ref NYT::TLockOptions::Waitable and @ref NYT::ILock::GetAcquiredFuture for more details.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#lock)
    virtual ILockPtr Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) = 0;

    ///
    /// @brief Remove all the locks (including pending ones) for this transaction from a Cypress node at `path`.
    ///
    /// If the locked version of the node differs from the original one,
    /// an error will be thrown.
    ///
    /// Command is successful even if the node has no locks.
    /// Only explicit (created by @ref NYT::ITransaction::Lock) locks are removed.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#unlock)
    virtual void Unlock(
        const TYPath& path,
        const TUnlockOptions& options = TUnlockOptions()) = 0;

    ///
    /// @brief Commit transaction.
    ///
    /// All changes that are made by transactions become visible globally or to parent transaction.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#commit_tx)
    virtual void Commit() = 0;

    ///
    /// @brief Abort transaction.
    ///
    /// All changes made by current transaction are lost.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#abort_tx)
    virtual void Abort() = 0;

    /// @brief Explicitly ping transaction.
    ///
    /// User usually does not need this method (as transactions are pinged automatically,
    /// see @ref NYT::TStartTransactionOptions::AutoPingable).
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#ping_tx)
    virtual void Ping() = 0;

    ///
    /// @brief Detach transaction.
    ///
    /// Stop any activities connected with it: pinging, aborting on crashes etc.
    /// Forget about the transaction totally.
    virtual void Detach();
};

////////////////////////////////////////////////////////////////////////////////

/// Interface containing non-transactional commands.
class IClient
    : virtual public IClientBase
{
public:
    ///
    /// @brief Attach to existing master transaction.
    ///
    /// Returned object WILL NOT:
    ///  - ping transaction automatically (unless @ref NYT::TAttachTransactionOptions::AutoPing is set)
    ///  - abort it on program termination (unless @ref NYT::TAttachTransactionOptions::AbortOnTermination is set).
    /// Otherwise returned object is similar to the object returned by @ref NYT::IClientBase::StartTransaction.
    /// and it can see all the changes made inside the transaction.
    [[nodiscard]] virtual ITransactionPtr AttachTransaction(
        const TTransactionId& transactionId,
        const TAttachTransactionOptions& options = TAttachTransactionOptions()) = 0;

    ///
    /// @brief Mount dynamic table.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#mount_table)
    virtual void MountTable(
        const TYPath& path,
        const TMountTableOptions& options = TMountTableOptions()) = 0;

    ///
    /// @brief Unmount dynamic table.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#unmount_table)
    virtual void UnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options = TUnmountTableOptions()) = 0;

    ///
    /// @brief Remount dynamic table.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#remount_table)
    virtual void RemountTable(
        const TYPath& path,
        const TRemountTableOptions& options = TRemountTableOptions()) = 0;

    ///
    /// @brief Switch dynamic table from `mounted' into `frozen' state.
    ///
    /// When table is in frozen state all its data is flushed to disk and writes are disabled.
    ///
    /// @note this function launches the process of switching, but doesn't wait until switching is accomplished.
    /// Waiting has to be performed by user.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#freeze_table)
    virtual void FreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options = TFreezeTableOptions()) = 0;

    ///
    /// @brief Switch dynamic table from `frozen` into `mounted` state.
    ///
    /// @note this function launches the process of switching, but doesn't wait until switching is accomplished.
    /// Waiting has to be performed by user.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#unfreeze_table)
    virtual void UnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options = TUnfreezeTableOptions()) = 0;

    ///
    /// @brief Reshard dynamic table (break it into tablets) by given pivot keys.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#reshard_table)
    virtual void ReshardTable(
        const TYPath& path,
        const TVector<TKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    ///
    /// @brief Reshard dynamic table, breaking it into given number of tablets.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#reshard_table)
    virtual void ReshardTable(
        const TYPath& path,
        i64 tabletCount,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    ///
    /// @brief Insert rows into dynamic table.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#insert_rows)
    virtual void InsertRows(
        const TYPath& path,
        const TNode::TListType& rows,
        const TInsertRowsOptions& options = TInsertRowsOptions()) = 0;

    ///
    /// @brief Delete rows from dynamic table.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#delete_rows)
    virtual void DeleteRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TDeleteRowsOptions& options = TDeleteRowsOptions()) = 0;

    ///
    /// @brief Trim rows from the beginning of ordered dynamic table.
    ///
    /// Asynchronously removes `rowCount` rows from the beginning of ordered dynamic table.
    /// Numeration of remaining rows *does not change*, e.g. after `trim(10)` and `trim(20)`
    /// you get in total `20` deleted rows.
    ///
    /// @param path Path to ordered dynamic table.
    /// @param tabletIndex Which tablet to trim.
    /// @param rowCount How many trimmed rows will be in the table after command.
    /// @param options Optional parameters.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#trim_rows)
    virtual void TrimRows(
        const TYPath& path,
        i64 tabletIndex,
        i64 rowCount,
        const TTrimRowsOptions& options = TTrimRowsOptions()) = 0;

    ///
    /// @brief Lookup rows with given keys from dynamic table.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#lookup-rows)
    virtual TNode::TListType LookupRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    ///
    /// @brief Select rows from dynamic table, using [SQL dialect](https://ytsaurus.tech/docs/en//description/dynamic_tables/dyn_query_language.html).
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#select-rows)
    virtual TNode::TListType SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    ///
    /// @brief Change properties of table replica.
    ///
    /// Allows to enable/disable replica and/or change its mode.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#alter_table_replica)
    virtual void AlterTableReplica(
        const TReplicaId& replicaId,
        const TAlterTableReplicaOptions& alterTableReplicaOptions) = 0;

    ///
    /// @brief Generate a monotonously increasing master timestamp.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#generate_timestamp)
    virtual ui64 GenerateTimestamp() = 0;

    /// Return YT username of current client.
    virtual TAuthorizationInfo WhoAmI() = 0;

    ///
    /// @brief Get operation attributes.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_operation)
    virtual TOperationAttributes GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = TGetOperationOptions()) = 0;

    ///
    /// @brief Get operation attributes.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_operation)
    virtual TOperationAttributes GetOperation(
        const TString& alias,
        const TGetOperationOptions& options = TGetOperationOptions()) = 0;

    ///
    /// @brief List operations satisfying given filters.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#list_operations)
    virtual TListOperationsResult ListOperations(
        const TListOperationsOptions& options = TListOperationsOptions()) = 0;

    ///
    /// @brief Update operation runtime parameters.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#update_operation_parameters)
    virtual void UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options) = 0;

    ///
    /// @brief Get job attributes.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_job)
    virtual TJobAttributes GetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options = TGetJobOptions()) = 0;

    ///
    /// List attributes of jobs satisfying given filters.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#list_jobs)
    virtual TListJobsResult ListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options = TListJobsOptions()) = 0;

    ///
    /// @brief Get the input of a running or failed job.
    ///
    /// @ref NYT::TErrorResponse exception is thrown if job is missing.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_job_input)
    virtual IFileReaderPtr GetJobInput(
        const TJobId& jobId,
        const TGetJobInputOptions& options = TGetJobInputOptions()) = 0;

    ///
    /// @brief Get fail context of a failed job.
    ///
    /// @ref NYT::TErrorResponse exception is thrown if it is missing.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_job_fail_context)
    virtual IFileReaderPtr GetJobFailContext(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& options = TGetJobFailContextOptions()) = 0;

    ///
    /// @brief Get stderr of a running or failed job.
    ///
    /// @ref NYT::TErrorResponse exception is thrown if it is missing.
    ///
    /// @note YT doesn't store all job stderrs
    ///
    /// @note If job stderr exceeds few megabytes YT will store only head and tail of stderr.
    ///
    /// @see Description of `max_stderr_size` spec option [here](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options).
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_job_stderr)
    virtual IFileReaderPtr GetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = TGetJobStderrOptions()) = 0;

    ///
    /// @brief Create one or several rbtorrents for files in a blob table.
    ///
    /// If specified, one torrent is created for each value of `KeyColumns` option.
    /// Otherwise, a single torrent with all files of a table is created.
    ///
    /// @return list of nodes, each node has two fields
    ///  * `key`: list of key columns values. Empty if `KeyColumns` is not specified.
    ///  * `rbtorrent`: rbtorrent string (with `rbtorrent:` prefix)
    ///
    /// @see [More info.](https://docs.yandex-team.ru/docs/yt/description/storage/blobtables#sky_share)
    virtual TNode::TListType SkyShareTable(
        const std::vector<TYPath>& tablePaths,
        const TSkyShareTableOptions& options) = 0;

    ///
    /// @brief Check if `user` has `permission` to access a Cypress node at `path`.
    ///
    /// For tables access to columns specified in `options.Columns_` can be checked
    /// (@see [the doc](https://ytsaurus.tech/docs/en/user-guide/storage/columnar-acl)).
    ///
    /// If access is denied (the returned result has `.Action == ESecurityAction::Deny`)
    /// because of a `deny` rule, the "denying" object name and id
    /// and "denied" subject name an id may be returned.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#check_permission)
    virtual TCheckPermissionResponse CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options = TCheckPermissionOptions()) = 0;

    /// @brief Get information about tablet
    /// @see NYT::TTabletInfo
    virtual TVector<TTabletInfo> GetTabletInfos(
        const TYPath& path,
        const TVector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options = TGetTabletInfosOptions()) = 0;

    ///
    /// @brief Suspend operation.
    ///
    /// Jobs will be aborted.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#suspend_operation)
    virtual void SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = TSuspendOperationOptions()) = 0;

    /// @brief Resume previously suspended operation.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#resume_operation)
    virtual void ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options = TResumeOperationOptions()) = 0;

    ///
    /// @brief Synchronously terminates all client's background activities
    ///
    /// e.g. no callbacks will be executed after the function is completed
    ///
    /// @note It is safe to call Shutdown multiple times
    ///
    /// @note @ref NYT::TApiUsageError will be thrown if any client's method is called after shutdown
    ///
    virtual void Shutdown() = 0;
};


/// Create a client for particular MapReduce cluster.
IClientPtr CreateClient(
    const TString& serverName,
    const TCreateClientOptions& options = TCreateClientOptions());


/// Create a client for mapreduce cluster specified in `YT_PROXY` environment variable.
IClientPtr CreateClientFromEnv(
    const TCreateClientOptions& options = TCreateClientOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
