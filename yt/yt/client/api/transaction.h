#pragma once

#include "client.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionCommitOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    //! If not null, then this particular cell will be the coordinator.
    NObjectClient::TCellId CoordinatorCellId;

    //! If |true| then two-phase-commit protocol is executed regardless of the number of participants.
    bool Force2PC = false;

    //! Eager: coordinator is committed first, success is reported immediately; the participants are committed afterwards.
    //! Lazy: all the participants must successfully commit before coordinator commits; only after this success is reported.
    ETransactionCoordinatorCommitMode CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Eager;

    //! Early: coordinator is prepared first and committed first.
    //! Late: coordinator is prepared last and after commit timestamp generation; prepare and commit
    //! are executed in single mutation.
    ETransactionCoordinatorPrepareMode CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Early;

    //! At non-coordinating participants, Transaction Manager will synchronize with
    //! these cells before running prepare.
    std::vector<NObjectClient::TCellId> CellIdsToSyncWithBeforePrepare;

    //! If |true| then all participants will use the commit timestamp provided by the coordinator.
    //! If |false| then the participants will use individual commit timestamps based on their cell tag.
    bool InheritCommitTimestamp = true;

    //! If |true| then the coordinator will generate a non-null prepare timestamp (which is a lower bound for
    //! the upcoming commit timestamp) and send it to all the participants.
    //! If |false| then no prepare timestamp is generated and null value is provided to the participants.
    //! The latter is useful for async replication that does not involve any local write operations
    //! and also relies on ETransactionCoordinatorCommitMode::Lazy transactions whose commit may be delayed
    //! for an arbitrary period of time in case of replica failure.
    bool GeneratePrepareTimestamp = true;

    //! If non-null then the coordinator will fail the commit if generated commit timestamp
    //! exceeds |MaxAllowedCommitTimestamp|.
    NTransactionClient::TTimestamp MaxAllowedCommitTimestamp = NTransactionClient::NullTimestamp;

    //! Cell ids of additional 2PC participants.
    //! Used to implement cross-cluster commit via RPC proxy.
    std::vector<NObjectClient::TCellId> AdditionalParticipantCellIds;
};

struct TTransactionPingOptions
{
    bool EnableRetries = false;
};

struct TTransactionCommitResult
{
    //! NullTimestamp for all cases when CommitTimestamps are empty.
    //! NullTimestamp when the primary cell did not participate in to transaction.
    NHiveClient::TTimestamp PrimaryCommitTimestamp = NHiveClient::NullTimestamp;
    //! Empty for non-atomic transactions (timestamps are fake).
    //! Empty for empty tablet transactions (since the commit is essentially no-op).
    //! May contain multiple items for cross-cluster commit.
    NHiveClient::TTimestampMap CommitTimestamps;
};

struct TTransactionAbortOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    bool Force = false;
};

struct TTransactionFlushResult
{
    std::vector<NElection::TCellId> ParticipantCellIds;
};

//! Either a write or delete.
struct TRowModification
{
    //! Discriminates between writes and deletes.
    ERowModificationType Type;
    //! Either a row (for write; versioned or unversioned) or a key (for delete; always unversioned).
    NTableClient::TTypeErasedRow Row;
    //! Locks.
    NTableClient::TLockMask Locks;
};

struct TModifyRowsOptions
{
    //! If this happens to be a modification of a replicated table,
    //! controls if at least one sync replica is required.
    bool RequireSyncReplica = true;

    //! For chaos replicated tables indicates if it is necessary to explore other replicas.
    bool TopmostTransaction = true;

    //! For chaos replicas pass replication card to ensure that all data is sent using same meta info.
    NChaosClient::TReplicationCardPtr ReplicationCard;

    //! For writes to replicas, this is the id of the replica at the upstream cluster.
    NTabletClient::TTableReplicaId UpstreamReplicaId;

    //! Modifications are sent asynchronously. Sequential numbering is
    //! required to restore their order.
    //!
    //! This parameter is only used by native client (in particular within RPC proxy server).
    std::optional<i64> SequenceNumber;

    //! Modifications can be sent from several sources in case of several clients
    //! attached to the same transaction.
    //!
    //! Modifications within one source will be serialized by this source sequence numbers.
    //! Modifications from different sources will be serialized arbitrarily, that is why
    //! different sources must send independent modifications.
    //!
    //! If sequence number is missing, source id is ignored.
    //!
    //! This parameter is only used by native client (in particular within RPC proxy server).
    i64 SequenceNumberSourceId = 0;

    //! If set treat missing key columns as null.
    bool AllowMissingKeyColumns = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a client-controlled transaction.
/*
 *  Transactions are created by calling IClientBase::Transaction.
 *
 *  For some table operations (e.g. #WriteRows), the transaction instance
 *  buffers all modifications and flushes them during #Commit. This, in
 *  particular, explains why these methods return |void|.
 *
 *  Thread affinity: any
 */
struct ITransaction
    : public virtual IClientBase
{
    virtual IClientPtr GetClient() const = 0;
    virtual NTransactionClient::ETransactionType GetType() const = 0;
    virtual NTransactionClient::TTransactionId GetId() const = 0;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const = 0;
    virtual NTransactionClient::EAtomicity GetAtomicity() const = 0;
    virtual NTransactionClient::EDurability GetDurability() const = 0;
    virtual TDuration GetTimeout() const = 0;

    virtual TFuture<void> Ping(const NApi::TTransactionPingOptions& options = {}) = 0;
    virtual TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options = {}) = 0;
    virtual TFuture<void> Abort(const TTransactionAbortOptions& options = {}) = 0;
    virtual void Detach() = 0;
    virtual TFuture<TTransactionFlushResult> Flush() = 0;
    virtual void RegisterAlienTransaction(const ITransactionPtr& transaction) = 0;

    using TCommittedHandlerSignature = void();
    using TCommittedHandler = TCallback<TCommittedHandlerSignature>;
    DECLARE_INTERFACE_SIGNAL(TCommittedHandlerSignature, Committed);

    using TAbortedHandlerSignature = void(const TError& error);
    using TAbortedHandler = TCallback<TAbortedHandlerSignature>;
    DECLARE_INTERFACE_SIGNAL(TAbortedHandlerSignature, Aborted);

    // Verified dynamic casts to a more specific interface.

    template <class TDerivedTransaction>
    TDerivedTransaction* As();
    template <class TDerivedTransaction>
    TDerivedTransaction* TryAs();

    template <class TDerivedTransaction>
    const TDerivedTransaction* As() const;
    template <class TDerivedTransaction>
    const TDerivedTransaction* TryAs() const;

    // Tables.

    void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TModifyRowsOptions& options = {});

    void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const TModifyRowsOptions& options = {});

    void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const TModifyRowsOptions& options = {});

    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::TLockMask lockMask);

    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong);

    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const std::vector<TString>& locks,
        NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong);

    virtual void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options = TModifyRowsOptions()) = 0;

    // Consumers.

    //! Advance the consumer's offset for the given partition, setting it to a new value.
    //!
    //! If oldOffset is specified, the current offset is read inside this transaction and compared with oldOffset.
    //! If they are equal, the new offset is written, otherwise an exception is thrown.
    void AdvanceConsumer(
        const NYPath::TYPath& path,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset);

    void AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset);
};

DEFINE_REFCOUNTED_TYPE(ITransaction)

////////////////////////////////////////////////////////////////////////////////

//! A subset of #TTransactionStartOptions tailored for the case of alien
//! transactions.
struct TAlienTransactionStartOptions
{
    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::EDurability Durability = NTransactionClient::EDurability::Sync;

    NTransactionClient::TTimestamp StartTimestamp = NTransactionClient::NullTimestamp;
};

//! A helper for starting an alien transaction at #alienClient.
/*!
 *  Internally, invokes #IClient::StartTransaction and #ITransaction::RegisterAlienTransaction
 *  but also takes care of the following issues:
 *  1) Alien and local transaction ids must be same;
 *  2) If #alienClient and #localTransaction have matching clusters then no
 *     new transaction must be created.
 */
TFuture<ITransactionPtr> StartAlienTransaction(
    const ITransactionPtr& localTransaction,
    const IClientPtr& alienClient,
    const TAlienTransactionStartOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

#define TRANSACTION_INL_H_
#include "transaction-inl.h"
#undef TRANSACTION_INL_H_
