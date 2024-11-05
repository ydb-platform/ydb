#pragma once

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionType,
    ((Master)          (0)) // Accepted by both masters and tablets
    ((Tablet)          (1)) // Accepted by tablets only
);

DEFINE_ENUM(EAtomicity,
    ((Full)            (0)) // 2PC enabled, Percolator mode :)
    ((None)            (1)) // 2PC disabled, HBase mode
);

DEFINE_ENUM(EDurability,
    ((Sync)            (0)) // Wait for Hydra commit result
    ((Async)           (1)) // Reply as soon as the request is enqueued to Hydra
);

//! Only applies to ordered tables.
DEFINE_ENUM(ECommitOrdering,
    ((Weak)            (0)) // Rows are appended to tablet in order of participant commits
    ((Strong)          (1)) // Rows are appended to tablet in order of timestamps
);

DEFINE_ENUM(ETimestampProviderFeature,
    ((AlienClocks)     (0))
);

YT_DEFINE_ERROR_ENUM(
    ((NoSuchTransaction)                (11000))
    ((NestedExternalTransactionExists)  (11001))
    ((TransactionDepthLimitReached)     (11002))
    ((InvalidTransactionState)          (11003))
    ((ParticipantFailedToPrepare)       (11004))
    ((SomeParticipantsAreDown)          (11005))
    ((AlienTransactionsForbidden)       (11006))
    ((MalformedAlienTransaction)        (11007))
    ((InvalidTransactionAtomicity)      (11008))
    ((UploadTransactionCannotHaveNested)(11009))
    ((ForeignParentTransaction)         (11010))
    ((ForeignPrerequisiteTransaction)   (11011))
    ((IncompletePrepareSignature)       (11012))
    ((TransactionSuccessorHasLeases)    (11013))
    ((UnknownClockClusterTag)           (11014))
    ((ClockClusterTagMismatch)          (11015))
);

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

//! Timestamp is a cluster-wide unique monotonically increasing number
//! used to implement the MVCC paradigm.
/*!
 *  Timestamp is a 64-bit unsigned integer of the following structure:
 *  bits  0-29:  auto-incrementing counter (allowing up to ~10^9 timestamps per second)
 *  bits 30-61:  Unix time in seconds (from 1 Jan 1970)
 *  bits 62-63:  reserved
 */
using TTimestamp = ui64;

//! Number of bits in the counter part.
constexpr int TimestampCounterWidth = 30;

// Timestamp values range:
//! Minimum valid (non-sentinel) timestamp.
constexpr TTimestamp MinTimestamp                 = 0x0000000000000001ULL;
//! Maximum valid (non-sentinel) timestamp.
constexpr TTimestamp MaxTimestamp                 = 0x3fffffffffffff00ULL;

// User sentinels:
//! Uninitialized/invalid timestamp.
constexpr TTimestamp NullTimestamp                = 0x0000000000000000ULL;
//! Truly (serializable) latest committed version.
//! May cause row blocking if concurrent writes are in progress.
constexpr TTimestamp SyncLastCommittedTimestamp   = 0x3fffffffffffff01ULL;
//! Relaxed (non-serializable) latest committed version.
//! Never leads to row blocking but may miss some concurrent writes.
constexpr TTimestamp AsyncLastCommittedTimestamp  = 0x3fffffffffffff04ULL;
//! Used to fetch all committed values during e.g. flushes or compactions.
//! Returns all versions that were committed at the moment the reader was created.
//! Never leads to row blocking but may miss some concurrent writes.
constexpr TTimestamp AllCommittedTimestamp        = 0x3fffffffffffff03ULL;

// System sentinels:
//! Used by TSortedDynamicStore to mark values being written by transactions.
constexpr TTimestamp UncommittedTimestamp         = 0x3fffffffffffff02ULL;
//! Used by TSortedDynamicStore in TLockDescriptor::PrepareTimestamp.
//! Must be larger than SyncLastCommittedTimestamp.
constexpr TTimestamp NotPreparedTimestamp         = 0x3fffffffffffffffULL;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITimestampProvider)
DECLARE_REFCOUNTED_CLASS(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
