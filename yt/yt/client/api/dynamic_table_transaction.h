#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/memory/shared_range.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

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

    //! If set then WriteViaQueueProducer table schema will be used instead of Write table schema.
    bool WriteViaQueueProducer = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IDynamicTableTransaction
{
    virtual ~IDynamicTableTransaction() = default;

    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TModifyRowsOptions& options = {},
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive) = 0;
    virtual void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const TModifyRowsOptions& options = {}) = 0;

    virtual void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const TModifyRowsOptions& options = {}) = 0;

    virtual void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::TLockMask lockMask) = 0;
    virtual void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong) = 0;
    virtual void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const std::vector<TString>& locks,
        NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong) = 0;

    virtual void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
