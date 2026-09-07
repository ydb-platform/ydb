#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ddisk_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

#include <ydb/core/mind/bscontroller/types.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// State of a logical session (lock) with a DDisk.
// Sessions are used only for DDisk connections.
enum class EDDiskSessionState
{
    NotLocked,
    Locked,
    Broken,
};

////////////////////////////////////////////////////////////////////////////////

struct TDDiskConnection
{
    using TPromise = NThreading::TPromise<NProto::TError>;
    using TFuture = NThreading::TFuture<NProto::TError>;

    NTransport::THostConnection HostConnection;
    TPromise ConnectPromise = NThreading::NewPromise<NProto::TError>();
    TFuture ConnectFuture{ConnectPromise.GetFuture()};

    EDDiskSessionState SessionState = EDDiskSessionState::NotLocked;

    ui64 ConfirmedSessionSeqNo = 0;

    void ResetSession();
    [[nodiscard]] const TFuture& GetFuture() const;
    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

class TDBGConnections
{
public:
    using EConnectionType = NTransport::THostConnection::EConnectionType;

    TDBGConnections(
        ui64 tabletId,
        ui32 tabletGeneration,
        ui32 directBlockGroupIndex,
        ui32 dbgConnectionsConfigGeneration);

    void AddSlot(
        THostIndex host,
        const NKikimr::NBsController::TDDiskId& ddiskId,
        const NKikimr::NBsController::TDDiskId& pbufferId,
        ui32 dbgConnectionsConfigGeneration);

    [[nodiscard]] ui32 GetGeneration() const;
    [[nodiscard]] size_t GetSlotCount() const;

    [[nodiscard]] const TVector<TDDiskConnection>& GetDDisks() const;
    [[nodiscard]] const TVector<TDDiskConnection>& GetPBuffers() const;

    [[nodiscard]] TDDiskConnection& GetDDisk(THostIndex host);
    [[nodiscard]] const TDDiskConnection& GetDDisk(THostIndex host) const;
    [[nodiscard]] TDDiskConnection& GetPBuffer(THostIndex host);
    [[nodiscard]] const TDDiskConnection& GetPBuffer(THostIndex host) const;

    [[nodiscard]] TDDiskConnection& Get(
        EConnectionType connectionType,
        THostIndex host);
    [[nodiscard]] const TDDiskConnection& Get(
        EConnectionType connectionType,
        THostIndex host) const;

    // The slot that holds this persistent buffer, if the group has one.
    [[nodiscard]] std::optional<THostIndex> FindByPBufferId(
        const NKikimrBlobStorage::NDDisk::TDDiskId& pbufferId) const;

private:
    using TDDiskIdToHostIndex =
        TMap<NKikimrBlobStorage::NDDisk::TDDiskId, THostIndex, TDDiskIdLess>;

    const ui64 TabletId;
    const ui32 TabletGeneration;
    const ui32 DirectBlockGroupIndex;

    ui32 DBGConnectionsConfigGeneration;

    TVector<TDDiskConnection> DDisks;
    TVector<TDDiskConnection> PBuffers;
    TDDiskIdToHostIndex PBufferIdToHostIndex;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
