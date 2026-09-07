#include "dbg_connections.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 InitialDDiskSessionSeqNo = 0;

NProto::TError MakeSessionResetError()
{
    return MakeError(E_REJECTED, "DDisk session reset");
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDDiskConnection::ResetSession()
{
    if (!ConnectPromise.HasValue()) {
        ConnectPromise.SetValue(MakeSessionResetError());
    }

    ConnectPromise = NThreading::NewPromise<NProto::TError>();
    ConnectFuture = ConnectPromise.GetFuture();
    SessionState = EDDiskSessionState::NotLocked;
}

const TFuture<NProto::TError>& TDDiskConnection::GetFuture() const
{
    return ConnectFuture;
}

TString TDDiskConnection::DebugPrint() const
{
    TStringBuilder result;
    result << HostConnection.DebugPrint();
    auto f = GetFuture();
    if (f.IsReady()) {
        result << " c:" << FormatError(f.GetValue());
    } else {
        result << "c:<none>";
    }
    result << " s:" << ToString(SessionState);
    result << " csn:" << ConfirmedSessionSeqNo;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TDBGConnections::TDBGConnections(
    ui64 tabletId,
    ui32 tabletGeneration,
    ui32 directBlockGroupIndex,
    ui32 dbgConnectionsConfigGeneration)
    : TabletId(tabletId)
    , TabletGeneration(tabletGeneration)
    , DirectBlockGroupIndex(directBlockGroupIndex)
    , DBGConnectionsConfigGeneration(dbgConnectionsConfigGeneration)
{}

void TDBGConnections::AddSlot(
    THostIndex host,
    const NBsController::TDDiskId& ddiskId,
    const NBsController::TDDiskId& pbufferId,
    ui32 dbgConnectionsConfigGeneration)
{
    Y_ABORT_UNLESS(
        static_cast<size_t>(host) == DDisks.size(),
        "a slot is appended at the end (host %lu vs slot count %lu)",
        static_cast<size_t>(host),
        DDisks.size());

    DBGConnectionsConfigGeneration = dbgConnectionsConfigGeneration;

    DDisks.push_back(TDDiskConnection{
        .HostConnection = NTransport::THostConnection{
            .ConnectionType = EConnectionType::DDisk,
            .DDiskId = ddiskId,
            .Credentials = NDDisk::TQueryCredentials::ToDDisk(
                TabletId,
                TabletGeneration,
                InitialDDiskSessionSeqNo,
                std::nullopt,
                DirectBlockGroupIndex)}});

    PBuffers.push_back(TDDiskConnection{
        .HostConnection = NTransport::THostConnection{
            .ConnectionType = EConnectionType::PBuffer,
            .DDiskId = pbufferId,
            .Credentials = NDDisk::TQueryCredentials::ToPersistentBuffer(
                TabletId,
                TabletGeneration,
                std::nullopt,
                DirectBlockGroupIndex)}});

    NKikimrBlobStorage::NDDisk::TDDiskId id;
    pbufferId.Serialize(&id);
    const auto [_, inserted] = PBufferIdToHostIndex.insert({id, host});
    Y_ABORT_UNLESS(inserted);
}

ui32 TDBGConnections::GetGeneration() const
{
    return DBGConnectionsConfigGeneration;
}

size_t TDBGConnections::GetSlotCount() const
{
    Y_ABORT_UNLESS(DDisks.size() == PBuffers.size());
    return DDisks.size();
}

const TVector<TDDiskConnection>& TDBGConnections::GetDDisks() const
{
    return DDisks;
}

const TVector<TDDiskConnection>& TDBGConnections::GetPBuffers() const
{
    return PBuffers;
}

TDDiskConnection& TDBGConnections::GetDDisk(THostIndex host)
{
    Y_ABORT_UNLESS(host < GetSlotCount());
    return DDisks[host];
}

const TDDiskConnection& TDBGConnections::GetDDisk(THostIndex host) const
{
    Y_ABORT_UNLESS(host < GetSlotCount());
    return DDisks[host];
}

TDDiskConnection& TDBGConnections::GetPBuffer(THostIndex host)
{
    Y_ABORT_UNLESS(host < GetSlotCount());
    return PBuffers[host];
}

const TDDiskConnection& TDBGConnections::GetPBuffer(THostIndex host) const
{
    Y_ABORT_UNLESS(host < GetSlotCount());
    return PBuffers[host];
}

TDDiskConnection& TDBGConnections::Get(
    EConnectionType connectionType,
    THostIndex host)
{
    return connectionType == EConnectionType::DDisk ? GetDDisk(host)
                                                    : GetPBuffer(host);
}

const TDDiskConnection& TDBGConnections::Get(
    EConnectionType connectionType,
    THostIndex host) const
{
    return connectionType == EConnectionType::DDisk ? GetDDisk(host)
                                                    : GetPBuffer(host);
}

std::optional<THostIndex> TDBGConnections::FindByPBufferId(
    const NKikimrBlobStorage::NDDisk::TDDiskId& pbufferId) const
{
    const THostIndex* const host = PBufferIdToHostIndex.FindPtr(pbufferId);
    if (!host) {
        return std::nullopt;
    }
    return *host;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
