#pragma once

#include "storage_transport.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

// Names the IStorageTransport method a fault rule can match.
enum class EStorageOperation
{
    Connect,                   // Connect
    ReadFromPBuffer,           // ReadFromPBuffer
    ReadFromDDisk,             // ReadFromDDisk
    WriteToPBuffer,            // WriteToPBuffer
    WriteToManyPBuffers,       // WriteToManyPBuffers
    WriteToDDisk,              // WriteToDDisk
    SyncWithPBuffer,           // SyncWithPBuffer (flush)
    BatchEraseFromPBuffer,     // BatchEraseFromPBuffer
    BarrierEraseFromPBuffer,   // BarrierEraseFromPBuffer
    ListPBufferEntries,        // ListPBufferEntries
    DeleteTabletChunks,        // DeleteTabletChunks
};

// Arms a fault rule; armed rules are listed by GetFaultRules.
// ConnectionType, DDiskId and Operation are optional; unset matches anything.
// RemainingHits is how many more matching requests the rule applies to; 0
// means exhausted. Hits counts how many it has applied to. Status (default
// TReplyStatus::ERROR) and Reason are the reply to synthesize.
struct TFaultRule
{
    std::optional<THostConnection::EConnectionType> ConnectionType;
    std::optional<NKikimr::NBsController::TDDiskId> DDiskId;
    std::optional<EStorageOperation> Operation;
    ui64 RemainingHits = 0;
    ui64 Hits = 0;
    NKikimrBlobStorage::NDDisk::TReplyStatus_E Status =
        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
    TString Reason;
};

////////////////////////////////////////////////////////////////////////////////

// Controls node availability for failure simulation.
class IChaosInjectorControl
{
public:
    virtual ~IChaosInjectorControl() = default;

    // Makes subsequent requests to nodeId fail with an undelivery error.
    virtual void DisableNode(ui32 nodeId) = 0;

    // Makes subsequent requests to nodeId use the underlying transport.
    virtual void EnableNode(ui32 nodeId) = 0;

    // Returns true when requests to nodeId are configured to fail.
    [[nodiscard]] virtual bool IsNodeDisabled(ui32 nodeId) const = 0;

    // Arms a fault rule; armed rules are listed by GetFaultRules.
    // Appends the rule. Safe to call concurrently with requests.
    virtual void ArmFaultRule(TFaultRule rule) = 0;

    // Drops every armed fault rule. Safe to call concurrently with requests.
    virtual void ClearFaultRules() = 0;

    // Snapshot of armed rules, including exhausted ones (RemainingHits == 0).
    // Safe to call concurrently with requests.
    [[nodiscard]] virtual TVector<TFaultRule> GetFaultRules() const = 0;
};

// Combines storage transport operations with node-failure controls.
class ITransportWithChaosInjectorControl
    : public IStorageTransport
    , public IChaosInjectorControl
{
};

////////////////////////////////////////////////////////////////////////////////

// Wraps a storage transport with a node-failure simulation layer.
[[nodiscard]] TTransportWithChaosInjectorControlPtr
CreateTransportChaosInjector(TStorageTransportPtr underlyingTransport);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
