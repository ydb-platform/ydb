#include "partition_registry.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/generic/hash.h>

namespace NYdb::NBS::NBlockStore {

using namespace NStorage::NPartitionDirect;

// A published registry version is immutable; registrations may outlive it.
struct TPartitionRegistry::TPartitions: public TAtomicRefCount<TPartitions>
{
    THashMap<TString, TPartitionRegistrationPtr> ByDiskId;
};

TPartitionRegistry::TPartitionRegistry()
    : Partitions(MakeIntrusive<TPartitions>())
{}

TPartitionRegistry::~TPartitionRegistry() = default;

TResultOrError<TString> TPartitionRegistry::Register(
    TPartitionSessionPtr session,
    IPartitionSessionControlPtr control)
{
    if (!session || !control) {
        return MakeError(
            E_ARGUMENT,
            "Missing partition control or session target");
    }
    const auto& diskId = session->GetVolumeMetadata().GetDiskId();
    const TString registrationId = session->GetRegistrationId();
    with_lock (WriterMutex) {
        auto next = MakeIntrusive<TPartitions>(*Partitions.AtomicLoad());
        next->ByDiskId[diskId] = std::make_shared<TPartitionRegistration>(
            TPartitionRegistration{std::move(session), std::move(control)});
        Partitions.AtomicStore(next);
    }
    return registrationId;
}

void TPartitionRegistry::Unregister(
    const TString& diskId,
    const TString& registrationId)
{
    with_lock (WriterMutex) {
        const auto current = Partitions.AtomicLoad();
        const auto* partition = current->ByDiskId.FindPtr(diskId);
        if (!partition) {
            return;
        }
        const auto& session = (*partition)->Session;
        if (session->GetRegistrationId() != registrationId) {
            return;
        }
        auto next = MakeIntrusive<TPartitions>(*current);
        next->ByDiskId.erase(diskId);
        Partitions.AtomicStore(next);
    }
}

TPartitionRegistrationPtr TPartitionRegistry::Find(const TString& diskId) const
{
    const auto current = Partitions.AtomicLoad();
    if (const auto* partition = current->ByDiskId.FindPtr(diskId)) {
        return *partition;
    }
    return nullptr;
}

}   // namespace NYdb::NBS::NBlockStore
