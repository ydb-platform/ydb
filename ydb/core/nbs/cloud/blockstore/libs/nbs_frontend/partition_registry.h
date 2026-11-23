#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/system/mutex.h>

namespace NYdb::NBS::NBlockStore {

// Keeps control and I/O bound to the same partition incarnation.
struct TPartitionRegistration
{
    const NStorage::NPartitionDirect::TPartitionSessionPtr Session;
    const NStorage::NPartitionDirect::IPartitionSessionControlPtr Control;
};

using TPartitionRegistrationPtr = std::shared_ptr<const TPartitionRegistration>;

// Host-local DiskId registry. Readers retain immutable registrations while
// writers publish replacements without invalidating already admitted requests.
class TPartitionRegistry final
{
public:
    TPartitionRegistry();
    ~TPartitionRegistry();

    // Replaces an existing disk entry and returns the new incarnation's token.
    // The old partition remains responsible for stopping its own backend.
    TResultOrError<TString> Register(
        NStorage::NPartitionDirect::TPartitionSessionPtr session,
        NStorage::NPartitionDirect::IPartitionSessionControlPtr control);

    // Removes only the matching incarnation; stale tokens are harmless.
    void Unregister(const TString& diskId, const TString& registrationId);

    // Returns an owning reference that survives removal or replacement,
    // or nullptr if the disk is not registered.
    TPartitionRegistrationPtr Find(const TString& diskId) const;

private:
    struct TPartitions;

    TMutex WriterMutex;
    THotSwap<TPartitions> Partitions;
};

}   // namespace NYdb::NBS::NBlockStore
