#include "mock.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace {

class TMockBlobStorageSubsystem final : public IBlobStorageSubsystem {
    const TVector<TIntrusivePtr<NFake::TProxyDS>> Groups;
    const ui32 PoolId;
    bool Prepared = false;

public:
    TMockBlobStorageSubsystem(TVector<TIntrusivePtr<NFake::TProxyDS>> groups, ui32 poolId)
        : Groups(std::move(groups))
        , PoolId(poolId)
    {}

    void Prepare(NActors::TActorSystemSetup& setup) override {
        Y_ABORT_UNLESS(!Prepared);
        THashSet<NActors::TActorId> serviceIds;
        for (const auto& service : setup.LocalServices) {
            serviceIds.insert(service.first);
        }
        // Validate all groups before adding any actors.
        for (const auto& group : Groups) {
            Y_ABORT_UNLESS(group);
            Y_ABORT_UNLESS(serviceIds.insert(MakeBlobStorageProxyID(group->GetGroupId())).second,
                "BlobStorage group proxy is already registered");
        }
        for (const auto& group : Groups) {
            setup.LocalServices.emplace_back(MakeBlobStorageProxyID(group->GetGroupId()),
                NActors::TActorSetupCmd(CreateBlobStorageGroupProxyMockActor(group),
                    NActors::TMailboxType::ReadAsFilled, PoolId));
        }
        Prepared = true;
    }
};

} // namespace

std::unique_ptr<IBlobStorageSubsystem> CreateMockBlobStorageSubsystem(
        TVector<TIntrusivePtr<NFake::TProxyDS>> groups, ui32 poolId) {
    return std::make_unique<TMockBlobStorageSubsystem>(std::move(groups), poolId);
}

} // namespace NKikimr
