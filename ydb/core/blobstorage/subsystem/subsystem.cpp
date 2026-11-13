#include "subsystem.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace {

class TBlobStorageSubsystem final : public IBlobStorageSubsystem {
    const TIntrusivePtr<TNodeWardenConfig> Config;
    const ui32 PoolId;
    const NActors::TMailboxType::EType MailboxType;
    bool Prepared = false;

public:
    TBlobStorageSubsystem(TIntrusivePtr<TNodeWardenConfig> config, ui32 poolId, NActors::TMailboxType::EType mailboxType)
        : Config(std::move(config))
        , PoolId(poolId)
        , MailboxType(mailboxType)
    {
        Y_ABORT_UNLESS(Config);
    }

    void Prepare(NActors::TActorSystemSetup& setup) override {
        Y_ABORT_UNLESS(!Prepared);
        const auto serviceId = MakeBlobStorageNodeWardenID(setup.NodeId);
        for (const auto& service : setup.LocalServices) {
            Y_ABORT_UNLESS(service.first != serviceId, "NodeWarden is already registered");
        }
        setup.LocalServices.emplace_back(serviceId, NActors::TActorSetupCmd(
            CreateBSNodeWarden(Config), MailboxType, PoolId));
        Prepared = true;
    }
};

} // namespace

std::unique_ptr<IBlobStorageSubsystem> CreateBlobStorageSubsystem(
        TIntrusivePtr<TNodeWardenConfig> config, ui32 poolId, NActors::TMailboxType::EType mailboxType) {
    return std::make_unique<TBlobStorageSubsystem>(std::move(config), poolId, mailboxType);
}

} // namespace NKikimr
