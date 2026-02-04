#include "rpc_deferrable.h"
#include <ydb/public/api/protos/draft/ydb_nbs.pb.h>

#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct.h>
#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>

namespace NKikimr::NGRpcService {

using TEvCreatePartitionRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::CreatePartitionRequest,
        Ydb::Nbs::CreatePartitionResponse>;
using TEvDeletePartitionRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::DeletePartitionRequest,
        Ydb::Nbs::DeletePartitionResponse>;
using TEvListPartitionsRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::ListPartitionsRequest,
        Ydb::Nbs::ListPartitionsResponse>;

using namespace NActors;
using namespace Ydb;

class TCreatePartitionRequest
    : public TRpcOperationRequestActor<TCreatePartitionRequest, TEvCreatePartitionRequest> {

public:
    TCreatePartitionRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        NYdb::NBS::NProto::TStorageConfig storageConfig;
        storageConfig.SetDDiskPoolName("ddp1");
        storageConfig.SetPersistentBufferDDiskPoolName("ddp1");
        // One trace per 100ms should be sufficient for observability.
        storageConfig.SetTraceSamplePeriod(100);

        // Create partition actor
        auto partition_actor_id = NYdb::NBS::NBlockStore::NStorage::NPartitionDirect::CreatePartitionTablet(
            SelfId(),
            std::move(storageConfig));

        LOG_INFO(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: created partition actor with id: %s",
            partition_actor_id.ToString().data());

        Ydb::Nbs::CreatePartitionResult result;
        result.SetTabletId(partition_actor_id.ToString());
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ev);
        // TODO
    }
};

class TDeletePartitionRequest
    : public TRpcOperationRequestActor<TDeletePartitionRequest, TEvDeletePartitionRequest> {

public:
    TDeletePartitionRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateWork);

        auto tabletIdStr = GetProtoRequest()->GetTabletId();

        // Parse the string to create a TActorId
        NActors::TActorId tabletId;
        tabletId.Parse(tabletIdStr.data(), tabletIdStr.size());

        // Send poison pill to partition actor
        ctx.Send(tabletId, new TEvents::TEvPoisonPill);

        LOG_INFO(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: sent poison event to partition actor with id: %s",
            tabletId.ToString().data());

        Ydb::Nbs::DeletePartitionResult result;
        result.SetTabletId(tabletIdStr);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ev);
        // TODO
    }
};

class TListPartitionsRequest
    : public TRpcOperationRequestActor<TListPartitionsRequest, TEvListPartitionsRequest> {

public:
    TListPartitionsRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        // TODO: list partition actors
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ev);
        // TODO
    }
};

void DoCreatePartition(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreatePartitionRequest(p.release()));
}

void DoDeletePartition(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDeletePartitionRequest(p.release()));
}

void DoListPartitions(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListPartitionsRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
