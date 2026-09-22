#include "nbs2_maintenance.h"
#include "cms_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_events_private.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/builder.h>

namespace NKikimr::NCms {

namespace {

namespace NDbsc = NYdb::NBS::NBlockStore::NStorage::NDbsController;
using TDbscEvents = NDbsc::TEvDbsControllerPrivate;
using TStatus = NKikimrCms::TStatus;

class TNbs2MaintenanceChecker final : public TActorBootstrapped<TNbs2MaintenanceChecker> {
public:
    TNbs2MaintenanceChecker(const TActorId& client, ui64 attemptId,
            TVector<ui32> nodeIds, TDuration timeout)
        : Client(client)
        , AttemptId(attemptId)
        , NodeIds(std::move(nodeIds))
        , Timeout(timeout)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SERVICE;
    }

    void Bootstrap() {
        if (Timeout == TDuration::Zero() || Timeout == TDuration::Max()) {
            ReplyAndDie(TStatus::ERROR_TEMP, "Invalid DBSController request timeout");
            return;
        }

        Become(&TThis::StateWork);
        Schedule(Timeout, new TEvents::TEvWakeup());

        // No retries within an attempt: CMS may retry the complete check later.
        Pipe = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), MakeDbsControllerID()));
        auto request = MakeHolder<TDbscEvents::TEvNodeMaintenancePermissionRequest>();
        for (const ui32 nodeId : NodeIds) {
            request->Record.AddNodeIds(nodeId);
        }
        NTabletPipe::SendData(SelfId(), Pipe, request.Release(), AttemptId);
    }

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TDbscEvents::TEvNodeMaintenancePermissionResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvWakeup, HandleTimeout);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    void Handle(TDbscEvents::TEvNodeMaintenancePermissionResponse::TPtr& ev) {
        if (ev->Cookie != AttemptId) {
            return;
        }

        const auto& record = ev->Get()->Record;
        // A successful proto3 response may omit both Error and Decision (ALLOW).
        if (NYdb::NBS::HasError(record.GetError())) {
            ReplyAndDie(TStatus::ERROR_TEMP,
                TStringBuilder() << "DBSController maintenance check failed: "
                    << NYdb::NBS::FormatError(record.GetError()));
            return;
        }

        switch (record.GetDecision()) {
        case NDbsc::NProto::ALLOW:
            ReplyAndDie(TStatus::ALLOW);
            return;
        case NDbsc::NProto::DENY:
            ReplyAndDie(TStatus::DISALLOW_TEMP, "DBSController denied node maintenance",
                TVector<ui64>(record.GetBlockingPartitionIds().begin(), record.GetBlockingPartitionIds().end()));
            return;
        default:
            ReplyAndDie(TStatus::ERROR_TEMP,
                TStringBuilder() << "Unknown DBSController maintenance decision: " << static_cast<int>(record.GetDecision()));
            return;
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto& msg = *ev->Get();
        if (msg.ClientId == Pipe && msg.Status != NKikimrProto::OK) {
            ReplyAndDie(TStatus::ERROR_TEMP,
                TStringBuilder() << "Cannot connect to DBSController: " << msg.Status);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == Pipe) {
            ReplyAndDie(TStatus::ERROR_TEMP, "DBSController connection lost");
        }
    }

    void HandleTimeout() {
        ReplyAndDie(TStatus::ERROR_TEMP, "DBSController maintenance check timed out");
    }

    void ReplyAndDie(TStatus::ECode status, TString reason = {}, TVector<ui64> blockingPartitionIds = {}) {
        auto result = MakeHolder<TCms::TEvPrivate::TEvNbs2MaintenanceResult>();
        result->AttemptId = AttemptId;
        result->Status = status;
        result->Reason = std::move(reason);
        result->BlockingPartitionIds = std::move(blockingPartitionIds);
        Send(Client, std::move(result));
        PassAway();
    }

    void PassAway() override {
        if (Pipe) {
            NTabletPipe::CloseAndForgetClient(SelfId(), Pipe);
        }
        TActorBootstrapped::PassAway();
    }

private:
    const TActorId Client;
    const ui64 AttemptId;
    const TVector<ui32> NodeIds;
    const TDuration Timeout;
    TActorId Pipe;
};

} // anonymous namespace

IActor* CreateNbs2MaintenanceChecker(const TActorId& client, ui64 attemptId,
        TVector<ui32> nodeIds, TDuration timeout)
{
    return new TNbs2MaintenanceChecker(client, attemptId, std::move(nodeIds), timeout);
}

} // namespace NKikimr::NCms
