#include "base/base.h"
#include "service_replication.h"

#include <ydb/core/tx/replication/checksum/checksum.h>
#include <google/protobuf/util/time_util.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {
namespace {

using TStreamRequest = TGrpcRequestNoOperationCall<Ydb::Replication::StreamChecksumTablesInternalRequest,
    Ydb::Replication::StreamChecksumTablesInternalResponse>;

bool ParseTimeout(const google::protobuf::Duration& duration, TDuration& result) {
    using TTime = google::protobuf::util::TimeUtil;
    if (!TTime::IsDurationValid(duration) || duration.seconds() < 0 || duration.nanos() < 0
        || (!duration.seconds() && !duration.nanos())
        || duration.seconds() >= Max<i64>() / 1000000)
    {
        return false;
    }
    result = TDuration::MicroSeconds(TTime::DurationToMicroseconds(duration));
    return result != TDuration::Zero();
}

class TStreamChecksumRPC: public TActorBootstrapped<TStreamChecksumRPC> {
public:
    explicit TStreamChecksumRPC(IRequestNoOpCtx* request)
        : Request(static_cast<TStreamRequest*>(request)) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        auto* system = TActivationContext::ActorSystem();
        const auto self = SelfId();
        Request->SetFinishAction([system, self] { system->Send(self, new TEvents::TEvPoison()); });
        auto* proto = Request->GetProtoRequest();
        TDuration timeout;
        if (!ParseTimeout(proto->timeout(), timeout)) {
            Ydb::Replication::StreamChecksumTablesInternalResponse response;
            response.set_status(Ydb::StatusIds::BAD_REQUEST);
            response.add_issues()->set_message("Checksum timeout must be positive");
            Request->SendSerializedResult(response.SerializeAsString(), response.status());
            Request->FinishStream(response.status());
            return PassAway();
        }
        NReplication::NChecksum::TSettings settings;
        settings.Database = Request->GetDatabaseName().GetOrElse("");
        // Presence is intentional even for unauthenticated requests: never
        // substitute a system identity for an external source request.
        settings.UserToken = Request->GetSerializedToken();
        settings.Request = *proto;
        settings.Deadline = TActivationContext::Now() + timeout;
        Worker = Register(NReplication::NChecksum::CreateChecksumTables(SelfId(), std::move(settings)));
    }

private:
    void Handle(NReplication::NChecksum::TEvResult::TPtr& ev) {
        const auto& result = ev->Get()->Record;
        Request->SendSerializedResult(result.SerializeAsString(), result.status());
        if (result.status() != Ydb::StatusIds::SUCCESS || result.complete()) {
            Worker = {};
            Request->FinishStream(result.status());
            PassAway();
        }
    }

    void PassAway() override {
        if (Worker) {
            Send(Worker, new TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(NReplication::NChecksum::TEvResult, Handle)
        sFunc(TEvents::TEvPoison, PassAway)
    )

    std::unique_ptr<TStreamRequest> Request;
    TActorId Worker;
};

} // namespace

void DoStreamChecksumTablesInternal(std::unique_ptr<IRequestNoOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TStreamChecksumRPC(request.release()));
}

} // namespace NKikimr::NGRpcService
