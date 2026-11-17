#include "checksum_source.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

namespace NKikimr::NReplication::NController {
namespace {

struct TEvPart: TEventLocal<TEvPart, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 102> {
    TChecksumStreamPart Part;
    explicit TEvPart(TChecksumStreamPart part) : Part(std::move(part)) {}
};

class TChecksumSource: public TActorBootstrapped<TChecksumSource> {
public:
    TChecksumSource(TActorId parent, TActorId proxy, Ydb::Replication::StreamChecksumTablesInternalRequest request, TInstant deadline)
        : Parent(parent), Proxy(proxy), Request(std::move(request)), Deadline(deadline) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        const auto timeout = Deadline > TActivationContext::Now() ? Deadline - TActivationContext::Now() : TDuration::MicroSeconds(1);
        Send(Proxy, new TEvYdbProxy::TEvChecksumStreamRequest(std::move(Request), timeout));
    }

private:
    void Handle(TEvYdbProxy::TEvChecksumStreamResponse::TPtr& ev) {
        Stream.emplace(std::move(ev->Get()->Result));
        if (!Stream->IsSuccess()) {
            return Fail(*Stream);
        }
        Read();
    }

    void Read() {
        const auto self = SelfId();
        auto* system = TActivationContext::ActorSystem();
        Stream->ReadNext().Subscribe([self, system](NThreading::TFuture<TChecksumStreamPart> result) {
            system->Send(self, new TEvPart(result.ExtractValue()));
        });
    }

    void Fail(const NYdb::TStatus& status) {
        auto result = MakeHolder<NChecksum::TEvResult>();
        result->Record.set_status(status.IsTransportError() ? Ydb::StatusIds::UNAVAILABLE
            : static_cast<Ydb::StatusIds::StatusCode>(status.GetStatus()));
        NYdb::NIssue::IssuesToMessage(status.GetIssues(), result->Record.mutable_issues());
        if (result->Record.issues().empty()) {
            result->Record.add_issues()->set_message("Source checksum stream ended before completion");
        }
        Send(Parent, result.Release());
        PassAway();
    }

    void Handle(TEvPart::TPtr& ev) {
        auto& part = ev->Get()->Part;
        if (!part.IsSuccess()) {
            return Fail(part);
        }
        auto result = MakeHolder<NChecksum::TEvResult>();
        result->Record = std::move(part.Record);
        const bool complete = result->Record.complete();
        Send(Parent, result.Release());
        if (complete) {
            PassAway();
        } else {
            Read();
        }
    }

    void PassAway() override {
        if (Stream) {
            Stream->Cancel();
        }
        TActorBootstrapped::PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvYdbProxy::TEvChecksumStreamResponse, Handle)
        hFunc(TEvPart, Handle)
        sFunc(TEvents::TEvPoison, PassAway)
    )

    const TActorId Parent;
    const TActorId Proxy;
    Ydb::Replication::StreamChecksumTablesInternalRequest Request;
    const TInstant Deadline;
    std::optional<TChecksumTableIterator> Stream;
};

}

IActor* CreateChecksumSource(const TActorId& parent, const TActorId& proxy,
    Ydb::Replication::StreamChecksumTablesInternalRequest request, TInstant deadline)
{
    return new TChecksumSource(parent, proxy, std::move(request), deadline);
}

}
