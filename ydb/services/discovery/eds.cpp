#include "grpc_service.h"

#include <ydb/core/discovery/discovery.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_mon.h>
#include <ydb/core/grpc_services/watched_list_endpoints.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/grpc/draft/xds/codec/codec.h>
#include <ydb/public/api/grpc/draft/xds/eds.grpc.pb.h>

#include <library/cpp/digest/md5/md5.h>
#include <util/random/random.h>

namespace NKikimr::NGRpcService {
namespace {

using TService = TGrpcServiceBase<envoy::service::endpoint::v3::YdbDraft::EndpointDiscoveryService>;

class TEdsService final : public TService {
public:
    using TService::TService;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr) override;
};

using TStream = NGRpcServer::TGRpcStreamingRequest<
    Ydb::Xds::DiscoveryRequest, Ydb::Xds::DiscoveryResponse, TEdsService, NKikimrServices::GRPC_SERVER>;
using TContext = TStream::TAcceptedContext;

struct TEvSnapshot : TEventLocal<TEvSnapshot, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    Ydb::Discovery::ListEndpointsResponse Response;
    grpc::Status Status;
    TActorId Cache;
    TString Path;
    ui64 Revision = 0;
};

grpc::Status ToGrpcStatus(const Ydb::Operations::Operation& operation) {
    grpc::StatusCode code;
    switch (operation.status()) {
        case Ydb::StatusIds::SUCCESS: return grpc::Status::OK;
        case Ydb::StatusIds::BAD_REQUEST: code = grpc::StatusCode::INVALID_ARGUMENT; break;
        case Ydb::StatusIds::SCHEME_ERROR:
        case Ydb::StatusIds::NOT_FOUND: code = grpc::StatusCode::NOT_FOUND; break;
        case Ydb::StatusIds::UNAUTHORIZED: code = grpc::StatusCode::PERMISSION_DENIED; break;
        case Ydb::StatusIds::OVERLOADED: code = grpc::StatusCode::RESOURCE_EXHAUSTED; break;
        case Ydb::StatusIds::UNAVAILABLE: code = grpc::StatusCode::UNAVAILABLE; break;
        default: code = grpc::StatusCode::INTERNAL; break;
    }
    return {code, operation.issues_size() ? operation.issues(0).message() : "Endpoint discovery failed"};
}

// A request-local authentication state starts every lookup with the listener's
// policy. Only immutable peer metadata is borrowed from the live stream.
class TSnapshotContext final : public NYdbGrpc::IRequestContextBase {
public:
    TSnapshotContext(TIntrusivePtr<TContext> stream, const TString& database, const TString& endpointId,
            bool ssl, std::shared_ptr<std::atomic_bool> closed, TActorSystem* system, TActorId recipient, ui64 cookie)
        : Stream(std::move(stream))
        , EndpointId(endpointId)
        , Ssl(ssl)
        , Closed(std::move(closed))
        , System(system)
        , Recipient(recipient)
        , Cookie(cookie)
        , AuthState(Stream->GetAuthState().NeedAuth)
    {
        Request.set_database(database);
    }

    void SetBoard(const TActorId& cache, const TString& path, ui64 revision) {
        Snapshot->Cache = cache;
        Snapshot->Path = path;
        Snapshot->Revision = revision;
    }

    void Reply(NProtoBuf::Message* response, ui32) override {
        Snapshot->Response.CopyFrom(*response);
        Complete();
    }

    void Reply(grpc::ByteBuffer* response, ui32, EStreamCtrl) override {
        Snapshot->Status = grpc::SerializationTraits<Ydb::Discovery::ListEndpointsResponse>::Deserialize(
            response, &Snapshot->Response);
        Complete();
    }

    void ReplyUnauthenticated(const TString& message) override {
        ReplyError(grpc::StatusCode::UNAUTHENTICATED, message, {});
    }

    void ReplyError(grpc::StatusCode code, const TString& message, const TString& details) override {
        Snapshot->Status = grpc::Status(code, message, details);
        Complete();
    }

    const NProtoBuf::Message* GetRequest() const override { return &Request; }
    NYdbGrpc::TAuthState& GetAuthState() override { return AuthState; }
    TInstant Deadline() const override { return Stream->Transport().Deadline(); }
    TSet<TStringBuf> GetPeerMetaKeys() const override { return Stream->Transport().GetPeerMetaKeys(); }
    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override { return Stream->GetPeerMetaValues(key); }
    TVector<TStringBuf> FindClientCert() const override { return Stream->Transport().FindClientCert(); }
    grpc_compression_level GetCompressionLevel() const override { return Stream->GetCompressionLevel(); }
    google::protobuf::Arena* GetArena() override { return &Arena; }
    TString GetPeer() const override { return Stream->GetPeerName(); }
    TString GetAuthority() const override { return Stream->Transport().GetAuthority(); }
    TString GetEndpointId() const override { return EndpointId; }
    TString GetRpcMethodName() const override { return Stream->GetRpcMethodName(); }
    bool SslServer() const override { return Ssl; }
    bool IsClientLost() const override { return Closed->load(); }
    bool IsStreamCall() const override { return false; }
    void AddTrailingMetadata(const TString&, const TString&) override {}
    void UseDatabase(const TString&) override {}
    void SetNextReplyCallback(TOnNextReply&&) override {}
    void FinishStreamingOk() override {}
    TAsyncFinishResult GetFinishFuture() override { return {}; }

private:
    void Complete() {
        System->Send(new IEventHandle(Recipient, {}, Snapshot.Release(), 0, Cookie));
    }

    TIntrusivePtr<TContext> Stream;
    const TString EndpointId;
    const bool Ssl;
    const std::shared_ptr<std::atomic_bool> Closed;
    TActorSystem* const System;
    const TActorId Recipient;
    const ui64 Cookie;
    NYdbGrpc::TAuthState AuthState;
    google::protobuf::Arena Arena;
    Ydb::Discovery::ListEndpointsRequest Request;
    THolder<TEvSnapshot> Snapshot = MakeHolder<TEvSnapshot>();
};

class TSnapshotRequest final : public TWatchedListEndpointsRequest {
public:
    explicit TSnapshotRequest(TSnapshotContext* context)
        : TWatchedListEndpointsRequest(context)
        , Context(context)
    {}

    void OnSnapshot(const TActorId& cache, const TString& path, ui64 revision) override {
        Context->SetBoard(cache, path, revision);
    }
private:
    TSnapshotContext* const Context; // owned by TWatchedListEndpointsRequest
};

class TEdsStreamActor final : public TActorBootstrapped<TEdsStreamActor> {
public:
    static constexpr auto ActorActivityType() { return NKikimrServices::TActivity::GRPC_REQ; }

    TEdsStreamActor(TIntrusivePtr<TContext> stream, TActorId proxy, TString endpointId, bool ssl)
        : Stream(std::move(stream)), Proxy(proxy), EndpointId(std::move(endpointId)), Ssl(ssl)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Stream->Attach(SelfId());
        const auto headers = Stream->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER);
        if (headers.size() != 1 || headers.front().empty()) {
            return Finish({grpc::StatusCode::INVALID_ARGUMENT, "Expected one x-ydb-database header"});
        }
        Database = CGIUnescapeRet(headers.front());
        Stream->Read();
        ScheduleRevalidation();
    }

private:
    STRICT_STFUNC(StateWork,
        hFunc(TContext::TEvReadFinished, Handle);
        hFunc(TEvDiscovery::TEvChanged, Handle);
        cFunc(TEvents::TSystem::Wakeup, Revalidate);
        cFunc(TContext::EvNotifiedWhenDone, PassAway);
        cFunc(TEvents::TSystem::Poison, PassAway);
    )

    void Handle(TContext::TEvReadFinished::TPtr& ev) {
        if (!ev->Get()->Success) {
            return Finish(grpc::Status::OK);
        }
        const auto& request = ev->Get()->Record;
        if (request.type_url() != ::NYdb::NEds::TypeUrl || request.resource_names_size() != 1
                || request.resource_names(0) != Database) {
            return Finish({grpc::StatusCode::INVALID_ARGUMENT, "Expected the EDS resource named by x-ydb-database"});
        }
        if (!Subscribed) {
            if (!request.response_nonce().empty() || request.has_error_detail()) {
                return Finish({grpc::StatusCode::INVALID_ARGUMENT, "Unexpected nonce or error on initial subscription"});
            }
            Subscribed = true;
            Refresh();
        } else if (!InFlightNonce.empty() && request.response_nonce() == InFlightNonce) {
            if (!request.has_error_detail() && request.version_info() != Version) {
                return Finish({grpc::StatusCode::INVALID_ARGUMENT, "ACK version does not match its nonce"});
            }
            // A NACK also releases this response. Retry only after a change.
            InFlightNonce.clear();
            Watch();
        }
        Stream->Read();
    }

    void Refresh() {
        Updating = true;
        const ui64 cookie = AllocateWaitCookie();
        auto context = MakeIntrusive<TSnapshotContext>(Stream, Database, EndpointId, Ssl, Closed,
            TActivationContext::ActorSystem(), SelfId(), cookie);
        Send(Proxy, new TSnapshotRequest(context.Get()));
        auto ev = co_await ActorWaitForEvent<TEvSnapshot>(cookie);
        auto& snapshot = *ev->Get();
        const grpc::Status status = snapshot.Status.ok() ? ToGrpcStatus(snapshot.Response.operation()) : snapshot.Status;
        if (!status.ok()) {
            Finish(status);
            co_return;
        }
        Cache = snapshot.Cache;
        BoardPath = std::move(snapshot.Path);
        Revision = snapshot.Revision;

        Ydb::Discovery::ListEndpointsResult result;
        if (!snapshot.Response.operation().result().UnpackTo(&result)) {
            Finish({grpc::StatusCode::INTERNAL, "Invalid ListEndpoints result"});
            co_return;
        }
        // Unary discovery shuffles the order for balancing. An order change is
        // not an endpoint update and must not produce a new resource version.
        std::sort(result.mutable_endpoints()->begin(), result.mutable_endpoints()->end(),
            [](const auto& a, const auto& b) {
                return std::make_tuple(a.address(), a.port()) < std::make_tuple(b.address(), b.port());
            });
        const auto version = MD5::Data(result.SerializeAsString());
        if (version != Version) {
            Version = version;
            InFlightNonce = ToString(++Sequence);
            Ydb::Xds::DiscoveryResponse response;
            ::NYdb::NEds::Encode(result, Database, Version, InFlightNonce, response);
            if (!Stream->Write(std::move(response))) {
                PassAway();
                co_return;
            }
            auto written = co_await ActorWaitForEvent<TContext::TEvWriteFinished>(0);
            if (!written->Get()->Success) {
                PassAway();
                co_return;
            }
        }
        Updating = false;
        Watch();
    }

    void Watch() {
        if (!Updating && InFlightNonce.empty() && Cache) {
            Send(Cache, new TEvDiscovery::TEvWatch(BoardPath, Revision));
        }
    }

    void Handle(TEvDiscovery::TEvChanged::TPtr&) {
        if (!Updating && InFlightNonce.empty()) {
            Refresh();
        }
    }

    void ScheduleRevalidation() {
        Schedule(TDuration::Minutes(5) + TDuration::Seconds(RandomNumber<ui32>(60)), new TEvents::TEvWakeup);
    }

    void Revalidate() {
        // Aliases, shared database placement and credentials can change while
        // the endpoint board stays still.
        ScheduleRevalidation();
        if (Subscribed && !Updating && InFlightNonce.empty()) {
            Unwatch();
            Refresh();
        }
    }

    void Unwatch() {
        if (Cache) {
            Send(Cache, new TEvDiscovery::TEvUnwatch(BoardPath));
        }
    }

    void Finish(const grpc::Status& status) {
        Stream->Finish(status);
        PassAway();
    }

    void PassAway() override {
        Closed->store(true);
        Unwatch();
        TActorBootstrapped::PassAway();
    }

    TIntrusivePtr<TContext> Stream;
    const TActorId Proxy;
    const TString EndpointId;
    const bool Ssl;
    const std::shared_ptr<std::atomic_bool> Closed = std::make_shared<std::atomic_bool>(false);
    TString Database;
    TActorId Cache;
    TString BoardPath;
    ui64 Revision = 0;
    ui64 Sequence = 0;
    TString InFlightNonce;
    TString Version;
    bool Subscribed = false;
    bool Updating = false;
};

void TEdsService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr) {
    auto counters = CreateCounterCb(Counters_, ActorSystem_);
    TStream::Start(this, &Service_, CQ_, &TGrpcAsyncService::RequestStreamEndpoints,
        [this](TIntrusivePtr<TContext> stream) {
            ReportGrpcReqToMon(*ActorSystem_, stream->GetPeerName());
            ActorSystem_->Register(new TEdsStreamActor(std::move(stream), GRpcRequestProxyId_, GetEndpointId(), SslServer()));
        }, *ActorSystem_, "StreamEndpoints", counters("discovery", "StreamEndpoints", true));
}

} // namespace

NYdbGrpc::IGRpcService* CreateEndpointDiscoveryService(NActors::TActorSystem* system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const NActors::TActorId& proxy, bool rlAllowed) {
    return new TEdsService(system, std::move(counters), proxy, rlAllowed);
}

} // namespace NKikimr::NGRpcService
