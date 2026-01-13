#pragma once

#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NRpcService {

// Local rpc streaming session:
// Owner actor        Rpc actor
//    TEvActorAttached(Rpc actor id)
//       <----------------
//    TEvReadRequest()
//       <----------------
//    IGRpcStreamingContext::TEvReadFinished(Proto response)
//       ---------------->
//    TEvWriteRequest(Proto request, Is success)
//       <----------------
//    IGRpcStreamingContext::TEvWriteFinished(Is success)
//       ---------------->
//    TEvFinishRequest(Status)
//       <----------------
//    IGRpcStreamingContext::TEvNotifiedWhenDone(Is success)
//       ---------------->
//
// To finish rpc session before TEvFinishRequest owner actor should send IGRpcStreamingContext::TEvNotifiedWhenDone to rpc actor

template <class TIn, class TOut>
class TLocalRpcBiStreamingCtx : public NGRpcServer::IGRpcStreamingContext<TIn, TOut> {
    using TBase = NGRpcServer::IGRpcStreamingContext<TIn, TOut>;

public:
    struct TRpcEvents {
        enum EEv {
            EvActorAttached = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvReadRequest,
            EvWriteRequest,
            EvFinishRequest,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvActorAttached : public TEventLocal<TEvActorAttached, EvActorAttached> {
            explicit TEvActorAttached(const TActorId& rpcActor)
                : RpcActor(rpcActor)
            {}

            const TActorId RpcActor;
        };

        struct TEvReadRequest : public TEventLocal<TEvReadRequest, EvReadRequest> {
        };

        struct TEvWriteRequest : public TEventLocal<TEvWriteRequest, EvWriteRequest> {
            TEvWriteRequest(TOut&& message, const grpc::WriteOptions& options)
                : Message(std::move(message))
                , Options(options)
            {}

            TOut Message;
            const grpc::WriteOptions Options;
        };

        struct TEvFinishRequest : public TEventLocal<TEvFinishRequest, EvFinishRequest> {
            explicit TEvFinishRequest(const grpc::Status& status)
                : Status(status)
            {}

            const grpc::Status Status;
        };
    };

    struct TSettings {
        TString Database;
        std::optional<TString> Token;
        TString PeerName = "localhost";
        std::optional<TString> RequestType;
        TString ParentTraceId;
        TString TraceId;
        TString RpcMethodName;
    };

    TLocalRpcBiStreamingCtx(const TActorSystem* actorSystem, const TActorId& owner, const TSettings& settings)
        : ActorSystem(actorSystem)
        , Owner(owner)
        , AuthState(/* needAuth */ false)
        , Database(settings.Database)
        , Token(settings.Token)
        , PeerName(settings.PeerName)
        , RequestType(settings.RequestType)
        , ParentTraceId(settings.ParentTraceId)
        , TraceId(settings.TraceId)
        , RpcMethodName(settings.RpcMethodName)
    {
        Y_VALIDATE(ActorSystem, "ActorSystem is not set");
        Y_VALIDATE(Owner, "Owner is not set");
        AuthState.State = NYdbGrpc::TAuthState::AS_OK;
    }

    void Cancel() override {
        Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Request cancelled"));
    }

    void Attach(TActorId actor) override {
        RpcActor = actor;
        ActorSystem->Send(Owner, new TRpcEvents::TEvActorAttached(actor));
    }

    bool Read() override {
        if (!RpcActor) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Actor is not attached to context before read call"));
            return false;
        }

        if (IsFinished) {
            return false;
        }

        ActorSystem->Send(Owner, new TRpcEvents::TEvReadRequest());
        return true;
    }

    bool Write(TOut&& message, const grpc::WriteOptions& options) override {
        if (!RpcActor) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Actor is not attached to context before write call"));
            return false;
        }

        if (IsFinished) {
            return false;
        }

        ActorSystem->Send(Owner, new TRpcEvents::TEvWriteRequest(std::move(message), options));
        return true;
    }

    bool WriteAndFinish(TOut&& message, const grpc::Status& status) override {
        return WriteAndFinish(std::move(message), {}, status);
    }

    bool WriteAndFinish(TOut&& message, const grpc::WriteOptions& options, const grpc::Status& status) override {
        if (!Write(std::move(message), options)) {
            return false;
        }
        return Finish(status);
    }

    bool Finish(const grpc::Status& status) override {
        if (IsFinished) {
            return false;
        }

        IsFinished = true;
        ActorSystem->Send(Owner, new TRpcEvents::TEvFinishRequest(status));

        if (RpcActor) {
            ActorSystem->Send(RpcActor, new TBase::TEvNotifiedWhenDone(/* success */ true));
        }

        return true;
    }

    NYdbGrpc::TAuthState& GetAuthState() const override {
        return AuthState;
    }

    TString GetPeerName() const override {
        return PeerName;
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        if (key == NYdb::YDB_DATABASE_HEADER) {
            return {TStringBuf(Database)};
        }
        if (key == NYdb::YDB_AUTH_TICKET_HEADER) {
            return Token ? TVector{TStringBuf(*Token)} : TVector<TStringBuf>();
        }
        if (key == NYdb::YDB_REQUEST_TYPE_HEADER) {
            return RequestType ? TVector{TStringBuf(*RequestType)} : TVector<TStringBuf>();
        }
        if (key == NYdb::YDB_TRACE_ID_HEADER) {
            return TraceId ? TVector{TStringBuf(TraceId)} : TVector<TStringBuf>();
        }
        if (key == NYdb::OTEL_TRACE_HEADER) {
            return ParentTraceId ? TVector{TStringBuf(ParentTraceId)} : TVector<TStringBuf>();
        }

        const auto it = PeerMeta.find(TString(key));
        return it != PeerMeta.end() ? TVector{TStringBuf(it->second)} : TVector<TStringBuf>();
    }

    void PutPeerMeta(const TString& key, const TString& value) {
        PeerMeta[key] = value;
    }

    grpc_compression_level GetCompressionLevel() const override {
        return grpc_compression_level::GRPC_COMPRESS_LEVEL_NONE;
    }

    void UseDatabase(const TString& database) override {
        Database = database;
    }

    TString GetRpcMethodName() const override {
        return RpcMethodName;
    }

private:
    const TActorSystem* const ActorSystem = nullptr;
    const TActorId Owner;
    mutable NYdbGrpc::TAuthState AuthState;
    TString Database;
    const std::optional<TString> Token;
    const TString PeerName;
    std::unordered_map<TString, TString> PeerMeta;
    const std::optional<TString> RequestType;
    const TString ParentTraceId;
    const TString TraceId;
    const TString RpcMethodName;
    TActorId RpcActor;
    bool IsFinished = false;
};

} // namespace NKikimr::NRpcService
