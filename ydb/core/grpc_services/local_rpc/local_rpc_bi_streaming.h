#pragma once

#include <ydb/core/grpc_streaming/grpc_streaming.h>

namespace NKikimr::NRpcService {

template <class TIn, class TOut>
class TLocalRpcStreamingCtxBase : public NGRpcServer::IGRpcStreamingContext<TIn, TOut> {
    using TBase = NGRpcServer::IGRpcStreamingContext<TIn, TOut>;

public:
    struct TSettings {
        TString Database;
        std::optional<TString> Token;
        TString PeerName = "localhost";
        std::optional<TString> RequestType;
        TString ParentTraceId;
        TString TraceId;
        TString RpcMethodName;
    };

    explicit TLocalRpcStreamingCtxBase(const TSettings& settings)
        : AuthState(/* needAuth */ false)
        , Database(settings.Database)
        , Token(settings.Token)
        , PeerName(settings.PeerName)
        , RequestType(settings.RequestType)
        , ParentTraceId(settings.ParentTraceId)
        , TraceId(settings.TraceId)
        , RpcMethodName(settings.RpcMethodName)
    {
        AuthState.State = NYdbGrpc::TAuthState::AS_OK;
    }

    void Cancel() override {
        Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Request cancelled"));
    }

    void Attach(TActorId actor) override {
        DoAttach(actor);
        IsAttached = true;
    }

    bool Read() override {
        if (!IsAttached) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Actor is not attached to context before read call"));
            return false;
        }

        if (IsFinished) {
            return false;
        }

        DoRead();
        return true;
    }

    bool Write(TOut&& message, const grpc::WriteOptions& options) override {
        if (!IsAttached) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Actor is not attached to context before write call"));
            return false;
        }

        if (IsFinished) {
            return false;
        }

        DoWrite(std::move(message), options);
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
        DoFinish(status);
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

protected:
    virtual void DoAttach(TActorId actor) = 0;

    // Should reply with IGRpcStreamingContext::TEvReadFinished
    virtual void DoRead() = 0;

    // Should reply with IGRpcStreamingContext::TEvWriteFinished
    virtual void DoWrite(TOut&& message, const grpc::WriteOptions& options) = 0;

    // Should reply with IGRpcStreamingContext::TEvNotifiedWhenDone
    virtual void DoFinish(const grpc::Status& status) = 0;

private:
    mutable NYdbGrpc::TAuthState AuthState;
    TString Database;
    const std::optional<TString> Token;
    const TString PeerName;
    std::unordered_map<TString, TString> PeerMeta;
    const std::optional<TString> RequestType;
    const TString ParentTraceId;
    const TString TraceId;
    const TString RpcMethodName;
    bool IsAttached = false;
    bool IsFinished = false;
};

} // namespace NKikimr::NRpcService
