#pragma once

#include <variant>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mailbox.h>
#include <ydb/core/util/simple_cache.h>

namespace NGrpcActorClient {

template <typename TEventRequestType, typename TEventResponseType>
class TGrpcServiceCache : public NActors::TActorBootstrapped<TGrpcServiceCache<TEventRequestType, TEventResponseType>> {
    using TThis = TGrpcServiceCache<TEventRequestType, TEventResponseType>;
    using TBase = NActors::TActorBootstrapped<TThis>;
    TDuration SuccessLifeTime = TDuration::Minutes(1);
    TDuration ErrorLifeTime = TDuration::Seconds(10);

    struct TCacheRecord {
        TInstant UpdateTimestamp;
        THolder<TEventResponseType> Response;
        std::deque<typename TEventRequestType::TPtr> Waiters;

        bool IsSafeToRelease() {
            return Waiters.empty();
        }
    };

    std::variant<NActors::TActorId, NActors::IActor*> UnderlyingActor;
    NKikimr::TNotSoSimpleCache<TString, TCacheRecord> Cache;

    NActors::TActorId GetUnderlyingActor() {
        return std::get<NActors::TActorId>(UnderlyingActor);
    }

    static TString GetCacheKey(const TEventRequestType* request) {
        return request->Token + request->Request.ShortDebugString();
    }

    bool IsTemporaryErrorStatus(const TCacheRecord& record) const {
        if (!record.Response) {
            return false;
        }
        return record.Response->Status.InternalError
               || record.Response->Status.GRpcStatusCode == grpc::StatusCode::UNKNOWN
               || record.Response->Status.GRpcStatusCode == grpc::StatusCode::DEADLINE_EXCEEDED
               || record.Response->Status.GRpcStatusCode == grpc::StatusCode::INTERNAL
               || record.Response->Status.GRpcStatusCode == grpc::StatusCode::UNAVAILABLE;
    }

    bool IsCacheRecordValid(const TCacheRecord& record, TInstant now = NActors::TActivationContext::Now()) const {
        if (record.Response && record.UpdateTimestamp) {
            TDuration lifeTime = now - record.UpdateTimestamp;
            if (!record.Response->Status.Ok()) {
                return !IsTemporaryErrorStatus(record) && (lifeTime < ErrorLifeTime);
            } else {
                return lifeTime < SuccessLifeTime;
            }
        } else {
            return false;
        }
    }

    void SendReply(typename TEventRequestType::TPtr& ev, TCacheRecord* cacheRecord) {
        NActors::TActorId sender = ev->Sender;
        THolder<TEventResponseType> response = MakeHolder<TEventResponseType>();
        if (ev->HasEvent()) {
            response->Request = ev;
        } else {
            response->Request = std::move(cacheRecord->Response->Request);
        }
        response->Response = cacheRecord->Response->Response;
        response->Status = cacheRecord->Response->Status;
        TBase::Send(sender, response.Release());
    }

    void Handle(typename TEventRequestType::TPtr& ev) {
        TString cacheKey = GetCacheKey(ev->Get());
        TCacheRecord* cacheRecord = Cache.FindPtr(cacheKey);
        if (cacheRecord) {
            if (IsCacheRecordValid(*cacheRecord)) {
                SendReply(ev, cacheRecord);
                return;
            }
        } else {
            cacheRecord = &Cache.Update(cacheKey);
        }
        if (cacheRecord->Waiters.empty()) {
            THolder<TEventRequestType> request = ev->Release();
            TBase::Send(GetUnderlyingActor(), request.Release());
        }
        cacheRecord->Waiters.emplace_back(ev);
    }

    void Handle(typename TEventResponseType::TPtr& ev) {
        TEventRequestType* request = ev->Get()->Request->template Get<TEventRequestType>();
        TString cacheKey = GetCacheKey(request);
        TCacheRecord* cacheRecord = Cache.FindPtr(cacheKey);
        if (cacheRecord) {
            cacheRecord->Response = ev->Release();
            cacheRecord->UpdateTimestamp = NActors::TActivationContext::Now();
            for (typename TEventRequestType::TPtr& waiter : cacheRecord->Waiters) {
                SendReply(waiter, cacheRecord);
            }
            cacheRecord->Waiters.clear();
        } else {
            LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::GRPC_CLIENT, "Couldn't find cache record for request");
        }
    }

    void PassAway() {
        TBase::Send(GetUnderlyingActor(), new NActors::TEvents::TEvPoison());
        TBase::PassAway();
    }

public:
    void Bootstrap() {
        if (std::holds_alternative<NActors::IActor*>(UnderlyingActor)) {
            // we register underlying actor with the same pool and mailbox type
            auto& ctx = NActors::TlsActivationContext;
            const auto& mailbox = ctx->Mailbox;
            UnderlyingActor = ctx->Register(std::get<NActors::IActor*>(UnderlyingActor), TBase::SelfId(), static_cast<NActors::TMailboxType::EType>(mailbox.Type));
        }
        TBase::Become(&TThis::StateWork);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::ACTOR_SERVICE_CACHE; }

    TGrpcServiceCache(const std::variant<NActors::TActorId, NActors::IActor*>& underlyingActor,
                      size_t grpcCacheSize = 1024,
                      TDuration grpcCacheSuccessLifeTime = TDuration::Minutes(1),
                      TDuration grpcCacheErrorLifeTime = TDuration::Seconds(10))
        : UnderlyingActor(underlyingActor)
    {
        Cache.MaxSize = grpcCacheSize;
        SuccessLifeTime = grpcCacheSuccessLifeTime;
        ErrorLifeTime = grpcCacheErrorLifeTime;
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEventRequestType, Handle);
            hFunc(TEventResponseType, Handle);
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
            default:
                this->Forward(ev, GetUnderlyingActor());
                break;
        }
    }
};

template <typename TEventRequestType, typename TEventResponseType>
inline NActors::IActor* CreateGrpcServiceCache(const NActors::TActorId& underlyingActor,
                                               size_t grpcCacheSize = 1024,
                                               TDuration grpcCacheSuccessLifeTime = TDuration::Minutes(1),
                                               TDuration grpcCacheErrorLifeTime = TDuration::Seconds(10)) {
    return new TGrpcServiceCache<TEventRequestType, TEventResponseType>(underlyingActor, grpcCacheSize, grpcCacheSuccessLifeTime, grpcCacheErrorLifeTime);
}

template <typename TEventRequestType, typename TEventResponseType>
inline NActors::IActor* CreateGrpcServiceCache(NActors::IActor* underlyingActor,
                                               size_t grpcCacheSize = 1024,
                                               TDuration grpcCacheSuccessLifeTime = TDuration::Minutes(1),
                                               TDuration grpcCacheErrorLifeTime = TDuration::Seconds(10)) {
    return new TGrpcServiceCache<TEventRequestType, TEventResponseType>(underlyingActor, grpcCacheSize, grpcCacheSuccessLifeTime, grpcCacheErrorLifeTime);
}

} // namespace NGrpcActorClient
