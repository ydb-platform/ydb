#pragma once

#include "cache_item.h"
#include "request_data.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/util/simple_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

#include <util/datetime/base.h>

namespace NKikimr {

    namespace NPrivate {

        template<ui32 RequestTag, ui32 ResponseTag, ui32 ResultTag, class TProtoService, class TProtoRequest, class TProtoResponse, class TRequestData>
        struct TGrpcRequestActorTypes {

            struct TEvRequest;
            struct TEvResponse;
            struct TEvCachedResult;

            using TCachedItemPtr = TIntrusivePtr<typename TEvCachedResult::TCachedItem>;
            using TEvRequestType = TEvRequest;
            using TEvResponseType = TEvResponse;
            using TEvResultType = TEvCachedResult;
            using TServiceConnection = NYdbGrpc::TServiceConnection<TProtoService>;
            using TGrpcRequestType = typename NYdbGrpc::TSimpleRequestProcessor<typename TProtoService::Stub, TProtoRequest, TProtoResponse>::TAsyncRequest;

            struct TEvRequest : TEventLocal<TEvRequest, RequestTag> {
                TProtoRequest Request;
                TRequestData Data;

                TEvRequest() = default;
                TEvRequest(TRequestData data, TProtoRequest request)
                        : Request(std::move(request)), Data(std::move(data)) {
                }
                TEvRequest(TProtoRequest request)
                        : Request(std::move(request)) {
                }

                TString GetCacheKey() const { return Request.ShortDebugString(); }
            };

            struct TEvResponse : TEventLocal<TEvResponse, ResponseTag> {
                THolder <IEventHandle> Request;
                TProtoResponse Response;
                NYdbGrpc::TGrpcStatus Status;
            };

            struct TEvCachedResult : TEventLocal<TEvCachedResult, ResultTag> {
                struct TCachedItem : public TCacheItem<TEvRequest, TEvResponse, TEvCachedResult> {
                    using TBase = TCacheItem<TEvRequest, TEvResponse, TEvCachedResult>;
                    TRequestData Data;

                    TCachedItem(const TString& key)
                        : TBase(key)
                    {}
                };

                TIntrusivePtr<TCachedItem> CacheItem;
                TProtoResponse Response;
                NYdbGrpc::TGrpcStatus Status;

                TEvCachedResult(TIntrusivePtr<TCachedItem> cacheItem)
                        : CacheItem(cacheItem) {}
            };

        };

    }

    template<ui32 RequestTag, ui32 ResponseTag, ui32 ResultTag, class TProtoService, class TProtoRequest, class TProtoResponse, class TRequestData>
    struct TCachedRequestProcessor {

        class TGrpcRequestActor;

        using TTypes = NPrivate::TGrpcRequestActorTypes<RequestTag, ResponseTag, ResultTag, TProtoService, TProtoRequest, TProtoResponse, TRequestData>;

        using TService = TProtoService;
        using TRequest = TProtoRequest;
        using TResponse = TProtoResponse;
        using TCachedItem = typename TTypes::TEvCachedResult::TCachedItem;
        using TCachedItemPtr = typename TTypes::TCachedItemPtr;
        using TEvRequestType = typename TTypes::TEvRequest;
        using TEvResponseType = typename TTypes::TEvResponse;
        using TEvResultType = typename TTypes::TEvCachedResult;
        using TServiceConnection = NYdbGrpc::TServiceConnection<TService>;
        using TGrpcRequestType = typename NYdbGrpc::TSimpleRequestProcessor<typename TService::Stub, TRequest, TResponse>::TAsyncRequest;

        struct TEvPrivate {
            enum EEv {
                EvResponse = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvError,
                EvEnd
            };

            struct TEvResponse : TEventLocal<TEvResponse, EvResponse> {
                TResponse Response;

                TEvResponse(TResponse&& response)
                        : Response(std::move(response))
                {}
            };

            struct TEvError : TEventLocal<TEvError, EvError> {
                NYdbGrpc::TGrpcStatus Status;

                TEvError(NYdbGrpc::TGrpcStatus&& status)
                        : Status(std::move(status))
                {}
            };

            static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
        };

        TSimpleCache<TString, typename TTypes::TCachedItemPtr> Cache;
        std::shared_ptr<TServiceConnection> GrpcConnection;
        NYdbGrpc::IQueueClientContextProvider& GrpcContextProvider;
        TGrpcRequestType GrpcProcessor;

        TCachedRequestProcessor(std::shared_ptr<TServiceConnection> connection, NYdbGrpc::IQueueClientContextProvider& contextProvider, TGrpcRequestType processor)
            : GrpcConnection(connection)
            , GrpcContextProvider(contextProvider)
            , GrpcProcessor(processor)
        {}

        class TGrpcRequestActor : public TActorBootstrapped<TGrpcRequestActor> {
        private:
            using TThis = TGrpcRequestActor;
            using TBase = TActorBootstrapped<TGrpcRequestActor>;

            TActorId Sender;
            TCachedItemPtr CacheItem;
            std::shared_ptr<TServiceConnection> Connection;
            std::shared_ptr<NYdbGrpc::IQueueClientContext> Context;
            TGrpcRequestType GrpcProcessor;

        public:
            TGrpcRequestActor(
                std::shared_ptr<TServiceConnection> connection,
                std::shared_ptr<NYdbGrpc::IQueueClientContext> context,
                const TActorId& sender,
                TCachedItemPtr item,
                TGrpcRequestType processor
            )
                : Sender(sender)
                , CacheItem(item)
                , Connection(connection)
                , Context(context)
                , GrpcProcessor(processor)
            {}

            void Bootstrap(const TActorContext& ctx) {
                TActorId actorId = ctx.SelfID;
                TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;

                NYdbGrpc::TResponseCallback<TResponse> responseCb =
                        [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, TResponse&& response) -> void {
                            if (status.Ok()) {
                                actorSystem->Send(actorId, new typename TEvPrivate::TEvResponse(std::move(response)));
                            } else {
                                actorSystem->Send(actorId, new typename TEvPrivate::TEvError(std::move(status)));
                            }
                        };

                auto callMeta = CacheItem->Data.FillCallMeta();
                Connection->DoRequest(CacheItem->GetRequest(), std::move(responseCb), GrpcProcessor, callMeta);
                TBase::Become(&TThis::StateWork, DEFAULT_CACHED_GRPC_REQUEST_TIMEOUT * 2, new TEvents::TEvWakeup());
            }

            void Handle(typename TEvPrivate::TEvResponse::TPtr& ev, const TActorContext& ctx) {
                THolder<TEvResultType> result = MakeHolder<TEvResultType>(CacheItem);
                result->Response = std::move(ev->Get()->Response);
                ctx.Send(Sender, result.Release());
                TBase::Die(ctx);
            }

            void Handle(typename TEvPrivate::TEvError::TPtr& ev, const TActorContext& ctx) {
                THolder<TEvResultType> result = MakeHolder<TEvResultType>(CacheItem);
                result->Status = ev->Get()->Status;
                ctx.Send(Sender, result.Release());
                TBase::Die(ctx);
            }

            void HandleTimeout(const TActorContext& ctx) {
                THolder<TEvResultType> result = MakeHolder<TEvResultType>(CacheItem);
                result->Status = NYdbGrpc::TGrpcStatus("Timeout", grpc::StatusCode::DEADLINE_EXCEEDED, true);
                ctx.Send(Sender, result.Release());
                TBase::Die(ctx);
            }

            void StateWork(TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvPrivate::TEvResponse, Handle);
                    HFunc(TEvPrivate::TEvError, Handle);
                    CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
                    CFunc(TEvents::TSystem::PoisonPill, TBase::Die);
                }
            }
        };

        void HandleRequest(typename TEvRequestType::TPtr& ev, const TActorContext& ctx) {
            TString key = ev->Get()->GetCacheKey();
            auto item = Cache.Update(key);
            if (item != nullptr && item->IsValid(ctx.Now())) {
                item->Respond(ev, ctx);
                return;
            }
            ev->Get()->Data.FillCachedRequestData(item ? TMaybe<TRequestData>(item->Data) : Nothing());
            if (item == nullptr) {
                item = Cache.Update(key, new TCachedItem(key));
                item->Data = ev->Get()->Data;
            }
            if (item->Waiters.empty()) {
                item->Request = &ev->Get()->Request;
                ctx.Register(new TGrpcRequestActor(GrpcConnection, GrpcContextProvider.CreateContext(), ctx.SelfID, item, GrpcProcessor));
            }
            item->Waiters.emplace_back(std::move(ev));
        }

        void HandleResponse(typename TEvResultType::TPtr ev, const TActorContext& ctx) {
            ev->Get()->CacheItem->Update(std::move(*ev->Get()), ctx);
        }
    };

}
