#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NKikimr {

    template <typename TRequestEvent, typename TResponseEvent, typename TPrivateResponseEvent>
    struct TCacheItem : TThrRefBase {
        static constexpr TDuration SuccessLifeTime = TDuration::Hours(1);
        static constexpr TDuration ErrorLifeTime = TDuration::Minutes(1);

        using TRequestRecord = decltype(TRequestEvent().Request);
        using TResponseRecord = decltype(TResponseEvent().Response);
        TString CacheKey;
        TInstant UpdateTimestamp;
        TResponseRecord Response;
        NYdbGrpc::TGrpcStatus Status;
        TVector<THolder<NActors::IEventHandle>> Waiters;
        TRequestRecord* Request = nullptr;
        TString RequestId;

        TCacheItem(const TString& key)
                : CacheKey(key)
        {}

        const TRequestRecord& GetRequest() const {
            Y_ABORT_UNLESS(Request != nullptr);
            return *Request;
        }

        bool IsTemporaryErrorStatus() const {
            return Status.InternalError
                   || Status.GRpcStatusCode == grpc::StatusCode::UNKNOWN
                   || Status.GRpcStatusCode == grpc::StatusCode::DEADLINE_EXCEEDED
                   || Status.GRpcStatusCode == grpc::StatusCode::INTERNAL
                   || Status.GRpcStatusCode == grpc::StatusCode::UNAVAILABLE;
        }

        bool IsValid(TInstant now) const {
            if (UpdateTimestamp != TInstant()) {
                if (!Status.Ok()) {
                    return !IsTemporaryErrorStatus() && (now - UpdateTimestamp) < ErrorLifeTime;
                } else {
                    return (now - UpdateTimestamp) < SuccessLifeTime;
                }
            } else {
                return false;
            }
        }

        void Respond(THolder<NActors::IEventHandle>&& request, const NActors::TActorContext& ctx) const {
            TResponseEvent* result = new TResponseEvent();
            result->Response = Response;
            result->Status = Status;
            result->Request = std::move(request);
            ctx.Send(result->Request->Sender, result, 0, result->Request->Cookie);
        }

        void Update(TPrivateResponseEvent&& result, const NActors::TActorContext& ctx) {
            Response = std::move(result.Response);
            Status = std::move(result.Status);
            UpdateTimestamp = ctx.Now();
            for (THolder<NActors::IEventHandle>& request : Waiters) {
                Respond(std::move(request), ctx);
            }
            Waiters.clear();
        }
    };

}


