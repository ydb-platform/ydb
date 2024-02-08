#pragma once
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr {
namespace NPQ {

    struct TOwnerInfo {
        bool NeedResetOwner;
        bool SourceIdDeleted;
        TString OwnerCookie;
        ui64 NextMessageNo;
        ui32 OwnerGeneration;
        TActorId PipeClient;
        TActorId Sender;

        ui64 ReservedSize;
        std::deque<ui64> Requests;

        std::deque<THolder<TEvPQ::TEvChangeOwner>> WaitToChangeOwner;

        TOwnerInfo()
            : NeedResetOwner(true)
            , SourceIdDeleted(false)
            , NextMessageNo(0)
            , OwnerGeneration(0)
            , ReservedSize(0)
        {}

        static TStringBuf GetOwnerFromOwnerCookie(const TString& owner);
        void GenerateCookie(const TString& owner, const TActorId& pipeClient, const TActorId& sender,
                            const TString& topicName, const TPartitionId& partition, const TActorContext& ctx);

        void AddReserveRequest(ui64 size, bool lastRequest) {
            ReservedSize += size;
            if (!lastRequest) {
                Requests.push_back(size);
            } else {
                Y_ABORT_UNLESS(!Requests.empty());
                Requests.back() += size;
            }
        }

        ui64 DecReservedSize() {
            //TODO: Y_ABORT_UNLESS(!Requests.empty());
            if (Requests.empty())
                return 0;
            ui64 size = Requests.front();
            Requests.pop_front();
            ReservedSize -= size;
            return size;
        }

        TOwnerInfo(const TOwnerInfo&) = delete;
    };
} //NPQ
} // NKikimr
