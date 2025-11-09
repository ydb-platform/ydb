#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NPQ::NMLP {

struct TResult {
    TResult(const NActors::TActorId& sender, ui64 cookie)
        : Sender(sender)
        , Cookie(cookie)
    {
    }

    NActors::TActorId Sender;
    ui64 Cookie;
};

struct TReadResult : public TResult {
    TReadResult(const NActors::TActorId& sender, ui64 cookie, std::deque<ui64>&& offsets)
        : TResult(sender, cookie)
        , Offsets(std::move(offsets))
    {}

    std::deque<ui64> Offsets;
};

std::unique_ptr<TEvPQ::TEvRead> MakeEvRead(
    const NActors::TActorId& selfId,
    const TString& consumerName,
    ui64 startOffset,
    ui64 count,
    ui64 cookie,
    ui64 nextPartNo = 0
);

std::unique_ptr<TEvPQ::TEvSetClientInfo> MakeEvCommit(
    const NKikimrPQ::TPQTabletConfig::TConsumer consumer,
    ui64 offset,
    ui64 cookie = 0
);

bool IsSucess(const TEvPQ::TEvProxyResponse::TPtr& ev);
bool IsSucess(const TEvPersQueue::TEvResponse::TPtr& ev);
ui64 GetCookie(const TEvPQ::TEvProxyResponse::TPtr& ev);

NActors::IActor* CreateMessageEnricher(const NActors::TActorId& tabletActorId,
                                       const ui32 partitionId,
                                       const TString& consumerName,
                                       std::deque<TReadResult>&& replies);

NActors::IActor* CreateDLQMover(const TString& database,
                                const ui64 tabletId,
                                const ui32 partitionId,
                                const TString& consumerName,
                                const TString& destinationTopic,
                                std::deque<ui64>&& offsets);

} // namespace NKikimr::NPQ::NMLP
