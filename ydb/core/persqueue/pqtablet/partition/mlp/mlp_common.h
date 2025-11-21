#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NPQ::NMLP {

struct TDLQMessage {
    ui64 Offset;
    ui64 SeqNo;
};

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

std::unique_ptr<TEvPersQueue::TEvRequest> MakeEvPQRead(
    const TString& consumerName,
    ui32 partitionId,
    ui64 startOffset,
    std::optional<ui64> count = std::nullopt
);

std::unique_ptr<TEvPQ::TEvRead> MakeEvRead(
    const NActors::TActorId& selfId,
    const TString& consumerName,
    ui64 startOffset,
    ui64 count,
    ui64 cookie,
    ui64 nextPartNo = 0
);

std::unique_ptr<TEvPQ::TEvSetClientInfo> MakeEvCommit(
    const NKikimrPQ::TPQTabletConfig::TConsumer& consumer,
    ui64 offset,
    ui64 cookie = 0
);

std::unique_ptr<TEvPersQueue::TEvHasDataInfo> MakeEvHasData(
    const TActorId& selfId,
    ui32 partitionId,
    ui64 offset,
    const NKikimrPQ::TPQTabletConfig::TConsumer& consumer
);

bool IsSucess(const TEvPQ::TEvProxyResponse::TPtr& ev);
bool IsSucess(const TEvPersQueue::TEvResponse::TPtr& ev);
ui64 GetCookie(const TEvPQ::TEvProxyResponse::TPtr& ev);

NActors::IActor* CreateMessageEnricher(ui64 tabletId,
                                       const ui32 partitionId,
                                       const TString& consumerName,
                                       std::deque<TReadResult>&& replies);

struct TDLQMoverSettings {
    TActorId ParentActorId;
    TString Database;
    ui64 TabletId;
    ui32 PartitionId;
    TString ConsumerName;
    ui64 ConsumerGeneration;
    TString DestinationTopic;
    std::deque<TDLQMessage> Messages;
};

NActors::IActor* CreateDLQMover(TDLQMoverSettings&& settings);

} // namespace NKikimr::NPQ::NMLP

template<>
inline void Out<std::pair<ui64 const, ui64>>(IOutputStream& o, const std::pair<ui64 const, ui64>& p) {
    o << "(" << p.first << ", " << p.second << ")";
}

template<>
inline void Out<std::pair<ui64, ui64>>(IOutputStream& o, const std::pair<ui64, ui64>& p) {
    o << "(" << p.first << ", " << p.second << ")";
}

template<>
inline void Out<NKikimr::NPQ::NMLP::TDLQMessage>(IOutputStream& o, const NKikimr::NPQ::NMLP::TDLQMessage& p) {
    o << "(" << p.Offset << ", " << p.SeqNo << ")";
}
