#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NKqp {

struct TEvKqpExecuterStreamData : public TEventPBWithArena<TEvKqpExecuterStreamData, NKikimrKqp::TEvExecuterStreamData, TKqpExecuterEvents::EvStreamData> {
    using TBaseEv = TEventPBWithArena<TEvKqpExecuterStreamData, NKikimrKqp::TEvExecuterStreamData, TKqpExecuterEvents::EvStreamData>;
    using TBaseEv::TEventPBBase;

    TEvKqpExecuterStreamData() = default;
    explicit TEvKqpExecuterStreamData(TIntrusivePtr<NActors::TProtoArenaHolder> arena)
        : TEventPBBase(std::move(arena))
    {}
};

struct TEvKqpExecuterStreamDataAck : public TEventPB<TEvKqpExecuterStreamDataAck, NKikimrKqp::TEvExecuterStreamDataAck,
    TKqpExecuterEvents::EvStreamDataAck>
{
    friend class TEventPBBase;
    explicit TEvKqpExecuterStreamDataAck(ui64 seqno, ui64 channelId)
    {
        Record.SetSeqNo(seqno);
        Record.SetChannelId(channelId);
    }

private:
    // using a little hack to hide default empty constructor
    TEvKqpExecuterStreamDataAck() = default;
};

// deprecated event, remove in the future releases.
struct TEvKqpExecuterProgress : public TEventPB<TEvKqpExecuterProgress, NKikimrKqp::TEvExecuterProgress,
        TKqpExecuterEvents::EvProgress> {};

} // namespace NKikimr::NKqp
