#pragma once

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/event_load.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/actors/util/rope.h>
#include <library/cpp/actors/prof/tag.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/lwtrace/shuttle.h>
#include <util/generic/string.h>
#include <util/generic/list.h>

#include "types.h"
#include "outgoing_stream.h"

#ifndef FORCE_EVENT_CHECKSUM
#define FORCE_EVENT_CHECKSUM 0
#endif

Y_FORCE_INLINE ui32 Crc32cExtendMSanCompatible(ui32 checksum, const void *data, size_t len) {
    if constexpr (NSan::MSanIsOn()) {
        const char *begin = static_cast<const char*>(data);
        const char *end = begin + len;
        begin -= reinterpret_cast<uintptr_t>(begin) & 15;
        end += -reinterpret_cast<uintptr_t>(end) & 15;
        NSan::Unpoison(begin, end - begin);
    }
    return Crc32cExtend(checksum, data, len);
}

#pragma pack(push, 1)
struct TTcpPacketHeader_v2 {
    ui64 Confirm;
    ui64 Serial;
    ui32 Checksum; // for the whole frame
    ui16 PayloadLength;
};
#pragma pack(pop)

struct TTcpPacketBuf {
    static constexpr ui64 PingRequestMask = 0x8000000000000000ULL;
    static constexpr ui64 PingResponseMask = 0x4000000000000000ULL;
    static constexpr ui64 ClockMask = 0x2000000000000000ULL;

    static constexpr size_t PacketDataLen = 4096 * 2 - 96 - sizeof(TTcpPacketHeader_v2);
};

struct TEventData {
    ui32 Type;
    ui32 Flags;
    TActorId Recipient;
    TActorId Sender;
    ui64 Cookie;
    NWilson::TTraceId TraceId;
    ui32 Checksum;
};

#pragma pack(push, 1)
struct TEventDescr2 {
    ui32 Type;
    ui32 Flags;
    TActorId Recipient;
    TActorId Sender;
    ui64 Cookie;
    NWilson::TTraceId::TSerializedTraceId TraceId;
    ui32 Checksum;
};
#pragma pack(pop)

struct TEventHolder : TNonCopyable {
    TEventData Descr;
    TActorId ForwardRecipient;
    THolder<IEventBase> Event;
    TIntrusivePtr<TEventSerializedData> Buffer;
    ui64 Serial;
    ui32 EventSerializedSize;
    ui32 EventActuallySerialized;
    mutable NLWTrace::TOrbit Orbit;
    NWilson::TSpan Span;

    ui32 Fill(IEventHandle& ev);

    void InitChecksum() {
        Descr.Checksum = 0;
    }

    void UpdateChecksum(const void *buffer, size_t len) {
        if (FORCE_EVENT_CHECKSUM) {
            Descr.Checksum = Crc32cExtendMSanCompatible(Descr.Checksum, buffer, len);
        }
    }

    void ForwardOnNondelivery(bool unsure) {
        TEventData& d = Descr;
        const TActorId& r = d.Recipient;
        const TActorId& s = d.Sender;
        const TActorId *f = ForwardRecipient ? &ForwardRecipient : nullptr;
        Span.EndError("nondelivery");
        auto ev = Event
            ? std::make_unique<IEventHandle>(r, s, Event.Release(), d.Flags, d.Cookie, f, Span.GetTraceId())
            : std::make_unique<IEventHandle>(d.Type, d.Flags, r, s, std::move(Buffer), d.Cookie, f, Span.GetTraceId());
        NActors::TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::move(ev), NActors::TEvents::TEvUndelivered::Disconnected, unsure));
    }

    void Clear() {
        Event.Reset();
        Buffer.Reset();
        Orbit.Reset();
        Span = {};
    }
};

namespace NActors {
    class TEventOutputChannel;
}

struct TTcpPacketOutTask : TNonCopyable {
    const TSessionParams& Params;
    NInterconnect::TOutgoingStream& OutgoingStream;
    NInterconnect::TOutgoingStream::TBookmark HeaderBookmark;
    size_t DataSize = 0;
    mutable NLWTrace::TOrbit Orbit;

    TTcpPacketOutTask(const TSessionParams& params, NInterconnect::TOutgoingStream& outgoingStream)
        : Params(params)
        , OutgoingStream(outgoingStream)
        , HeaderBookmark(OutgoingStream.Bookmark(sizeof(TTcpPacketHeader_v2)))
    {}

    // Preallocate some space to fill it later.
    NInterconnect::TOutgoingStream::TBookmark Bookmark(size_t len) {
        Y_VERIFY_DEBUG(len <= GetVirtualFreeAmount());
        DataSize += len;
        return OutgoingStream.Bookmark(len);
    }

    // Write previously bookmarked space.
    void WriteBookmark(NInterconnect::TOutgoingStream::TBookmark&& bookmark, const void *buffer, size_t len) {
        OutgoingStream.WriteBookmark(std::move(bookmark), {static_cast<const char*>(buffer), len});
    }

    // Acquire raw pointer to write some data.
    TMutableContiguousSpan AcquireSpanForWriting() {
        return OutgoingStream.AcquireSpanForWriting(GetVirtualFreeAmount());
    }

    // Append reference to some data (acquired previously or external pointer).
    void Append(const void *buffer, size_t len) {
        Y_VERIFY_DEBUG(len <= GetVirtualFreeAmount());
        DataSize += len;
        OutgoingStream.Append({static_cast<const char*>(buffer), len});
    }

    // Write some data with copying.
    void Write(const void *buffer, size_t len) {
        Y_VERIFY_DEBUG(len <= GetVirtualFreeAmount());
        DataSize += len;
        OutgoingStream.Write({static_cast<const char*>(buffer), len});
    }

    void Finish(ui64 serial, ui64 confirm) {
        Y_VERIFY(DataSize <= Max<ui16>());

        TTcpPacketHeader_v2 header{
            confirm,
            serial,
            0,
            static_cast<ui16>(DataSize)
        };

        if (Checksumming()) {
            // pre-write header without checksum for correct checksum calculation
            WriteBookmark(NInterconnect::TOutgoingStream::TBookmark(HeaderBookmark), &header, sizeof(header));

            size_t total = 0;
            ui32 checksum = 0;
            OutgoingStream.ScanLastBytes(GetFullSize(), [&](TContiguousSpan span) {
                checksum = Crc32cExtendMSanCompatible(checksum, span.data(), span.size());
                total += span.size();
            });
            header.Checksum = checksum;
            Y_VERIFY(total == sizeof(header) + DataSize, "total# %zu DataSize# %zu GetFullSize# %zu", total, DataSize,
                GetFullSize());
        }

        WriteBookmark(std::exchange(HeaderBookmark, {}), &header, sizeof(header));
    }

    bool Checksumming() const {
        return !Params.Encryption;
    }

    bool IsFull() const { return GetVirtualFreeAmount() == 0; }
    bool IsEmpty() const { return GetDataSize() == 0; }
    size_t GetDataSize() const { return DataSize; }
    size_t GetFullSize() const { return sizeof(TTcpPacketHeader_v2) + GetDataSize(); }
    size_t GetVirtualFreeAmount() const { return TTcpPacketBuf::PacketDataLen - DataSize; }
};
