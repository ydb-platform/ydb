#pragma once

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/lwtrace/shuttle.h>
#include <util/generic/string.h>
#include <util/generic/list.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

#include "types.h"
#include "outgoing_stream.h"

#ifndef FORCE_EVENT_CHECKSUM
#define FORCE_EVENT_CHECKSUM 0
#endif

// WARNING: turning this feature on will make protocol incompatible with ordinary Interconnect, use with caution
#define IC_FORCE_HARDENED_PACKET_CHECKS 0

#if IC_FORCE_HARDENED_PACKET_CHECKS
#undef FORCE_EVENT_CHECKSUM
#define FORCE_EVENT_CHECKSUM 1
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
#if IC_FORCE_HARDENED_PACKET_CHECKS
    ui32 Len;
#endif
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
#if IC_FORCE_HARDENED_PACKET_CHECKS
    ui32 Len;
#endif
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
    ui32 ZcTransferId; //id of zero copy transfer. In case of RDMA it is a place where some internal handle can be stored to identify events

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
    NInterconnect::TOutgoingStream& XdcStream;
    NInterconnect::TOutgoingStream::TBookmark HeaderBookmark;
    ui32 InternalSize = 0;
    ui32 ExternalSize = 0;

    ui32 PreBookmarkChecksum = 0;
    ui32 InternalChecksum = 0;
    ui32 InternalChecksumLen = 0;
    bool InsideBookmark = false;

    ui32 ExternalChecksum = 0;

    TTcpPacketOutTask(const TSessionParams& params, NInterconnect::TOutgoingStream& outgoingStream,
            NInterconnect::TOutgoingStream& xdcStream)
        : Params(params)
        , OutgoingStream(outgoingStream)
        , XdcStream(xdcStream)
        , HeaderBookmark(OutgoingStream.Bookmark(sizeof(TTcpPacketHeader_v2)))
    {}

    // Preallocate some space to fill it later.
    NInterconnect::TOutgoingStream::TBookmark Bookmark(size_t len) {
        if (ChecksummingCrc32c()) {
            Y_DEBUG_ABORT_UNLESS(!InsideBookmark);
            InsideBookmark = true;
            PreBookmarkChecksum = std::exchange(InternalChecksum, 0);
            InternalChecksumLen = 0;
        }
        Y_DEBUG_ABORT_UNLESS(len <= GetInternalFreeAmount());
        InternalSize += len;
        return OutgoingStream.Bookmark(len);
    }

    // Write previously bookmarked space.
    void WriteBookmark(NInterconnect::TOutgoingStream::TBookmark&& bookmark, const void *buffer, size_t len) {
        if (ChecksummingCrc32c()) {
            Y_DEBUG_ABORT_UNLESS(InsideBookmark);
            InsideBookmark = false;
            const ui32 bookmarkChecksum = Crc32cExtendMSanCompatible(PreBookmarkChecksum, buffer, len);
            InternalChecksum = Crc32cCombine(bookmarkChecksum, InternalChecksum, InternalChecksumLen);
        }
        OutgoingStream.WriteBookmark(std::move(bookmark), {static_cast<const char*>(buffer), len});
    }

    // Acquire raw pointer to write some data.
    template<bool External>
    TMutableContiguousSpan AcquireSpanForWriting() {
        if (External) {
            return XdcStream.AcquireSpanForWriting(GetExternalFreeAmount());
        } else {
            return OutgoingStream.AcquireSpanForWriting(GetInternalFreeAmount());
        }
    }

    // Append reference to some data (acquired previously or external pointer).
    template<bool External>
    void Append(const void *buffer, size_t len, ui32* const zcHandle) {
        Y_DEBUG_ABORT_UNLESS(len <= (External ? GetExternalFreeAmount() : GetInternalFreeAmount()));
        (External ? ExternalSize : InternalSize) += len;
        (External ? XdcStream : OutgoingStream).Append({static_cast<const char*>(buffer), len}, zcHandle);
        ProcessChecksum<External>(buffer, len);
    }

    // Write some data with copying.
    template<bool External>
    void Write(const void *buffer, size_t len) {
        Y_DEBUG_ABORT_UNLESS(len <= (External ? GetExternalFreeAmount() : GetInternalFreeAmount()));
        (External ? ExternalSize : InternalSize) += len;
        (External ? XdcStream : OutgoingStream).Write({static_cast<const char*>(buffer), len});
        ProcessChecksum<External>(buffer, len);
    }

    template<bool External>
    void ProcessChecksum(const void *buffer, size_t len) {
        if (ChecksummingCrc32c()) {
            if (External) {
                ExternalChecksum = Crc32cExtendMSanCompatible(ExternalChecksum, buffer, len);
            } else {
                InternalChecksum = Crc32cExtendMSanCompatible(InternalChecksum, buffer, len);
                InternalChecksumLen += len;
            }
        }
    }

    void Finish(ui64 serial, ui64 confirm) {
        Y_ABORT_UNLESS(InternalSize <= Max<ui16>());

        TTcpPacketHeader_v2 header{
            confirm,
            serial,
            0,
            static_cast<ui16>(InternalSize)
        };

        if (ChecksummingXxhash()) {
            // write header with zero checksum to calculate whole packet checksum correctly
            OutgoingStream.WriteBookmark(NInterconnect::TOutgoingStream::TBookmark(HeaderBookmark),
                {reinterpret_cast<const char*>(&header), sizeof(header)});

            // calculate packet checksum
            XXH3_state_t state;
            XXH3_64bits_reset(&state);
            OutgoingStream.ScanLastBytes(GetPacketSize(), [&state](TContiguousSpan span) {
                XXH3_64bits_update(&state, span.data(), span.size());
            });
            header.Checksum = XXH3_64bits_digest(&state);
        } else if (ChecksummingCrc32c()) {
            Y_DEBUG_ABORT_UNLESS(!InsideBookmark);
            const ui32 headerChecksum = Crc32cExtendMSanCompatible(0, &header, sizeof(header));
            header.Checksum = Crc32cCombine(headerChecksum, InternalChecksum, InternalSize);
        }

        OutgoingStream.WriteBookmark(std::exchange(HeaderBookmark, {}), {reinterpret_cast<const char*>(&header),
            sizeof(header)});
    }

    bool ChecksummingCrc32c() const {
        return !Params.Encryption && !Params.UseXxhash;
    }

    bool ChecksummingXxhash() const {
        return !Params.Encryption && Params.UseXxhash;
    }

    bool IsEmpty() const { return GetDataSize() == 0; }
    ui32 GetDataSize() const { return InternalSize + ExternalSize; }
    ui32 GetPacketSize() const { return sizeof(TTcpPacketHeader_v2) + InternalSize; }
    ui32 GetInternalFreeAmount() const { return TTcpPacketBuf::PacketDataLen - InternalSize; }
    ui32 GetExternalFreeAmount() const { return 16384 - ExternalSize; }
    ui32 GetExternalSize() const { return ExternalSize; }
};

namespace NInterconnect::NDetail {
    static constexpr size_t MaxNumberBytes = (sizeof(ui64) * CHAR_BIT + 6) / 7;

    inline size_t SerializeNumber(ui64 num, char *buffer) {
        char *begin = buffer;
        do {
            *buffer++ = (num & 0x7F) | (num >= 128 ? 0x80 : 0x00);
            num >>= 7;
        } while (num);
        return buffer - begin;
    }

    inline ui64 DeserializeNumber(const char **ptr, const char *end) {
        const char *p = *ptr;
        size_t res = 0;
        size_t offset = 0;
        for (;;) {
            if (p == end) {
                return Max<ui64>();
            }
            const char byte = *p++;
            res |= (static_cast<size_t>(byte) & 0x7F) << offset;
            offset += 7;
            if (!(byte & 0x80)) {
                break;
            }
        }
        *ptr = p;
        return res;
    }
}
