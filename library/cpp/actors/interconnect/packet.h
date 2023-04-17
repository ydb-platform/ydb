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

    TTcpPacketHeader_v2 Header;
    char Data[PacketDataLen];
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
    TTcpPacketBuf Packet;
    size_t DataSize;
    TStackVec<TConstIoVec, 32> Bufs;
    size_t BufferIndex;
    size_t FirstBufferOffset;
    bool TriedWriting;
    char *FreeArea;
    char *End;
    mutable NLWTrace::TOrbit Orbit;

public:
    TTcpPacketOutTask(const TSessionParams& params)
        : Params(params)
    {
        Reuse();
    }

    bool IsAtBegin() const {
        return !BufferIndex && !FirstBufferOffset && !TriedWriting;
    }

    void MarkTriedWriting() {
        TriedWriting = true;
    }

    void Reuse() {
        DataSize = 0;
        Bufs.assign(1, {&Packet.Header, sizeof(Packet.Header)});
        BufferIndex = 0;
        FirstBufferOffset = 0;
        TriedWriting = false;
        FreeArea = Packet.Data;
        End = FreeArea + TTcpPacketBuf::PacketDataLen;
        Orbit.Reset();
    }

    bool IsEmpty() const {
        return !DataSize;
    }

    void SetMetadata(ui64 serial, ui64 confirm) {
        Packet.Header.Serial = serial;
        Packet.Header.Confirm = confirm;
    }

    size_t GetDataSize() const { return DataSize; }

    ui64 GetSerial() const {
        return Packet.Header.Serial;
    }

    bool Confirmed(ui64 confirm) const {
        return IsEmpty() || Packet.Header.Serial <= confirm;
    }

    void *GetFreeArea() {
        return FreeArea;
    }

    size_t GetVirtualFreeAmount() const {
        return TTcpPacketBuf::PacketDataLen - DataSize;
    }

    void AppendBuf(const void *buf, size_t size) {
        DataSize += size;
        Y_VERIFY_DEBUG(DataSize <= TTcpPacketBuf::PacketDataLen, "DataSize# %zu AppendBuf buf# %p size# %zu"
            " FreeArea# %p End# %p", DataSize, buf, size, FreeArea, End);

        if (Bufs && static_cast<const char*>(Bufs.back().Data) + Bufs.back().Size == buf) {
            Bufs.back().Size += size;
        } else {
            Bufs.push_back({buf, size});
        }

        if (buf >= FreeArea && buf < End) {
            Y_VERIFY_DEBUG(buf == FreeArea);
            FreeArea = const_cast<char*>(static_cast<const char*>(buf)) + size;
            Y_VERIFY_DEBUG(FreeArea <= End);
        }
    }

    void Undo(size_t size) {
        Y_VERIFY(Bufs);
        auto& buf = Bufs.back();
        Y_VERIFY(buf.Data == FreeArea - buf.Size);
        buf.Size -= size;
        if (!buf.Size) {
            Bufs.pop_back();
        }
        FreeArea -= size;
        DataSize -= size;
    }

    bool DropBufs(size_t& amount) {
        while (BufferIndex != Bufs.size()) {
            TConstIoVec& item = Bufs[BufferIndex];
            // calculate number of bytes to the end in current buffer
            const size_t remain = item.Size - FirstBufferOffset;
            if (amount >= remain) {
                // vector item completely fits into the received amount, drop it out and switch to next buffer
                amount -= remain;
                ++BufferIndex;
                FirstBufferOffset = 0;
            } else {
                // adjust first buffer by "amount" bytes forward and reset amount to zero
                FirstBufferOffset += amount;
                amount = 0;
                // return false meaning that we have some more data to send
                return false;
            }
        }
        return true;
    }

    void ResetBufs() {
        BufferIndex = FirstBufferOffset = 0;
        TriedWriting = false;
    }

    template <typename TVectorType>
    void AppendToIoVector(TVectorType& vector, size_t max) {
        for (size_t k = BufferIndex, offset = FirstBufferOffset; k != Bufs.size() && vector.size() < max; ++k, offset = 0) {
            TConstIoVec v = Bufs[k];
            v.Data = static_cast<const char*>(v.Data) + offset;
            v.Size -= offset;
            vector.push_back(v);
        }
    }

    void Sign() {
        Packet.Header.Checksum = 0;
        Packet.Header.PayloadLength = DataSize;
        if (!Params.Encryption) {
            ui32 sum = 0;
            for (const auto& item : Bufs) {
                sum = Crc32cExtendMSanCompatible(sum, item.Data, item.Size);
            }
            Packet.Header.Checksum = sum;
        }
    }
};
