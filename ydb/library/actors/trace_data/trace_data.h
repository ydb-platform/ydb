#pragma once

#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors::NTracing {

    enum class ETraceEventType : ui8 {
        SendLocal = 0,
        ReceiveLocal = 1,
        New = 2,
        Die = 3,
        ForwardLocal = 4,
    };

    struct TTraceEvent {
        ui64 Actor1;       // Sender LocalId (Send/Receive), ActorId (New/Die), NewHandlePtr (ForwardLocal)
        ui64 Actor2;       // Recipient LocalId (Send/Receive), 0 (New/Die), Recipient LocalId (ForwardLocal)
        ui64 HandlePtr;    // IEventHandle* (Send/Receive), old HandlePtr (ForwardLocal), 0 (New/Die)
        ui32 DeltaUs;      // microseconds since TTraceFileHeader::StartTimestampUs; saturates at UINT32_MAX (~71 min)
        ui32 Aux;          // MessageType index (Send/Receive), 0 (New/Die), Original message type (ForwardLocal)
        ui16 Extra;        // ActivityIndex (Send/Receive), 0 (New/Die), Forwarder ActivityIndex (ForwardLocal)
        ui8  Type;         // ETraceEventType
        ui8  Flags;        // Thread buffer index (0-127)
    } __attribute__((packed));

    static_assert(sizeof(TTraceEvent) == 36);

    constexpr ui32 TraceFileMagic = 0x41525459; // "YTRA" little-endian
    constexpr ui32 TraceFileVersion = 3;

    struct TTraceFileHeader {
        ui32 Magic = TraceFileMagic;
        ui32 Version = TraceFileVersion;
        ui32 NodeId = 0;
        ui32 HeaderSize = 0;
        ui64 EventCount = 0;
        ui64 StartTimestampUs = 0; // wall-clock microseconds at Start(); absolute time = StartTimestampUs + event.DeltaUs
    };

    static_assert(sizeof(TTraceFileHeader) == 32);

    using TActivityDict = TVector<std::pair<ui32, TString>>;
    using TEventNamesDict = THashMap<ui32, TString>;
    using TThreadPoolDict = TVector<std::pair<ui32, TString>>;

    struct TTraceChunk {
        ui64 StartTimestampUs = 0;
        TActivityDict ActivityDict;
        TEventNamesDict EventNamesDict;
        TThreadPoolDict ThreadPoolDict;
        TVector<TTraceEvent> Events;
    };

    TBuffer SerializeTrace(const TTraceChunk& chunk, ui32 nodeId);
    bool DeserializeTrace(const TBuffer& data, TTraceChunk& chunk, ui32& nodeId);

} // namespace NActors::NTracing
