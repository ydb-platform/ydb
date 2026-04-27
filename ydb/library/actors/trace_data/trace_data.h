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
        ui64 Sender;         // Send/Receive: sender LocalId; New/Die: actor's own LocalId; Forward: lower 32 bits hold new HandleHash, upper 32 bits are 0
        ui64 Recipient;      // Send/Receive/Forward: recipient LocalId; New/Die: 0
        ui32 HandleHash;     // Send/Receive: XOR-fold of IEventHandle*; Forward: hash of old handle; New/Die: 0
        ui32 DeltaUs;        // microseconds since TTraceFileHeader::StartTimestampUs; saturates at UINT32_MAX (~71 min)
        ui32 MessageType;    // Send/Receive: event type id; Forward: original event type id; New/Die: 0
        ui16 ActivityIndex;  // Send/Receive: ActivityType index; Forward: forwarder's ActivityIndex; New/Die: 0
        ui8  Type;           // ETraceEventType
        ui8  ThreadIdx;      // thread buffer index (0..127)
    };

    static_assert(sizeof(TTraceEvent) == 32);

    inline ui32 HashHandlePointer(ui64 ptr) {
        return static_cast<ui32>(ptr) ^ static_cast<ui32>(ptr >> 32);
    }

    constexpr ui32 TraceFileMagic = 0x41525459; // "YTRA" little-endian
    constexpr ui32 TraceFileVersion = 4;

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
