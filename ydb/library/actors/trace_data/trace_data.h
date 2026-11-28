#pragma once

#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cstddef>
#include <type_traits>

namespace NActors::NTracing {

    enum class ETraceEventType : ui8 {
        SendLocal = 0,
        ReceiveLocal = 1,
        New = 2,
        Die = 3,
        ForwardLocal = 4,
    };

    struct TTraceEvent {
        // Send/Receive: sender LocalId; New/Die: actor's own LocalId;
        // Forward: lower 32 bits hold the new HandleHash, upper 32 bits are 0.
        ui64 Sender;
        // Send/Receive/Forward: recipient LocalId; New/Die: 0.
        ui64 Recipient;
        // Send/Receive: XOR-fold of IEventHandle*; Forward: hash of the old handle;
        // New/Die: 0. Together with MessageType, provides best-effort correlation within NodeId.
        ui32 HandleHash;
        // Microseconds since TTraceFileHeader::StartTimestampUs; saturates at UINT32_MAX (~71 min).
        ui32 DeltaUs;
        // Send/Receive: event type id; Forward: original event type id; New/Die: 0.
        ui32 MessageType;
        // Send/Receive: ActivityType index; New: actor's ActivityType index;
        // Forward: forwarder's ActivityIndex; Die: 0.
        ui16 ActivityIndex;
        ui8 Type;       // ETraceEventType
        ui8 ThreadIdx;  // thread buffer index (0..127)
    };

    static_assert(sizeof(TTraceEvent) == 32);
    static_assert(std::is_standard_layout_v<TTraceEvent>);
    static_assert(std::is_trivially_copyable_v<TTraceEvent>);
    static_assert(offsetof(TTraceEvent, Sender) == 0);
    static_assert(offsetof(TTraceEvent, Recipient) == 8);
    static_assert(offsetof(TTraceEvent, HandleHash) == 16);
    static_assert(offsetof(TTraceEvent, DeltaUs) == 20);
    static_assert(offsetof(TTraceEvent, MessageType) == 24);
    static_assert(offsetof(TTraceEvent, ActivityIndex) == 28);
    static_assert(offsetof(TTraceEvent, Type) == 30);
    static_assert(offsetof(TTraceEvent, ThreadIdx) == 31);

    inline ui32 HashHandlePointer(ui64 ptr) {
        return static_cast<ui32>(ptr) ^ static_cast<ui32>(ptr >> 32);
    }

    constexpr ui32 TraceFileMagic = 0x41525459; // "YTRA" little-endian
    constexpr ui32 TraceFileVersion = 1;

    struct TTraceFileHeader {
        ui32 Magic = TraceFileMagic;
        ui32 Version = TraceFileVersion;
        ui32 NodeId = 0;
        ui32 MetadataSize = 0;
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
