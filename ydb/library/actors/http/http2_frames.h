#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/byteorder.h>

#include <cstdint>
#include <cstring>

namespace NHttp::NHttp2 {

// HTTP/2 Connection Preface (RFC 7540 Section 3.5)
inline constexpr TStringBuf CONNECTION_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

// Frame header size is always 9 bytes (RFC 7540 Section 4.1)
inline constexpr size_t FRAME_HEADER_SIZE = 9;

// Default limits
inline constexpr uint32_t DEFAULT_HEADER_TABLE_SIZE = 4096;
inline constexpr uint32_t DEFAULT_MAX_CONCURRENT_STREAMS = 100;
inline constexpr uint32_t DEFAULT_INITIAL_WINDOW_SIZE = 65535;
inline constexpr uint32_t DEFAULT_MAX_FRAME_SIZE = 16384;
inline constexpr uint32_t DEFAULT_MAX_HEADER_LIST_SIZE = 8192;
inline constexpr uint32_t MAX_WINDOW_SIZE = 0x7FFFFFFF;
inline constexpr uint32_t MAX_MAX_FRAME_SIZE = 16777215; // 2^24-1

// Frame types (RFC 7540 Section 6)
enum class EFrameType : uint8_t {
    DATA          = 0x0,
    HEADERS       = 0x1,
    PRIORITY      = 0x2,
    RST_STREAM    = 0x3,
    SETTINGS      = 0x4,
    PUSH_PROMISE  = 0x5,
    PING          = 0x6,
    GOAWAY        = 0x7,
    WINDOW_UPDATE = 0x8,
    CONTINUATION  = 0x9,
};

// Frame flags
namespace NFrameFlag {
    inline constexpr uint8_t NONE         = 0x0;
    // DATA & HEADERS
    inline constexpr uint8_t END_STREAM   = 0x1;
    // HEADERS & CONTINUATION
    inline constexpr uint8_t END_HEADERS  = 0x4;
    // DATA
    inline constexpr uint8_t PADDED       = 0x8;
    // HEADERS
    inline constexpr uint8_t PRIORITY     = 0x20;
    // SETTINGS & PING
    inline constexpr uint8_t ACK          = 0x1;
}

// Settings identifiers (RFC 7540 Section 6.5.2)
enum class ESettingsId : uint16_t {
    HEADER_TABLE_SIZE      = 0x1,
    ENABLE_PUSH            = 0x2,
    MAX_CONCURRENT_STREAMS = 0x3,
    INITIAL_WINDOW_SIZE    = 0x4,
    MAX_FRAME_SIZE         = 0x5,
    MAX_HEADER_LIST_SIZE   = 0x6,
};

// Error codes (RFC 7540 Section 7)
enum class EErrorCode : uint32_t {
    SUCCESS             = 0x0,
    PROTOCOL_ERROR      = 0x1,
    INTERNAL_ERROR      = 0x2,
    FLOW_CONTROL_ERROR  = 0x3,
    SETTINGS_TIMEOUT    = 0x4,
    STREAM_CLOSED       = 0x5,
    FRAME_SIZE_ERROR    = 0x6,
    REFUSED_STREAM      = 0x7,
    CANCEL              = 0x8,
    COMPRESSION_ERROR   = 0x9,
    CONNECT_ERROR       = 0xa,
    ENHANCE_YOUR_CALM   = 0xb,
    INADEQUATE_SECURITY = 0xc,
    HTTP_1_1_REQUIRED   = 0xd,
};

// Stream states (RFC 7540 Section 5.1)
enum class EStreamState {
    Idle,
    ReservedLocal,
    ReservedRemote,
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
};

// Parsed frame header
struct TFrameHeader {
    uint32_t Length = 0;     // 24 bits
    EFrameType Type = EFrameType::DATA;
    uint8_t Flags = 0;
    uint32_t StreamId = 0;   // 31 bits (R bit ignored)

    bool HasFlag(uint8_t flag) const {
        return (Flags & flag) != 0;
    }

    // Parse from 9-byte buffer
    bool Parse(const char* data) {
        Length = (static_cast<uint8_t>(data[0]) << 16)
               | (static_cast<uint8_t>(data[1]) << 8)
               | static_cast<uint8_t>(data[2]);
        Type = static_cast<EFrameType>(data[3]);
        Flags = static_cast<uint8_t>(data[4]);
        StreamId = (static_cast<uint8_t>(data[5]) << 24)
                 | (static_cast<uint8_t>(data[6]) << 16)
                 | (static_cast<uint8_t>(data[7]) << 8)
                 | static_cast<uint8_t>(data[8]);
        StreamId &= 0x7FFFFFFF; // clear R bit
        return true;
    }

    // Serialize to 9-byte buffer
    void Serialize(char* out) const {
        out[0] = static_cast<char>((Length >> 16) & 0xFF);
        out[1] = static_cast<char>((Length >> 8) & 0xFF);
        out[2] = static_cast<char>(Length & 0xFF);
        out[3] = static_cast<char>(Type);
        out[4] = static_cast<char>(Flags);
        out[5] = static_cast<char>((StreamId >> 24) & 0x7F);
        out[6] = static_cast<char>((StreamId >> 16) & 0xFF);
        out[7] = static_cast<char>((StreamId >> 8) & 0xFF);
        out[8] = static_cast<char>(StreamId & 0xFF);
    }
};

// Settings entry
struct TSettingsEntry {
    ESettingsId Id;
    uint32_t Value;
};

// Connection settings
struct TSettings {
    uint32_t HeaderTableSize = DEFAULT_HEADER_TABLE_SIZE;
    bool EnablePush = true;
    uint32_t MaxConcurrentStreams = DEFAULT_MAX_CONCURRENT_STREAMS;
    uint32_t InitialWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;
    uint32_t MaxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    uint32_t MaxHeaderListSize = DEFAULT_MAX_HEADER_LIST_SIZE;

    bool Apply(ESettingsId id, uint32_t value) {
        switch (id) {
            case ESettingsId::HEADER_TABLE_SIZE:
                HeaderTableSize = value;
                return true;
            case ESettingsId::ENABLE_PUSH:
                if (value > 1) return false;
                EnablePush = (value == 1);
                return true;
            case ESettingsId::MAX_CONCURRENT_STREAMS:
                MaxConcurrentStreams = value;
                return true;
            case ESettingsId::INITIAL_WINDOW_SIZE:
                if (value > MAX_WINDOW_SIZE) return false;
                InitialWindowSize = value;
                return true;
            case ESettingsId::MAX_FRAME_SIZE:
                if (value < DEFAULT_MAX_FRAME_SIZE || value > MAX_MAX_FRAME_SIZE) return false;
                MaxFrameSize = value;
                return true;
            case ESettingsId::MAX_HEADER_LIST_SIZE:
                MaxHeaderListSize = value;
                return true;
        }
        // Unknown settings MUST be ignored (RFC 7540 Section 6.5.2)
        return true;
    }
};

// Helper to build frames
class TFrameBuilder {
public:
    // Build a SETTINGS frame
    static TString BuildSettings(uint32_t streamId, uint8_t flags, const TVector<TSettingsEntry>& entries) {
        TString result;
        size_t payloadSize = entries.size() * 6;
        result.resize(FRAME_HEADER_SIZE + payloadSize);
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = payloadSize;
        hdr.Type = EFrameType::SETTINGS;
        hdr.Flags = flags;
        hdr.StreamId = streamId;
        hdr.Serialize(out);

        char* payload = out + FRAME_HEADER_SIZE;
        for (const auto& entry : entries) {
            uint16_t id = static_cast<uint16_t>(entry.Id);
            payload[0] = static_cast<char>((id >> 8) & 0xFF);
            payload[1] = static_cast<char>(id & 0xFF);
            payload[2] = static_cast<char>((entry.Value >> 24) & 0xFF);
            payload[3] = static_cast<char>((entry.Value >> 16) & 0xFF);
            payload[4] = static_cast<char>((entry.Value >> 8) & 0xFF);
            payload[5] = static_cast<char>(entry.Value & 0xFF);
            payload += 6;
        }
        return result;
    }

    static TString BuildSettingsAck() {
        return BuildSettings(0, NFrameFlag::ACK, {});
    }

    // Build a HEADERS frame (header block fragment)
    static TString BuildHeaders(uint32_t streamId, uint8_t flags, TStringBuf headerBlock) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + headerBlock.size());
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = headerBlock.size();
        hdr.Type = EFrameType::HEADERS;
        hdr.Flags = flags;
        hdr.StreamId = streamId;
        hdr.Serialize(out);

        std::memcpy(out + FRAME_HEADER_SIZE, headerBlock.data(), headerBlock.size());
        return result;
    }

    // Build a DATA frame
    static TString BuildData(uint32_t streamId, uint8_t flags, TStringBuf data) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + data.size());
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = data.size();
        hdr.Type = EFrameType::DATA;
        hdr.Flags = flags;
        hdr.StreamId = streamId;
        hdr.Serialize(out);

        std::memcpy(out + FRAME_HEADER_SIZE, data.data(), data.size());
        return result;
    }

    // Build a WINDOW_UPDATE frame
    static TString BuildWindowUpdate(uint32_t streamId, uint32_t increment) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + 4);
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = 4;
        hdr.Type = EFrameType::WINDOW_UPDATE;
        hdr.Flags = NFrameFlag::NONE;
        hdr.StreamId = streamId;
        hdr.Serialize(out);

        char* payload = out + FRAME_HEADER_SIZE;
        payload[0] = static_cast<char>((increment >> 24) & 0x7F);
        payload[1] = static_cast<char>((increment >> 16) & 0xFF);
        payload[2] = static_cast<char>((increment >> 8) & 0xFF);
        payload[3] = static_cast<char>(increment & 0xFF);
        return result;
    }

    // Build a RST_STREAM frame
    static TString BuildRstStream(uint32_t streamId, EErrorCode errorCode) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + 4);
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = 4;
        hdr.Type = EFrameType::RST_STREAM;
        hdr.Flags = NFrameFlag::NONE;
        hdr.StreamId = streamId;
        hdr.Serialize(out);

        uint32_t code = static_cast<uint32_t>(errorCode);
        char* payload = out + FRAME_HEADER_SIZE;
        payload[0] = static_cast<char>((code >> 24) & 0xFF);
        payload[1] = static_cast<char>((code >> 16) & 0xFF);
        payload[2] = static_cast<char>((code >> 8) & 0xFF);
        payload[3] = static_cast<char>(code & 0xFF);
        return result;
    }

    // Build a GOAWAY frame
    static TString BuildGoaway(uint32_t lastStreamId, EErrorCode errorCode, TStringBuf debugData = {}) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + 8 + debugData.size());
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = 8 + debugData.size();
        hdr.Type = EFrameType::GOAWAY;
        hdr.Flags = NFrameFlag::NONE;
        hdr.StreamId = 0;
        hdr.Serialize(out);

        char* payload = out + FRAME_HEADER_SIZE;
        payload[0] = static_cast<char>((lastStreamId >> 24) & 0x7F);
        payload[1] = static_cast<char>((lastStreamId >> 16) & 0xFF);
        payload[2] = static_cast<char>((lastStreamId >> 8) & 0xFF);
        payload[3] = static_cast<char>(lastStreamId & 0xFF);

        uint32_t code = static_cast<uint32_t>(errorCode);
        payload[4] = static_cast<char>((code >> 24) & 0xFF);
        payload[5] = static_cast<char>((code >> 16) & 0xFF);
        payload[6] = static_cast<char>((code >> 8) & 0xFF);
        payload[7] = static_cast<char>(code & 0xFF);

        if (!debugData.empty()) {
            std::memcpy(payload + 8, debugData.data(), debugData.size());
        }
        return result;
    }

    // Build a PING frame
    static TString BuildPing(uint8_t flags, const char opaqueData[8]) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + 8);
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = 8;
        hdr.Type = EFrameType::PING;
        hdr.Flags = flags;
        hdr.StreamId = 0;
        hdr.Serialize(out);

        std::memcpy(out + FRAME_HEADER_SIZE, opaqueData, 8);
        return result;
    }

    // Build a CONTINUATION frame
    static TString BuildContinuation(uint32_t streamId, uint8_t flags, TStringBuf headerBlock) {
        TString result;
        result.resize(FRAME_HEADER_SIZE + headerBlock.size());
        char* out = result.Detach();

        TFrameHeader hdr;
        hdr.Length = headerBlock.size();
        hdr.Type = EFrameType::CONTINUATION;
        hdr.Flags = flags;
        hdr.StreamId = streamId;
        hdr.Serialize(out);

        std::memcpy(out + FRAME_HEADER_SIZE, headerBlock.data(), headerBlock.size());
        return result;
    }
};

} // namespace NHttp::NHttp2
