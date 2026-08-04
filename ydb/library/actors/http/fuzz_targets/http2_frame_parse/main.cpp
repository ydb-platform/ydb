// Fuzzer for HTTP/2 frame header parsing.
// TFrameHeader::Parse reads 9 bytes of frame header from the wire.
// Every HTTP/2 connection starts with a stream of frames; this is the
// first parsing step on every incoming byte before any authentication.
// Also exercises TSettings::Apply with attacker-controlled SETTINGS values
// (integer overflows in window size / frame size checks).
#include <ydb/library/actors/http/http2_frames.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < NHttp::NHttp2::FRAME_HEADER_SIZE) return 0;
    if (size > 65536) return 0;

    try {
        // Parse the frame header
        NHttp::NHttp2::TFrameHeader hdr;
        hdr.Parse(reinterpret_cast<const char*>(data));

        // If this is a SETTINGS frame, simulate applying each 6-byte entry
        if (hdr.Type == NHttp::NHttp2::EFrameType::SETTINGS) {
            size_t payloadSize = size - NHttp::NHttp2::FRAME_HEADER_SIZE;
            const uint8_t* payload = data + NHttp::NHttp2::FRAME_HEADER_SIZE;
            NHttp::NHttp2::TSettings settings;
            for (size_t i = 0; i + 6 <= payloadSize; i += 6) {
                uint16_t id = (static_cast<uint16_t>(payload[i]) << 8) | payload[i + 1];
                uint32_t val = (static_cast<uint32_t>(payload[i+2]) << 24)
                             | (static_cast<uint32_t>(payload[i+3]) << 16)
                             | (static_cast<uint32_t>(payload[i+4]) << 8)
                             | static_cast<uint32_t>(payload[i+5]);
                settings.Apply(static_cast<NHttp::NHttp2::ESettingsId>(id), val);
            }
        }
    } catch (...) {}
    return 0;
}
