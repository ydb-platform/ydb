#include "http2_frames.h"
#include "http2_hpack.h"
#include "http2.h"
#include "http_proxy.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <thread>
#include <atomic>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace NHttp::NHttp2;

namespace {

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(Http2Frames) {
    Y_UNIT_TEST(FrameHeaderParseSerializeRoundtrip) {
        TFrameHeader hdr;
        hdr.Length = 0x123456 & 0xFFFFFF; // 24-bit
        hdr.Type = EFrameType::HEADERS;
        hdr.Flags = NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM;
        hdr.StreamId = 7;

        char buf[FRAME_HEADER_SIZE];
        hdr.Serialize(buf);

        TFrameHeader parsed;
        parsed.Parse(buf);

        UNIT_ASSERT_VALUES_EQUAL(parsed.Length, hdr.Length);
        UNIT_ASSERT_EQUAL(parsed.Type, EFrameType::HEADERS);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Flags, hdr.Flags);
        UNIT_ASSERT_VALUES_EQUAL(parsed.StreamId, 7u);
    }

    Y_UNIT_TEST(FrameHeaderRBitCleared) {
        // R bit (highest bit of stream ID) should be masked off
        char buf[FRAME_HEADER_SIZE] = {};
        buf[5] = static_cast<char>(0xFF); // set R bit + high bits
        buf[6] = static_cast<char>(0xFF);
        buf[7] = static_cast<char>(0xFF);
        buf[8] = static_cast<char>(0xFF);

        TFrameHeader hdr;
        hdr.Parse(buf);
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 0x7FFFFFFFu);
    }

    Y_UNIT_TEST(FrameHeaderZeroValues) {
        TFrameHeader hdr;
        hdr.Length = 0;
        hdr.Type = EFrameType::DATA;
        hdr.Flags = 0;
        hdr.StreamId = 0;

        char buf[FRAME_HEADER_SIZE];
        hdr.Serialize(buf);

        TFrameHeader parsed;
        parsed.Parse(buf);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Length, 0u);
        UNIT_ASSERT_EQUAL(parsed.Type, EFrameType::DATA);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Flags, 0u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.StreamId, 0u);
    }

    Y_UNIT_TEST(FrameHeaderMaxValues) {
        TFrameHeader hdr;
        hdr.Length = 0xFFFFFF; // max 24-bit
        hdr.Type = EFrameType::CONTINUATION;
        hdr.Flags = 0xFF;
        hdr.StreamId = 0x7FFFFFFF; // max 31-bit

        char buf[FRAME_HEADER_SIZE];
        hdr.Serialize(buf);

        TFrameHeader parsed;
        parsed.Parse(buf);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Length, 0xFFFFFFu);
        UNIT_ASSERT_EQUAL(parsed.Type, EFrameType::CONTINUATION);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Flags, 0xFFu);
        UNIT_ASSERT_VALUES_EQUAL(parsed.StreamId, 0x7FFFFFFFu);
    }

    Y_UNIT_TEST(HasFlag) {
        TFrameHeader hdr;
        hdr.Flags = NFrameFlag::END_STREAM | NFrameFlag::END_HEADERS;
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::END_STREAM));
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::END_HEADERS));
        UNIT_ASSERT(!hdr.HasFlag(NFrameFlag::PADDED));
        UNIT_ASSERT(!hdr.HasFlag(NFrameFlag::PRIORITY));
    }
}

Y_UNIT_TEST_SUITE(Http2Settings) {
    Y_UNIT_TEST(DefaultValues) {
        TSettings s;
        UNIT_ASSERT_VALUES_EQUAL(s.HeaderTableSize, DEFAULT_HEADER_TABLE_SIZE);
        UNIT_ASSERT(s.EnablePush);
        UNIT_ASSERT_VALUES_EQUAL(s.MaxConcurrentStreams, DEFAULT_MAX_CONCURRENT_STREAMS);
        UNIT_ASSERT_VALUES_EQUAL(s.InitialWindowSize, DEFAULT_INITIAL_WINDOW_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(s.MaxFrameSize, DEFAULT_MAX_FRAME_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(s.MaxHeaderListSize, DEFAULT_MAX_HEADER_LIST_SIZE);
    }

    Y_UNIT_TEST(ApplyValidSettings) {
        TSettings s;
        UNIT_ASSERT(s.Apply(ESettingsId::HEADER_TABLE_SIZE, 8192));
        UNIT_ASSERT_VALUES_EQUAL(s.HeaderTableSize, 8192u);

        UNIT_ASSERT(s.Apply(ESettingsId::ENABLE_PUSH, 0));
        UNIT_ASSERT(!s.EnablePush);
        UNIT_ASSERT(s.Apply(ESettingsId::ENABLE_PUSH, 1));
        UNIT_ASSERT(s.EnablePush);

        UNIT_ASSERT(s.Apply(ESettingsId::MAX_CONCURRENT_STREAMS, 200));
        UNIT_ASSERT_VALUES_EQUAL(s.MaxConcurrentStreams, 200u);

        UNIT_ASSERT(s.Apply(ESettingsId::INITIAL_WINDOW_SIZE, 32768));
        UNIT_ASSERT_VALUES_EQUAL(s.InitialWindowSize, 32768u);

        UNIT_ASSERT(s.Apply(ESettingsId::MAX_FRAME_SIZE, 32768));
        UNIT_ASSERT_VALUES_EQUAL(s.MaxFrameSize, 32768u);

        UNIT_ASSERT(s.Apply(ESettingsId::MAX_HEADER_LIST_SIZE, 16384));
        UNIT_ASSERT_VALUES_EQUAL(s.MaxHeaderListSize, 16384u);
    }

    Y_UNIT_TEST(ApplyInvalidSettings) {
        TSettings s;
        // ENABLE_PUSH > 1 is invalid
        UNIT_ASSERT(!s.Apply(ESettingsId::ENABLE_PUSH, 2));

        // INITIAL_WINDOW_SIZE > MAX_WINDOW_SIZE is invalid
        UNIT_ASSERT(!s.Apply(ESettingsId::INITIAL_WINDOW_SIZE, MAX_WINDOW_SIZE + 1));

        // MAX_FRAME_SIZE < DEFAULT_MAX_FRAME_SIZE is invalid
        UNIT_ASSERT(!s.Apply(ESettingsId::MAX_FRAME_SIZE, DEFAULT_MAX_FRAME_SIZE - 1));
        // MAX_FRAME_SIZE > MAX_MAX_FRAME_SIZE is invalid
        UNIT_ASSERT(!s.Apply(ESettingsId::MAX_FRAME_SIZE, MAX_MAX_FRAME_SIZE + 1));
    }

    Y_UNIT_TEST(UnknownSettingsIgnored) {
        TSettings s;
        // Unknown settings must be ignored (return true)
        UNIT_ASSERT(s.Apply(static_cast<ESettingsId>(0xFF), 42));
    }
}

Y_UNIT_TEST_SUITE(Http2FrameBuilder) {
    Y_UNIT_TEST(BuildSettingsAck) {
        TString frame = TFrameBuilder::BuildSettingsAck();
        UNIT_ASSERT_VALUES_EQUAL(frame.size(), FRAME_HEADER_SIZE);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 0u);
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::SETTINGS);
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::ACK));
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 0u);
    }

    Y_UNIT_TEST(BuildSettings) {
        TVector<TSettingsEntry> entries = {
            {ESettingsId::MAX_CONCURRENT_STREAMS, 128},
            {ESettingsId::INITIAL_WINDOW_SIZE, 32768},
        };
        TString frame = TFrameBuilder::BuildSettings(0, NFrameFlag::NONE, entries);
        UNIT_ASSERT_VALUES_EQUAL(frame.size(), FRAME_HEADER_SIZE + 12);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 12u);
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::SETTINGS);
        UNIT_ASSERT_VALUES_EQUAL(hdr.Flags, NFrameFlag::NONE);

        // Parse first entry
        const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.data() + FRAME_HEADER_SIZE);
        uint16_t id1 = (p[0] << 8) | p[1];
        uint32_t val1 = (p[2] << 24) | (p[3] << 16) | (p[4] << 8) | p[5];
        UNIT_ASSERT_VALUES_EQUAL(id1, static_cast<uint16_t>(ESettingsId::MAX_CONCURRENT_STREAMS));
        UNIT_ASSERT_VALUES_EQUAL(val1, 128u);

        // Parse second entry
        uint16_t id2 = (p[6] << 8) | p[7];
        uint32_t val2 = (p[8] << 24) | (p[9] << 16) | (p[10] << 8) | p[11];
        UNIT_ASSERT_VALUES_EQUAL(id2, static_cast<uint16_t>(ESettingsId::INITIAL_WINDOW_SIZE));
        UNIT_ASSERT_VALUES_EQUAL(val2, 32768u);
    }

    Y_UNIT_TEST(BuildHeaders) {
        TString headerBlock = "test-header-block";
        TString frame = TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, headerBlock);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, headerBlock.size());
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::HEADERS);
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::END_HEADERS));
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::END_STREAM));
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 1u);

        TStringBuf payload(frame.data() + FRAME_HEADER_SIZE, hdr.Length);
        UNIT_ASSERT_VALUES_EQUAL(payload, headerBlock);
    }

    Y_UNIT_TEST(BuildData) {
        TString data = "Hello, HTTP/2!";
        TString frame = TFrameBuilder::BuildData(3, NFrameFlag::END_STREAM, data);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, data.size());
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::DATA);
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::END_STREAM));
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 3u);

        TStringBuf payload(frame.data() + FRAME_HEADER_SIZE, hdr.Length);
        UNIT_ASSERT_VALUES_EQUAL(payload, data);
    }

    Y_UNIT_TEST(BuildWindowUpdate) {
        TString frame = TFrameBuilder::BuildWindowUpdate(5, 65535);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 4u);
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::WINDOW_UPDATE);
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 5u);

        const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.data() + FRAME_HEADER_SIZE);
        uint32_t increment = (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
        increment &= 0x7FFFFFFF;
        UNIT_ASSERT_VALUES_EQUAL(increment, 65535u);
    }

    Y_UNIT_TEST(BuildRstStream) {
        TString frame = TFrameBuilder::BuildRstStream(1, EErrorCode::CANCEL);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 4u);
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::RST_STREAM);
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 1u);

        const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.data() + FRAME_HEADER_SIZE);
        uint32_t errorCode = (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
        UNIT_ASSERT_VALUES_EQUAL(errorCode, static_cast<uint32_t>(EErrorCode::CANCEL));
    }

    Y_UNIT_TEST(BuildGoaway) {
        TString debug = "test error";
        TString frame = TFrameBuilder::BuildGoaway(3, EErrorCode::SUCCESS, debug);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 8 + debug.size());
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::GOAWAY);
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 0u);

        const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.data() + FRAME_HEADER_SIZE);
        uint32_t lastStreamId = ((p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]) & 0x7FFFFFFF;
        UNIT_ASSERT_VALUES_EQUAL(lastStreamId, 3u);
        uint32_t errorCode = (p[4] << 24) | (p[5] << 16) | (p[6] << 8) | p[7];
        UNIT_ASSERT_VALUES_EQUAL(errorCode, static_cast<uint32_t>(EErrorCode::SUCCESS));

        TStringBuf debugPayload(frame.data() + FRAME_HEADER_SIZE + 8, debug.size());
        UNIT_ASSERT_VALUES_EQUAL(debugPayload, debug);
    }

    Y_UNIT_TEST(BuildGoawayNoDebug) {
        TString frame = TFrameBuilder::BuildGoaway(0, EErrorCode::PROTOCOL_ERROR);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 8u);
    }

    Y_UNIT_TEST(BuildPing) {
        char opaqueData[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        TString frame = TFrameBuilder::BuildPing(NFrameFlag::NONE, opaqueData);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, 8u);
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::PING);
        UNIT_ASSERT_VALUES_EQUAL(hdr.Flags, NFrameFlag::NONE);
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 0u);

        UNIT_ASSERT(std::memcmp(frame.data() + FRAME_HEADER_SIZE, opaqueData, 8) == 0);
    }

    Y_UNIT_TEST(BuildPingAck) {
        char opaqueData[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        TString frame = TFrameBuilder::BuildPing(NFrameFlag::ACK, opaqueData);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::ACK));
    }

    Y_UNIT_TEST(BuildContinuation) {
        TString headerBlock = "continuation-data";
        TString frame = TFrameBuilder::BuildContinuation(1, NFrameFlag::END_HEADERS, headerBlock);

        TFrameHeader hdr;
        hdr.Parse(frame.data());
        UNIT_ASSERT_VALUES_EQUAL(hdr.Length, headerBlock.size());
        UNIT_ASSERT_EQUAL(hdr.Type, EFrameType::CONTINUATION);
        UNIT_ASSERT(hdr.HasFlag(NFrameFlag::END_HEADERS));
        UNIT_ASSERT_VALUES_EQUAL(hdr.StreamId, 1u);

        TStringBuf payload(frame.data() + FRAME_HEADER_SIZE, hdr.Length);
        UNIT_ASSERT_VALUES_EQUAL(payload, headerBlock);
    }
}

Y_UNIT_TEST_SUITE(Http2Hpack) {
    Y_UNIT_TEST(HuffmanEncodeDecodeRoundtrip) {
        // Test Huffman through the encoder/decoder roundtrip
        // Encode headers with values that will use Huffman coding
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {"x-test", "www.example.com"},
            {"x-path", "/index.html"},
            {"x-custom", "custom-value"},
        };

        TString encoded = encoder.Encode(headers);
        UNIT_ASSERT(!encoded.empty());

        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), headers.size());

        for (size_t i = 0; i < headers.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(decoded[i].first, headers[i].first);
            UNIT_ASSERT_VALUES_EQUAL(decoded[i].second, headers[i].second);
        }
    }

    Y_UNIT_TEST(HuffmanEncodeDecodeEmpty) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {"x-empty", ""},
        };

        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(decoded[0].second, "");
    }

    Y_UNIT_TEST(HuffmanEncodeDecodeNumbers) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {":status", "200"},
        };

        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(decoded[0].second, "200");
    }

    Y_UNIT_TEST(DynamicTableAddAndGet) {
        THPackDynamicTable table(4096);
        table.Add("custom-header", "custom-value");

        TStringBuf name, value;
        UNIT_ASSERT(table.Get(1, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, "custom-header");
        UNIT_ASSERT_VALUES_EQUAL(value, "custom-value");
        UNIT_ASSERT_VALUES_EQUAL(table.Size(), 1u);
    }

    Y_UNIT_TEST(DynamicTableFIFO) {
        THPackDynamicTable table(4096);
        table.Add("header-1", "value-1");
        table.Add("header-2", "value-2");

        // Most recently added is index 1
        TStringBuf name, value;
        UNIT_ASSERT(table.Get(1, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, "header-2");

        UNIT_ASSERT(table.Get(2, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, "header-1");
    }

    Y_UNIT_TEST(DynamicTableEviction) {
        // Each entry has 32 bytes overhead + name.size() + value.size()
        // "a" + "b" + 32 = 34 bytes. Set max to allow only 1 entry.
        THPackDynamicTable table(34);
        table.Add("a", "b"); // 34 bytes
        UNIT_ASSERT_VALUES_EQUAL(table.Size(), 1u);

        table.Add("c", "d"); // evicts first, adds new
        UNIT_ASSERT_VALUES_EQUAL(table.Size(), 1u);

        TStringBuf name, value;
        UNIT_ASSERT(table.Get(1, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, "c");
    }

    Y_UNIT_TEST(DynamicTableOversizeEntry) {
        // Entry larger than max table size should empty the table and not be added
        THPackDynamicTable table(33); // too small for any entry (32 overhead + at least 1 byte)
        table.Add("a", "b"); // 34 bytes > 33, not added
        UNIT_ASSERT_VALUES_EQUAL(table.Size(), 0u);
    }

    Y_UNIT_TEST(DynamicTableSetMaxSize) {
        THPackDynamicTable table(4096);
        table.Add("header-1", "value-1"); // 32 + 8 + 7 = 47
        table.Add("header-2", "value-2"); // 47
        UNIT_ASSERT_VALUES_EQUAL(table.Size(), 2u);

        // Shrink to fit only 1 entry
        table.SetMaxSize(50);
        UNIT_ASSERT_VALUES_EQUAL(table.Size(), 1u);

        TStringBuf name, value;
        UNIT_ASSERT(table.Get(1, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, "header-2"); // most recent survives
    }

    Y_UNIT_TEST(DynamicTableOutOfBounds) {
        THPackDynamicTable table(4096);
        table.Add("a", "b");

        TStringBuf name, value;
        UNIT_ASSERT(!table.Get(0, name, value)); // 0 is invalid (1-based)
        UNIT_ASSERT(!table.Get(2, name, value)); // only 1 entry
    }

    Y_UNIT_TEST(EncoderDecoderRoundtripSimple) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {":method", "GET"},
            {":path", "/"},
            {":scheme", "https"},
            {":authority", "example.com"},
            {"accept", "text/html"},
        };

        TString encoded = encoder.Encode(headers);
        UNIT_ASSERT(!encoded.empty());

        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), headers.size());

        for (size_t i = 0; i < headers.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(decoded[i].first, headers[i].first);
            UNIT_ASSERT_VALUES_EQUAL(decoded[i].second, headers[i].second);
        }
    }

    Y_UNIT_TEST(EncoderDecoderRoundtripCustomHeaders) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {":status", "200"},
            {"content-type", "application/json"},
            {"x-custom-header", "custom-value"},
            {"x-request-id", "12345-abcde"},
        };

        TString encoded = encoder.Encode(headers);

        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), headers.size());

        for (size_t i = 0; i < headers.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(decoded[i].first, headers[i].first);
            UNIT_ASSERT_VALUES_EQUAL(decoded[i].second, headers[i].second);
        }
    }

    Y_UNIT_TEST(EncoderDecoderMultipleEncodes) {
        // Verify dynamic table state evolves correctly across multiple encodes
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers1 = {
            {":method", "GET"},
            {":path", "/api/v1"},
            {":scheme", "https"},
            {":authority", "example.com"},
        };

        TString encoded1 = encoder.Encode(headers1);
        TVector<std::pair<TString, TString>> decoded1;
        UNIT_ASSERT(decoder.Decode(encoded1, decoded1));
        UNIT_ASSERT_VALUES_EQUAL(decoded1.size(), headers1.size());

        // Second request with same authority (should reference dynamic table)
        TVector<std::pair<TString, TString>> headers2 = {
            {":method", "POST"},
            {":path", "/api/v2"},
            {":scheme", "https"},
            {":authority", "example.com"},
            {"content-type", "application/json"},
        };

        TString encoded2 = encoder.Encode(headers2);
        TVector<std::pair<TString, TString>> decoded2;
        UNIT_ASSERT(decoder.Decode(encoded2, decoded2));
        UNIT_ASSERT_VALUES_EQUAL(decoded2.size(), headers2.size());

        for (size_t i = 0; i < headers2.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(decoded2[i].first, headers2[i].first);
            UNIT_ASSERT_VALUES_EQUAL(decoded2[i].second, headers2[i].second);
        }

        // Second encode should be smaller due to dynamic table references
        UNIT_ASSERT_LE(encoded2.size(), encoded1.size() + 30); // approximate; custom header adds overhead
    }

    Y_UNIT_TEST(EncoderDecoderEmptyValue) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {":status", "204"},
            {"x-empty", ""},
        };

        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(decoded[1].first, "x-empty");
        UNIT_ASSERT_VALUES_EQUAL(decoded[1].second, "");
    }

    Y_UNIT_TEST(EncoderUsesStaticTableForKnownHeaders) {
        THPackEncoder encoder;

        // ":method GET" is static table entry 2 - should encode as single byte
        TVector<std::pair<TString, TString>> headers = {
            {":method", "GET"},
        };
        TString encoded = encoder.Encode(headers);
        // Indexed representation: single byte 0x82 (1_0000010)
        UNIT_ASSERT_VALUES_EQUAL(encoded.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<uint8_t>(encoded[0]), 0x82u);
    }

    Y_UNIT_TEST(DecoderRejectsInvalidIndex) {
        THPackDecoder decoder;
        // Indexed header with index 0 is invalid
        TString data;
        data.push_back(static_cast<char>(0x80)); // index 0 with indexed flag
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(!decoder.Decode(data, decoded));
    }

    Y_UNIT_TEST(EncoderSetMaxTableSize) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        encoder.SetMaxTableSize(0); // no dynamic table
        decoder.SetMaxTableSize(0);

        TVector<std::pair<TString, TString>> headers = {
            {":method", "GET"},
            {":path", "/test"},
            {"x-custom", "value"},
        };

        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), headers.size());
    }
}

Y_UNIT_TEST_SUITE(Http2Session) {
    // Helper to connect a client and server session together
    struct TSessionPair {
        TString ServerRecvBuf;
        TString ClientRecvBuf;
        TString ServerError;
        TString ClientError;

        struct TReceivedRequest {
            uint32_t StreamId;
            TString Method;
            TString Path;
            TString Authority;
            TString Body;
            TVector<std::pair<TString, TString>> Headers;
        };

        struct TReceivedResponse {
            uint32_t StreamId;
            TString Status;
            TString Body;
            TVector<std::pair<TString, TString>> Headers;
        };

        TVector<TReceivedRequest> ServerRequests;
        TVector<TReceivedResponse> ClientResponses;
        bool ServerGotGoaway = false;
        bool ClientGotGoaway = false;

        std::unique_ptr<TSession> Server;
        std::unique_ptr<TSession> Client;

        TSessionPair() {
            TSessionCallbacks serverCb;
            serverCb.OnRequest = [this](uint32_t streamId, TStream& stream) {
                TReceivedRequest req;
                req.StreamId = streamId;
                req.Method = stream.Method;
                req.Path = stream.Path;
                req.Authority = stream.Authority;
                req.Body = stream.Body;
                req.Headers = stream.Headers;
                ServerRequests.push_back(std::move(req));
            };
            serverCb.OnSend = [this](TString data) {
                ClientRecvBuf.append(data);
            };
            serverCb.OnError = [this](TString error) {
                ServerError = error;
            };
            serverCb.OnGoaway = [this](uint32_t, EErrorCode) {
                ServerGotGoaway = true;
            };

            TSessionCallbacks clientCb;
            clientCb.OnResponse = [this](uint32_t streamId, TStream& stream) {
                TReceivedResponse resp;
                resp.StreamId = streamId;
                resp.Status = stream.Status;
                resp.Body = stream.Body;
                resp.Headers = stream.Headers;
                ClientResponses.push_back(std::move(resp));
            };
            clientCb.OnSend = [this](TString data) {
                ServerRecvBuf.append(data);
            };
            clientCb.OnError = [this](TString error) {
                ClientError = error;
            };
            clientCb.OnGoaway = [this](uint32_t, EErrorCode) {
                ClientGotGoaway = true;
            };

            Server = std::make_unique<TSession>(TSession::ERole::Server, std::move(serverCb));
            Client = std::make_unique<TSession>(TSession::ERole::Client, std::move(clientCb));
        }

        void Initialize() {
            Client->Initialize();
            Server->Initialize();
            // Exchange init data
            Pump();
        }

        // Pump data between client and server until no more pending data
        void Pump() {
            for (int i = 0; i < 10; ++i) { // limit iterations to prevent infinite loop
                bool progress = false;
                if (!ServerRecvBuf.empty()) {
                    TString data;
                    data.swap(ServerRecvBuf);
                    Server->Feed(data.data(), data.size());
                    progress = true;
                }
                if (!ClientRecvBuf.empty()) {
                    TString data;
                    data.swap(ClientRecvBuf);
                    Client->Feed(data.data(), data.size());
                    progress = true;
                }
                if (!progress) break;
            }
        }
    };

    Y_UNIT_TEST(Handshake) {
        TSessionPair pair;
        pair.Initialize();

        UNIT_ASSERT(pair.Server->IsReady());
        UNIT_ASSERT(pair.Client->IsReady());
        UNIT_ASSERT(pair.ServerError.empty());
        UNIT_ASSERT(pair.ClientError.empty());
    }

    Y_UNIT_TEST(SimpleGetRequest) {
        TSessionPair pair;
        pair.Initialize();

        // Client sends GET request
        uint32_t streamId = pair.Client->SendRequest("GET", "/test", "example.com", "https", {}, "");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].StreamId, streamId);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Method, "GET");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Path, "/test");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Authority, "example.com");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Body, "");
    }

    Y_UNIT_TEST(SimplePostRequest) {
        TSessionPair pair;
        pair.Initialize();

        TString body = "Hello, World!";
        uint32_t streamId = pair.Client->SendRequest("POST", "/submit", "example.com", "https",
            {{"content-type", "text/plain"}}, body);
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].StreamId, streamId);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Method, "POST");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Path, "/submit");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Body, body);
    }

    Y_UNIT_TEST(RequestResponse) {
        TSessionPair pair;
        pair.Initialize();

        uint32_t streamId = pair.Client->SendRequest("GET", "/api", "example.com", "https", {}, "");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);

        // Server sends response
        TVector<std::pair<TString, TString>> respHeaders = {
            {"content-type", "application/json"},
        };
        TString respBody = R"({"status":"ok"})";
        pair.Server->SendResponse(streamId, "200", respHeaders, respBody);
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].StreamId, streamId);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Body, respBody);

        // Check response headers contain content-type
        bool foundContentType = false;
        for (const auto& [name, value] : pair.ClientResponses[0].Headers) {
            if (name == "content-type") {
                UNIT_ASSERT_VALUES_EQUAL(value, "application/json");
                foundContentType = true;
            }
        }
        UNIT_ASSERT(foundContentType);
    }

    Y_UNIT_TEST(EmptyBodyResponse) {
        TSessionPair pair;
        pair.Initialize();

        uint32_t streamId = pair.Client->SendRequest("GET", "/empty", "example.com", "https", {}, "");
        pair.Pump();

        pair.Server->SendResponse(streamId, "204", {}, "");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Status, "204");
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Body, "");
    }

    Y_UNIT_TEST(MultipleConcurrentStreams) {
        TSessionPair pair;
        pair.Initialize();

        uint32_t stream1 = pair.Client->SendRequest("GET", "/path1", "example.com", "https", {}, "");
        uint32_t stream2 = pair.Client->SendRequest("GET", "/path2", "example.com", "https", {}, "");
        uint32_t stream3 = pair.Client->SendRequest("POST", "/path3", "example.com", "https",
            {{"content-type", "text/plain"}}, "body3");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 3u);

        // Stream IDs should be odd and increasing (client-initiated)
        UNIT_ASSERT_VALUES_EQUAL(stream1, 1u);
        UNIT_ASSERT_VALUES_EQUAL(stream2, 3u);
        UNIT_ASSERT_VALUES_EQUAL(stream3, 5u);

        // Verify each request
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Path, "/path1");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[1].Path, "/path2");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[2].Path, "/path3");
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[2].Body, "body3");

        // Respond to all in different order
        pair.Server->SendResponse(stream2, "200", {}, "resp2");
        pair.Server->SendResponse(stream3, "201", {}, "resp3");
        pair.Server->SendResponse(stream1, "200", {}, "resp1");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses.size(), 3u);
        // Responses arrive in the order sent by server
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].StreamId, stream2);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Body, "resp2");
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[1].StreamId, stream3);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[1].Body, "resp3");
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[2].StreamId, stream1);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[2].Body, "resp1");
    }

    Y_UNIT_TEST(PingPong) {
        TSessionPair pair;
        pair.Initialize();

        // Client sends PING
        char pingData[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        TString pingFrame = TFrameBuilder::BuildPing(NFrameFlag::NONE, pingData);

        // Feed ping to server
        pair.ServerRecvBuf.append(pingFrame);
        pair.Pump();

        // Server should have sent PING ACK back (it's in ClientRecvBuf which was already pumped)
        // No errors
        UNIT_ASSERT(pair.ServerError.empty());
        UNIT_ASSERT(pair.ClientError.empty());
    }

    Y_UNIT_TEST(Goaway) {
        TSessionPair pair;
        pair.Initialize();

        pair.Client->SendGoaway(EErrorCode::SUCCESS, "shutting down");
        pair.Pump();

        UNIT_ASSERT(pair.Client->IsGoaway());
        UNIT_ASSERT(pair.ServerGotGoaway);
    }

    Y_UNIT_TEST(ServerGoaway) {
        TSessionPair pair;
        pair.Initialize();

        pair.Server->SendGoaway(EErrorCode::SUCCESS);
        pair.Pump();

        UNIT_ASSERT(pair.Server->IsGoaway());
        UNIT_ASSERT(pair.ClientGotGoaway);
    }

    Y_UNIT_TEST(GoawayDoubleCallNoop) {
        TSessionPair pair;
        pair.Initialize();

        pair.Client->SendGoaway(EErrorCode::SUCCESS);
        pair.Client->SendGoaway(EErrorCode::SUCCESS); // should be noop
        pair.Pump();

        UNIT_ASSERT(pair.Client->IsGoaway());
    }

    Y_UNIT_TEST(InvalidPreface) {
        TSessionCallbacks cb;
        TString error;
        cb.OnError = [&error](TString e) { error = e; };
        cb.OnSend = [](TString) {};

        TSession server(TSession::ERole::Server, std::move(cb));
        server.Initialize();

        // Feed invalid preface
        TString badPreface = "GET / HTTP/1.1\r\n\r\n";
        server.Feed(badPreface.data(), badPreface.size());

        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(IncrementalFeed) {
        // Verify that feeding data byte-by-byte works
        TSessionPair pair;
        pair.Initialize();

        uint32_t streamId = pair.Client->SendRequest("GET", "/incremental", "example.com", "https", {}, "");
        // Don't use Pump - manually feed byte by byte to server
        TString data;
        data.swap(pair.ServerRecvBuf);
        for (size_t i = 0; i < data.size(); ++i) {
            pair.Server->Feed(data.data() + i, 1);
        }
        // Also feed server responses to client
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Path, "/incremental");
        Y_UNUSED(streamId);
    }

    Y_UNIT_TEST(LargeBody) {
        TSessionPair pair;
        pair.Initialize();

        // Send a body larger than default frame size (16384 bytes)
        TString largeBody(32000, 'X');
        uint32_t streamId = pair.Client->SendRequest("POST", "/large", "example.com", "https",
            {{"content-type", "application/octet-stream"}}, largeBody);
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Body.size(), largeBody.size());
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Body, largeBody);

        // Server responds with large body
        TString largeResp(32000, 'Y');
        pair.Server->SendResponse(streamId, "200", {}, largeResp);
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Body, largeResp);
    }

    Y_UNIT_TEST(ManyHeaders) {
        TSessionPair pair;
        pair.Initialize();

        TVector<std::pair<TString, TString>> headers;
        for (int i = 0; i < 50; ++i) {
            headers.emplace_back("x-header-" + ToString(i), "value-" + ToString(i));
        }

        pair.Client->SendRequest("GET", "/headers", "example.com", "https", headers, "");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        // All custom headers should be present
        UNIT_ASSERT_GE(pair.ServerRequests[0].Headers.size(), 50u);
    }

    Y_UNIT_TEST(ClientStreamIdsOdd) {
        TSessionPair pair;
        pair.Initialize();

        for (int i = 0; i < 5; ++i) {
            uint32_t streamId = pair.Client->SendRequest("GET", "/", "example.com", "https", {}, "");
            UNIT_ASSERT(streamId % 2 == 1); // client streams are odd
        }
    }

    Y_UNIT_TEST(WindowUpdateOnLargeBody) {
        TSessionPair pair;
        pair.Initialize();

        // Send enough data to trigger WINDOW_UPDATE
        TString body(50000, 'Z');
        pair.Client->SendRequest("POST", "/big", "example.com", "https", {}, body);
        pair.Pump();

        // Should have received the full body
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Body.size(), 50000u);
        UNIT_ASSERT(pair.ServerError.empty());
    }

    Y_UNIT_TEST(SettingsExchange) {
        TSessionPair pair;
        pair.Initialize();

        // Both sides should be ready after settings exchange
        UNIT_ASSERT(pair.Server->IsReady());
        UNIT_ASSERT(pair.Client->IsReady());

        // Should be able to exchange requests after settings
        pair.Client->SendRequest("GET", "/after-settings", "example.com", "https", {}, "");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
    }

    Y_UNIT_TEST(RequestWithCustomHeaders) {
        TSessionPair pair;
        pair.Initialize();

        TVector<std::pair<TString, TString>> customHeaders = {
            {"authorization", "Bearer token123"},
            {"accept", "application/json"},
            {"x-request-id", "req-001"},
        };

        pair.Client->SendRequest("GET", "/api/data", "api.example.com", "https", customHeaders, "");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests.size(), 1u);
        const auto& req = pair.ServerRequests[0];
        UNIT_ASSERT_VALUES_EQUAL(req.Authority, "api.example.com");

        // Verify custom headers are present
        THashMap<TString, TString> headerMap;
        for (const auto& [name, value] : req.Headers) {
            headerMap[name] = value;
        }
        UNIT_ASSERT_VALUES_EQUAL(headerMap["authorization"], "Bearer token123");
        UNIT_ASSERT_VALUES_EQUAL(headerMap["accept"], "application/json");
        UNIT_ASSERT_VALUES_EQUAL(headerMap["x-request-id"], "req-001");
    }

    Y_UNIT_TEST(ResponseWithMultipleHeaders) {
        TSessionPair pair;
        pair.Initialize();

        uint32_t streamId = pair.Client->SendRequest("GET", "/multi-header", "example.com", "https", {}, "");
        pair.Pump();

        TVector<std::pair<TString, TString>> respHeaders = {
            {"content-type", "text/html"},
            {"cache-control", "no-cache"},
            {"x-powered-by", "ydb"},
            {"set-cookie", "session=abc123"},
        };

        pair.Server->SendResponse(streamId, "200", respHeaders, "<html></html>");
        pair.Pump();

        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses.size(), 1u);
        THashMap<TString, TString> headerMap;
        for (const auto& [name, value] : pair.ClientResponses[0].Headers) {
            headerMap[name] = value;
        }
        UNIT_ASSERT_VALUES_EQUAL(headerMap["content-type"], "text/html");
        UNIT_ASSERT_VALUES_EQUAL(headerMap["cache-control"], "no-cache");
        UNIT_ASSERT_VALUES_EQUAL(headerMap["x-powered-by"], "ydb");
        UNIT_ASSERT_VALUES_EQUAL(headerMap["set-cookie"], "session=abc123");
    }

    Y_UNIT_TEST(RstStreamOnConnection) {
        TSessionPair pair;
        pair.Initialize();

        // Directly feed RST_STREAM for stream 0 (should be protocol error)
        TString rstFrame = TFrameBuilder::BuildRstStream(0, EErrorCode::CANCEL);
        pair.ServerRecvBuf.append(rstFrame);
        pair.Pump();

        // Server should report error (RST_STREAM on stream 0 is protocol error)
        UNIT_ASSERT(!pair.ServerError.empty());
    }
}

Y_UNIT_TEST_SUITE(Http2ConversionHelpers) {
    Y_UNIT_TEST(StreamToIncomingRequestBasic) {
        TStream stream;
        stream.Method = "GET";
        stream.Path = "/test";
        stream.Scheme = "https";
        stream.Authority = "example.com";
        stream.Headers = {{"accept", "text/html"}};
        stream.Body = "";

        auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
        NHttp::THttpConfig::SocketAddressType address;
        auto request = StreamToIncomingRequest(stream, endpoint, address);
        UNIT_ASSERT(request);
        UNIT_ASSERT_VALUES_EQUAL(request->Method, "GET");
        UNIT_ASSERT_VALUES_EQUAL(request->URL, "/test");
    }

    Y_UNIT_TEST(StreamToIncomingRequestWithBody) {
        TStream stream;
        stream.Method = "POST";
        stream.Path = "/submit";
        stream.Scheme = "https";
        stream.Authority = "example.com";
        stream.Headers = {{"content-type", "application/json"}};
        stream.Body = R"({"key":"value"})";

        auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
        NHttp::THttpConfig::SocketAddressType address;
        auto request = StreamToIncomingRequest(stream, endpoint, address);
        UNIT_ASSERT(request);
        UNIT_ASSERT_VALUES_EQUAL(request->Method, "POST");
        UNIT_ASSERT_VALUES_EQUAL(request->Body, R"({"key":"value"})");
    }

    Y_UNIT_TEST(OutgoingResponseToStreamBasic) {
        NHttp::THttpIncomingRequestPtr inReq = new NHttp::THttpIncomingRequest();
        EatWholeString(inReq, "GET /test HTTP/1.1\r\nHost: test\r\n\r\n");

        NHttp::THttpOutgoingResponsePtr response = inReq->CreateResponseOK("hello", "text/plain");

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        UNIT_ASSERT_VALUES_EQUAL(status, "200");
        UNIT_ASSERT_VALUES_EQUAL(body, "hello");

        bool foundContentType = false;
        for (const auto& [name, value] : headers) {
            if (name == "content-type" || name == "Content-Type") {
                foundContentType = true;
            }
        }
        UNIT_ASSERT(foundContentType);
    }

    Y_UNIT_TEST(OutgoingResponseSkipsConnectionHeaders) {
        NHttp::THttpIncomingRequestPtr inReq = new NHttp::THttpIncomingRequest();
        EatWholeString(inReq, "GET /test HTTP/1.1\r\nHost: test\r\n\r\n");

        NHttp::THttpOutgoingResponsePtr response = inReq->CreateResponseOK("ok", "text/plain");

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        // connection, transfer-encoding, keep-alive should be filtered
        for (const auto& [name, _] : headers) {
            TString lowerName = name;
            lowerName.to_lower();
            UNIT_ASSERT_UNEQUAL(lowerName, "connection");
            UNIT_ASSERT_UNEQUAL(lowerName, "transfer-encoding");
            UNIT_ASSERT_UNEQUAL(lowerName, "keep-alive");
        }
    }

    Y_UNIT_TEST(OutgoingRequestToStreamBasic) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/api");

        TString method, path, authority, scheme;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingRequestToStream(request, method, path, authority, scheme, headers, body);

        UNIT_ASSERT_VALUES_EQUAL(method, "GET");
        UNIT_ASSERT_VALUES_EQUAL(path, "/api");
        UNIT_ASSERT_VALUES_EQUAL(authority, "example.com");
        UNIT_ASSERT_VALUES_EQUAL(scheme, "http");
    }

    Y_UNIT_TEST(StreamToIncomingResponseBasic) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");

        TStream stream;
        stream.Status = "200";
        stream.Headers = {
            {"content-type", "text/plain"},
            {"x-custom", "value"},
        };
        stream.Body = "response body";

        auto response = StreamToIncomingResponse(stream, request);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(response->Body, "response body");
    }

    Y_UNIT_TEST(StreamToIncomingResponseEmptyBody) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/empty");

        TStream stream;
        stream.Status = "204";
        stream.Headers = {};
        stream.Body = "";

        auto response = StreamToIncomingResponse(stream, request);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "204");
    }

    Y_UNIT_TEST(StreamToIncomingResponseVariousStatusCodes) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/");

        // Test a few status codes for proper reason phrase mapping
        for (TString statusCode : {"200", "404", "500", "302"}) {
            TStream stream;
            stream.Status = statusCode;
            stream.Headers = {};
            stream.Body = "";

            auto response = StreamToIncomingResponse(stream, request);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Status, statusCode);
        }
    }
}

Y_UNIT_TEST_SUITE(Http2Integration) {
    Y_UNIT_TEST(Http2ClearTextUpgrade) {
        // Test h2c (HTTP/2 over cleartext TCP) protocol detection.
        // Simulates what curl --http2-prior-knowledge does:
        // connect via plain TCP and send HTTP/2 connection preface.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->AllowHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), add.Release()), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/h2test", serverId)), 0, true);

        TString clientError;
        TString clientResponseStatus;
        TString clientResponseBody;
        std::atomic<bool> clientDone{false};

        std::thread clientThread([port, &clientError, &clientResponseStatus, &clientResponseBody, &clientDone]() {
            // Connect a raw TCP socket and speak HTTP/2 directly
            int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
            if (fd < 0) {
                clientError = "socket() failed";
                clientDone = true;
                return;
            }
            struct sockaddr_in6 addr = {};
            addr.sin6_family = AF_INET6;
            addr.sin6_port = htons(port);
            addr.sin6_addr = in6addr_loopback;
            if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                clientError = "connect() failed";
                ::close(fd);
                clientDone = true;
                return;
            }

            // Use TSession as HTTP/2 client
            TString outputData;
            TString responseStatus;
            TString responseBody;
            bool gotResponse = false;

            NHttp::NHttp2::TSessionCallbacks callbacks;
            callbacks.OnResponse = [&](uint32_t /*streamId*/, NHttp::NHttp2::TStream& stream) {
                responseStatus = stream.Status;
                responseBody = stream.Body;
                gotResponse = true;
            };
            callbacks.OnSend = [&](TString data) {
                outputData.append(data);
            };
            callbacks.OnError = [&](TString error) {
                clientError = error;
            };

            NHttp::NHttp2::TSession session(NHttp::NHttp2::TSession::ERole::Client, std::move(callbacks));
            session.Initialize();

            // Send a request
            TVector<std::pair<TString, TString>> headers;
            session.SendRequest("GET", "/h2test", "localhost", "http", headers, "");

            // Flush all pending output (connection preface + SETTINGS + HEADERS)
            if (!outputData.empty()) {
                const char* buf = outputData.data();
                size_t remaining = outputData.size();
                while (remaining > 0) {
                    ssize_t sent = ::send(fd, buf, remaining, 0);
                    if (sent <= 0) {
                        clientError = "send() failed";
                        ::close(fd);
                        clientDone = true;
                        return;
                    }
                    buf += sent;
                    remaining -= sent;
                }
                outputData.clear();
            }

            // Read responses until we have what we need (with timeout)
            auto deadline = TInstant::Now() + TDuration::Seconds(5);
            char readBuf[16384];
            while (!gotResponse && TInstant::Now() < deadline) {
                struct timeval tv;
                tv.tv_sec = 1;
                tv.tv_usec = 0;
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

                ssize_t n = ::recv(fd, readBuf, sizeof(readBuf), 0);
                if (n > 0) {
                    session.Feed(readBuf, static_cast<size_t>(n));
                    // Send any pending data (e.g. SETTINGS ACK, WINDOW_UPDATE)
                    if (!outputData.empty()) {
                        const char* buf2 = outputData.data();
                        size_t rem = outputData.size();
                        while (rem > 0) {
                            ssize_t sent = ::send(fd, buf2, rem, 0);
                            if (sent <= 0) break;
                            buf2 += sent;
                            rem -= sent;
                        }
                        outputData.clear();
                    }
                } else if (n == 0) {
                    break; // connection closed
                } else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        continue;
                    }
                    break;
                }
            }

            if (gotResponse) {
                clientResponseStatus = responseStatus;
                clientResponseBody = responseBody;
            } else if (clientError.empty()) {
                clientError = "timed out waiting for HTTP/2 response";
            }

            ::close(fd);
            clientDone = true;
        });

        // Server side: grab the incoming request delivered through the actor system
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, "/h2test");
        UNIT_ASSERT_EQUAL(request->Request->Method, "GET");

        // Send response back
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseOK("h2 works", "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        clientThread.join();

        UNIT_ASSERT_C(clientError.empty(), "Client error: " + clientError);
        UNIT_ASSERT_EQUAL(clientResponseStatus, "200");
        UNIT_ASSERT_EQUAL(clientResponseBody, "h2 works");
    }

    Y_UNIT_TEST(Http2UpgradeFrom101) {
        // Test HTTP/1.1 -> h2c upgrade via 101 Switching Protocols (RFC 7540 §3.2).
        // Simulates what curl --http2 does:
        // send HTTP/1.1 request with Upgrade: h2c, expect 101, then switch to HTTP/2.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->AllowHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), add.Release()), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/h2upgrade", serverId)), 0, true);

        TString clientError;
        TString clientResponseStatus;
        TString clientResponseBody;
        std::atomic<bool> clientDone{false};

        std::thread clientThread([port, &clientError, &clientResponseStatus, &clientResponseBody, &clientDone]() {
            int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
            if (fd < 0) {
                clientError = "socket() failed";
                clientDone = true;
                return;
            }
            struct sockaddr_in6 addr = {};
            addr.sin6_family = AF_INET6;
            addr.sin6_port = htons(port);
            addr.sin6_addr = in6addr_loopback;
            if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                clientError = "connect() failed";
                ::close(fd);
                clientDone = true;
                return;
            }

            // Step 1: Send HTTP/1.1 request with Upgrade: h2c
            TString httpRequest =
                "GET /h2upgrade HTTP/1.1\r\n"
                "Host: localhost\r\n"
                "Connection: Upgrade, HTTP2-Settings\r\n"
                "Upgrade: h2c\r\n"
                "HTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n"
                "\r\n";
            {
                const char* buf = httpRequest.data();
                size_t remaining = httpRequest.size();
                while (remaining > 0) {
                    ssize_t sent = ::send(fd, buf, remaining, 0);
                    if (sent <= 0) {
                        clientError = "send() failed for HTTP/1.1 request";
                        ::close(fd);
                        clientDone = true;
                        return;
                    }
                    buf += sent;
                    remaining -= sent;
                }
            }

            // Step 2: Read 101 Switching Protocols response
            TString responseBuffer;
            {
                auto deadline = TInstant::Now() + TDuration::Seconds(5);
                char readBuf[4096];
                while (TInstant::Now() < deadline) {
                    struct timeval tv;
                    tv.tv_sec = 1;
                    tv.tv_usec = 0;
                    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

                    ssize_t n = ::recv(fd, readBuf, sizeof(readBuf), 0);
                    if (n > 0) {
                        responseBuffer.append(readBuf, n);
                        if (responseBuffer.Contains("\r\n\r\n")) {
                            break;
                        }
                    } else if (n == 0) {
                        clientError = "connection closed before 101";
                        ::close(fd);
                        clientDone = true;
                        return;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                        clientError = "recv() failed waiting for 101";
                        ::close(fd);
                        clientDone = true;
                        return;
                    }
                }
                if (!responseBuffer.StartsWith("HTTP/1.1 101")) {
                    clientError = "expected 101, got: " + responseBuffer;
                    ::close(fd);
                    clientDone = true;
                    return;
                }
            }

            // Step 3: Switch to HTTP/2 — send client connection preface + SETTINGS
            TString outputData;
            TString responseStatus;
            TString responseBody;
            bool gotResponse = false;

            NHttp::NHttp2::TSessionCallbacks callbacks;
            callbacks.OnResponse = [&](uint32_t /*streamId*/, NHttp::NHttp2::TStream& stream) {
                responseStatus = stream.Status;
                responseBody = stream.Body;
                gotResponse = true;
            };
            callbacks.OnSend = [&](TString data) {
                outputData.append(data);
            };
            callbacks.OnError = [&](TString error) {
                clientError = error;
            };

            NHttp::NHttp2::TSession session(NHttp::NHttp2::TSession::ERole::Client, std::move(callbacks));
            session.Initialize();

            // The original request was already sent as HTTP/1.1, so stream 1 is
            // reserved for the upgrade response — we don't send a new HEADERS frame
            // for it. Just register that we expect a response on stream 1.
            session.RegisterUpgradeStream("/h2upgrade", "localhost");

            // Flush client preface + SETTINGS
            if (!outputData.empty()) {
                const char* buf = outputData.data();
                size_t remaining = outputData.size();
                while (remaining > 0) {
                    ssize_t sent = ::send(fd, buf, remaining, 0);
                    if (sent <= 0) {
                        clientError = "send() failed for h2 preface";
                        ::close(fd);
                        clientDone = true;
                        return;
                    }
                    buf += sent;
                    remaining -= sent;
                }
                outputData.clear();
            }

            // Step 4: Read HTTP/2 frames (server SETTINGS + response on stream 1)
            auto deadline = TInstant::Now() + TDuration::Seconds(5);
            char readBuf[16384];
            while (!gotResponse && TInstant::Now() < deadline) {
                struct timeval tv;
                tv.tv_sec = 1;
                tv.tv_usec = 0;
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

                ssize_t n = ::recv(fd, readBuf, sizeof(readBuf), 0);
                if (n > 0) {
                    session.Feed(readBuf, static_cast<size_t>(n));
                    if (!outputData.empty()) {
                        const char* buf2 = outputData.data();
                        size_t rem = outputData.size();
                        while (rem > 0) {
                            ssize_t sent = ::send(fd, buf2, rem, 0);
                            if (sent <= 0) break;
                            buf2 += sent;
                            rem -= sent;
                        }
                        outputData.clear();
                    }
                } else if (n == 0) {
                    break;
                } else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                    break;
                }
            }

            if (gotResponse) {
                clientResponseStatus = responseStatus;
                clientResponseBody = responseBody;
            } else if (clientError.empty()) {
                clientError = "timed out waiting for HTTP/2 response";
            }

            ::close(fd);
            clientDone = true;
        });

        // Server side: grab the incoming request delivered through the actor system
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, "/h2upgrade");
        UNIT_ASSERT_EQUAL(request->Request->Method, "GET");

        // Send response back — this should be delivered as HTTP/2 stream 1
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseOK("upgrade works", "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        clientThread.join();

        UNIT_ASSERT_C(clientError.empty(), "Client error: " + clientError);
        UNIT_ASSERT_EQUAL(clientResponseStatus, "200");
        UNIT_ASSERT_EQUAL(clientResponseBody, "upgrade works");
    }
}

Y_UNIT_TEST_SUITE(Http2Compression) {

    NHttp::THttpIncomingRequestPtr MakeRequestWithAcceptEncoding(TStringBuf acceptEncoding, const std::vector<TString>& compressTypes = {"text/plain"}) {
        auto endpoint = std::make_shared<NHttp::TPrivateEndpointInfo>(compressTypes);
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest(
            std::shared_ptr<NHttp::THttpEndpointInfo>(endpoint, static_cast<NHttp::THttpEndpointInfo*>(endpoint.get())),
            NHttp::THttpConfig::SocketAddressType());
        TString raw = TStringBuilder()
            << "GET /test HTTP/1.1\r\n"
            << "Host: test\r\n"
            << "Accept-Encoding: " << acceptEncoding << "\r\n"
            << "\r\n";
        EatWholeString(request, raw);
        return request;
    }

    Y_UNIT_TEST(DeflateCompressedResponseRoundtrip) {
        auto request = MakeRequestWithAcceptEncoding("deflate");
        TString originalBody = "hello world, this is a test body for compression";

        auto response = request->CreateResponseOK(originalBody, "text/plain");
        // Response should have compression enabled
        UNIT_ASSERT_VALUES_EQUAL(TString(response->ContentEncoding), "deflate");
        // response->Body stores uncompressed data
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Body), originalBody);

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        UNIT_ASSERT_VALUES_EQUAL(status, "200");

        // Body should be compressed (different from original)
        UNIT_ASSERT(body != originalBody);
        UNIT_ASSERT(!body.empty());

        // content-encoding header should be present
        bool foundEncoding = false;
        TString encodingValue;
        for (const auto& [name, value] : headers) {
            if (name == "content-encoding") {
                foundEncoding = true;
                encodingValue = value;
            }
        }
        UNIT_ASSERT(foundEncoding);
        UNIT_ASSERT_VALUES_EQUAL(encodingValue, "deflate");

        // content-length should match compressed body size
        bool foundLength = false;
        for (const auto& [name, value] : headers) {
            if (name == "content-length") {
                foundLength = true;
                UNIT_ASSERT_VALUES_EQUAL(value, ToString(body.size()));
            }
        }
        UNIT_ASSERT(foundLength);

        // Decompress and verify roundtrip
        NHttp::TCompressContext decompressor;
        decompressor.InitDecompress("deflate");
        TString decompressed = decompressor.Decompress(body);
        UNIT_ASSERT_VALUES_EQUAL(decompressed, originalBody);
    }

    Y_UNIT_TEST(GzipCompressedResponseRoundtrip) {
        auto request = MakeRequestWithAcceptEncoding("gzip");
        TString originalBody = "hello world, this is a test body for gzip compression";

        auto response = request->CreateResponseOK(originalBody, "text/plain");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->ContentEncoding), "gzip");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Body), originalBody);

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        UNIT_ASSERT_VALUES_EQUAL(status, "200");
        UNIT_ASSERT(body != originalBody);

        bool foundEncoding = false;
        for (const auto& [name, value] : headers) {
            if (name == "content-encoding") {
                foundEncoding = true;
                UNIT_ASSERT_VALUES_EQUAL(value, "gzip");
            }
        }
        UNIT_ASSERT(foundEncoding);

        NHttp::TCompressContext decompressor;
        decompressor.InitDecompress("gzip");
        TString decompressed = decompressor.Decompress(body);
        UNIT_ASSERT_VALUES_EQUAL(decompressed, originalBody);
    }

    Y_UNIT_TEST(NoCompressionWhenNotRequested) {
        // Request without Accept-Encoding — no compression
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, TString("GET /test HTTP/1.1\r\nHost: test\r\n\r\n"));

        auto response = request->CreateResponseOK("hello", "text/plain");
        UNIT_ASSERT(response->ContentEncoding.empty());

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        // Body should be uncompressed
        UNIT_ASSERT_VALUES_EQUAL(body, "hello");

        // No content-encoding header
        for (const auto& [name, _] : headers) {
            UNIT_ASSERT_UNEQUAL(name, "content-encoding");
        }
    }

    Y_UNIT_TEST(NoCompressionWhenContentTypeNotInList) {
        // Accept-Encoding present but content type not in CompressContentTypes
        auto request = MakeRequestWithAcceptEncoding("deflate", {"application/json"});
        auto response = request->CreateResponseOK("plain text body", "text/plain");

        // text/plain is not in compress list, so no encoding
        UNIT_ASSERT(response->ContentEncoding.empty());

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        UNIT_ASSERT_VALUES_EQUAL(body, "plain text body");
        for (const auto& [name, _] : headers) {
            UNIT_ASSERT_UNEQUAL(name, "content-encoding");
        }
    }

    Y_UNIT_TEST(EmptyBodyNotCompressed) {
        auto request = MakeRequestWithAcceptEncoding("deflate");
        auto response = request->CreateResponse("204", "No Content");

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        UNIT_ASSERT_VALUES_EQUAL(status, "204");
        UNIT_ASSERT(body.empty());
    }

    Y_UNIT_TEST(CompressedResponseViaCreateResponseString) {
        // This tests the path where an upstream HTTP/1.1 response is forwarded as HTTP/2
        auto request = MakeRequestWithAcceptEncoding("deflate");

        TString upstreamBody = "upstream response body that should be compressed for HTTP/2";
        TString rawUpstreamResponse = TStringBuilder()
            << "HTTP/1.1 200 OK\r\n"
            << "Content-Type: text/plain\r\n"
            << "Content-Length: " << upstreamBody.size() << "\r\n"
            << "\r\n"
            << upstreamBody;

        auto response = request->CreateResponseString(rawUpstreamResponse);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Status), "200");
        // Body should be uncompressed in response->Body
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Body), upstreamBody);

        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        OutgoingResponseToStream(response, status, headers, body);

        // Should be compressed for HTTP/2
        UNIT_ASSERT(body != upstreamBody);

        NHttp::TCompressContext decompressor;
        decompressor.InitDecompress("deflate");
        TString decompressed = decompressor.Decompress(body);
        UNIT_ASSERT_VALUES_EQUAL(decompressed, upstreamBody);
    }
}

// ============== Helper: Build a raw frame of arbitrary type ==============
namespace {

TString BuildRawFrame(EFrameType type, uint8_t flags, uint32_t streamId, TStringBuf payload) {
    TString result;
    result.resize(FRAME_HEADER_SIZE + payload.size());
    char* out = result.Detach();
    TFrameHeader hdr;
    hdr.Length = payload.size();
    hdr.Type = type;
    hdr.Flags = flags;
    hdr.StreamId = streamId;
    hdr.Serialize(out);
    if (!payload.empty()) {
        std::memcpy(out + FRAME_HEADER_SIZE, payload.data(), payload.size());
    }
    return result;
}

// Helper: create an initialized server session for raw frame injection
struct TRawServerSession {
    std::unique_ptr<TSession> Session;
    TString OutputBuf;
    TString Error;
    bool GotGoaway = false;

    struct TReceivedRequest {
        uint32_t StreamId;
        TString Method;
        TString Path;
        TString Body;
        TVector<std::pair<TString, TString>> Headers;
    };
    TVector<TReceivedRequest> Requests;

    TRawServerSession() {
        TSessionCallbacks cb;
        cb.OnSend = [this](TString data) { OutputBuf.append(data); };
        cb.OnError = [this](TString e) { Error = e; };
        cb.OnRequest = [this](uint32_t streamId, TStream& stream) {
            TReceivedRequest req;
            req.StreamId = streamId;
            req.Method = stream.Method;
            req.Path = stream.Path;
            req.Body = stream.Body;
            req.Headers = stream.Headers;
            Requests.push_back(std::move(req));
        };
        cb.OnGoaway = [this](uint32_t, EErrorCode) { GotGoaway = true; };

        Session = std::make_unique<TSession>(TSession::ERole::Server, std::move(cb));
        Session->Initialize();

        // Feed connection preface from "client"
        Session->Feed(CONNECTION_PREFACE.data(), CONNECTION_PREFACE.size());

        // Feed client SETTINGS (empty)
        TString settings = TFrameBuilder::BuildSettings(0, NFrameFlag::NONE, {});
        Session->Feed(settings.data(), settings.size());

        // Feed SETTINGS ACK for server's settings
        TString settingsAck = TFrameBuilder::BuildSettingsAck();
        Session->Feed(settingsAck.data(), settingsAck.size());

        OutputBuf.clear();
        Error.clear();
    }

    void Feed(const TString& data) {
        Session->Feed(data.data(), data.size());
    }
};

} // anonymous namespace

// ============== Huffman Codec Tests ==============

Y_UNIT_TEST_SUITE(Http2HuffmanCodec) {
    Y_UNIT_TEST(HuffmanEncodeDecodeRoundtrip) {
        THuffmanDecoder decoder;
        TString original = "www.example.com";
        TString encoded = THPackEncoder::HuffmanEncode(original);
        UNIT_ASSERT(!encoded.empty());
        UNIT_ASSERT_LT(encoded.size(), original.size()); // Huffman should compress

        TString decoded;
        UNIT_ASSERT(decoder.Decode(
            reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size(), decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded, original);
    }

    Y_UNIT_TEST(HuffmanEncodeDecodeVariousStrings) {
        THuffmanDecoder decoder;
        TVector<TString> testStrings = {
            "",
            "a",
            "Hello, World!",
            "/index.html",
            "application/json",
            "text/html; charset=utf-8",
            "0123456789",
            "AAAAAAAAAA",
            TString(200, 'x'),
        };

        for (const auto& original : testStrings) {
            TString encoded = THPackEncoder::HuffmanEncode(original);
            TString decoded;
            UNIT_ASSERT_C(decoder.Decode(
                reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size(), decoded),
                "Failed to decode Huffman for: " + original);
            UNIT_ASSERT_VALUES_EQUAL(decoded, original);
        }
    }

    Y_UNIT_TEST(HuffmanDecodeInvalidData) {
        THuffmanDecoder decoder;
        // Invalid Huffman data (random bytes that end in the middle of a symbol)
        uint8_t badData[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00};
        TString out;
        // May or may not fail depending on exact bit pattern, but shouldn't crash
        decoder.Decode(badData, sizeof(badData), out);
    }

    Y_UNIT_TEST(EncodeStringWithHuffman) {
        THPackEncoder encoder;
        TString result;
        encoder.EncodeString(result, "www.example.com", true);
        UNIT_ASSERT(!result.empty());
        // First byte should have H bit set (0x80)
        UNIT_ASSERT((static_cast<uint8_t>(result[0]) & 0x80) != 0);
    }

    Y_UNIT_TEST(EncodeStringWithoutHuffman) {
        THPackEncoder encoder;
        TString result;
        encoder.EncodeString(result, "test", false);
        UNIT_ASSERT(!result.empty());
        // First byte should NOT have H bit set
        UNIT_ASSERT((static_cast<uint8_t>(result[0]) & 0x80) == 0);
    }

    Y_UNIT_TEST(EncodeDecodeStringHuffmanRoundtrip) {
        // Encode a string with huffman, then decode it using THPackDecoder's DecodeString
        THPackEncoder encoder;
        THPackDecoder decoder;

        // Build a header block with huffman-encoded string using literal-with-indexing
        // We'll just use Encode() which internally calls EncodeString
        // Instead, let's test the full roundtrip through encoder/decoder with huffman
        TVector<std::pair<TString, TString>> headers = {
            {"x-test-header", "this-is-a-huffman-encoded-value"},
        };

        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(decoded[0].first, "x-test-header");
        UNIT_ASSERT_VALUES_EQUAL(decoded[0].second, "this-is-a-huffman-encoded-value");
    }

    Y_UNIT_TEST(HuffmanEmptyString) {
        THuffmanDecoder decoder;
        TString encoded = THPackEncoder::HuffmanEncode("");
        UNIT_ASSERT(encoded.empty());
        TString decoded;
        UNIT_ASSERT(decoder.Decode(
            reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size(), decoded));
        UNIT_ASSERT(decoded.empty());
    }

    Y_UNIT_TEST(HuffmanAllPrintableAscii) {
        // Test encoding/decoding all printable ASCII byte values
        THuffmanDecoder decoder;
        TString original;
        for (int i = 32; i < 127; ++i) {
            original.push_back(static_cast<char>(i));
        }
        TString encoded = THPackEncoder::HuffmanEncode(original);
        TString decoded;
        UNIT_ASSERT(decoder.Decode(
            reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size(), decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded, original);
    }
}

// ============== HPACK Advanced Tests ==============

Y_UNIT_TEST_SUITE(Http2HpackAdvanced) {
    Y_UNIT_TEST(EncodeIntegerMultiByte) {
        THPackEncoder encoder;
        // Value that requires multi-byte encoding (value >= maxPrefix)
        // With 5-bit prefix, maxPrefix = 31. So value=300 needs multi-byte
        TString result;
        encoder.EncodeInteger(result, 300, 5, 0x00);
        UNIT_ASSERT_GT(result.size(), 1u);
        // First byte should have all 5 prefix bits set (31)
        UNIT_ASSERT_VALUES_EQUAL(static_cast<uint8_t>(result[0]) & 0x1F, 0x1Fu);
    }

    Y_UNIT_TEST(EncodeDecodeIntegerRoundtrip) {
        // Test encode/decode roundtrip for various integer sizes
        THPackEncoder encoder;
        TVector<uint32_t> values = {0, 1, 30, 31, 127, 128, 255, 256, 1000, 16384, 65535, 100000};

        for (uint8_t prefix = 4; prefix <= 7; ++prefix) {
            for (uint32_t val : values) {
                TString encoded;
                encoder.EncodeInteger(encoded, val, prefix, 0x00);

                // Decode
                const uint8_t* pos = reinterpret_cast<const uint8_t*>(encoded.data());
                const uint8_t* end = pos + encoded.size();
                uint32_t decoded;
                THPackDecoder decoder;
                UNIT_ASSERT_C(decoder.DecodeInteger(pos, end, prefix, decoded),
                    "Failed for val=" + ToString(val) + " prefix=" + ToString(prefix));
                UNIT_ASSERT_VALUES_EQUAL_C(decoded, val,
                    "Mismatch for val=" + ToString(val) + " prefix=" + ToString(prefix));
                UNIT_ASSERT_VALUES_EQUAL(pos, end); // all consumed
            }
        }
    }

    Y_UNIT_TEST(DecodeIntegerEmpty) {
        THPackDecoder decoder;
        const uint8_t* pos = nullptr;
        const uint8_t* end = nullptr;
        uint32_t value;
        UNIT_ASSERT(!decoder.DecodeInteger(pos, end, 7, value));
    }

    Y_UNIT_TEST(DecodeIntegerTruncated) {
        THPackDecoder decoder;
        // Multi-byte integer with continuation bit set but no more bytes
        uint8_t data[] = {0x7F, 0x80}; // prefix=7: value=127+... but continuation bit set and no next byte
        const uint8_t* pos = data;
        const uint8_t* end = data + sizeof(data);
        uint32_t value;
        UNIT_ASSERT(!decoder.DecodeInteger(pos, end, 7, value));
    }

    Y_UNIT_TEST(DecodeStringEmpty) {
        THPackDecoder decoder;
        const uint8_t* pos = nullptr;
        const uint8_t* end = nullptr;
        TString out;
        UNIT_ASSERT(!decoder.DecodeString(pos, end, out));
    }

    Y_UNIT_TEST(DecodeStringHuffman) {
        // Build a huffman-encoded string and decode it
        TString original = "custom-value";
        TString huffEncoded = THPackEncoder::HuffmanEncode(original);

        // Build the string representation: H=1 bit + length + data
        TString strData;
        THPackEncoder encoder;
        encoder.EncodeString(strData, original, true);

        THPackDecoder decoder;
        const uint8_t* pos = reinterpret_cast<const uint8_t*>(strData.data());
        const uint8_t* end = pos + strData.size();
        TString decoded;
        UNIT_ASSERT(decoder.DecodeString(pos, end, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded, original);
    }

    Y_UNIT_TEST(LookupIndexStaticTable) {
        THPackDecoder decoder;
        TString name, value;
        // Static table index 2 = :method GET
        UNIT_ASSERT(decoder.LookupIndex(2, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, ":method");
        UNIT_ASSERT_VALUES_EQUAL(value, "GET");
    }

    Y_UNIT_TEST(LookupIndexInvalid) {
        THPackDecoder decoder;
        TString name, value;
        // Index 0 is invalid
        UNIT_ASSERT(!decoder.LookupIndex(0, name, value));
        // Very large index (beyond static + dynamic)
        UNIT_ASSERT(!decoder.LookupIndex(1000, name, value));
    }

    Y_UNIT_TEST(LookupIndexDynamicTable) {
        // Add entries to dynamic table through decoding, then look them up
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {"x-custom", "myvalue"},
        };
        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));

        // Now the dynamic table should have "x-custom: myvalue" at dynamic index 1
        // which is static table size (61) + 1
        TString name, value;
        UNIT_ASSERT(decoder.LookupIndex(62, name, value));
        UNIT_ASSERT_VALUES_EQUAL(name, "x-custom");
        UNIT_ASSERT_VALUES_EQUAL(value, "myvalue");
    }

    Y_UNIT_TEST(LookupIndexNameInvalid) {
        THPackDecoder decoder;
        TString name;
        UNIT_ASSERT(!decoder.LookupIndexName(0, name));
        UNIT_ASSERT(!decoder.LookupIndexName(1000, name));
    }

    Y_UNIT_TEST(LookupIndexNameDynamicTable) {
        THPackEncoder encoder;
        THPackDecoder decoder;

        TVector<std::pair<TString, TString>> headers = {
            {"x-my-header", "value1"},
        };
        TString encoded = encoder.Encode(headers);
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(encoded, decoded));

        TString name;
        UNIT_ASSERT(decoder.LookupIndexName(62, name));
        UNIT_ASSERT_VALUES_EQUAL(name, "x-my-header");
    }

    Y_UNIT_TEST(DynamicTableSizeUpdateInDecode) {
        // Test §6.3: Dynamic Table Size Update issued inside a header block
        THPackDecoder decoder;

        // Build raw HPACK block: dynamic table size update to 0, then a literal header
        TString data;
        // §6.3: 001xxxxx pattern, 5-bit prefix
        // Size 0: 0x20 (001_00000)
        data.push_back(0x20);
        // Then a literal-without-indexing: 0000xxxx with new name
        // index=0, new name "a", value "b"
        data.push_back(0x00); // literal without indexing, index=0
        data.push_back(0x01); // name length=1
        data.push_back('a');
        data.push_back(0x01); // value length=1
        data.push_back('b');

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(decoder.Decode(data, headers));
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(headers[0].first, "a");
        UNIT_ASSERT_VALUES_EQUAL(headers[0].second, "b");
    }

    Y_UNIT_TEST(LiteralWithoutIndexing) {
        // §6.2.2: 0000xxxx pattern
        THPackDecoder decoder;

        TString data;
        // Literal without indexing, new name
        data.push_back(0x00); // pattern 0000, index=0
        data.push_back(0x06); // name length=6
        data.append("x-test");
        data.push_back(0x05); // value length=5
        data.append("hello");

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(decoder.Decode(data, headers));
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(headers[0].first, "x-test");
        UNIT_ASSERT_VALUES_EQUAL(headers[0].second, "hello");
    }

    Y_UNIT_TEST(LiteralNeverIndexed) {
        // §6.2.3: 0001xxxx pattern
        THPackDecoder decoder;

        TString data;
        // Literal never indexed, new name
        data.push_back(0x10); // pattern 0001, index=0
        data.push_back(0x09); // name length=9
        data.append("sensitive");
        data.push_back(0x06); // value length=6
        data.append("secret");

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(decoder.Decode(data, headers));
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(headers[0].first, "sensitive");
        UNIT_ASSERT_VALUES_EQUAL(headers[0].second, "secret");
    }

    Y_UNIT_TEST(LiteralWithIndexingByIndex) {
        // §6.2.1 with index>0: references a name from the table
        THPackDecoder decoder;

        TString data;
        // Literal with incremental indexing, referencing static table entry
        // :path (index 4 in static table) = 01_000100 = 0x44
        data.push_back(0x44);
        data.push_back(0x0A); // value length=10
        data.append("/api/items");

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(decoder.Decode(data, headers));
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(headers[0].first, ":path");
        UNIT_ASSERT_VALUES_EQUAL(headers[0].second, "/api/items");
    }

    Y_UNIT_TEST(LiteralWithoutIndexingByIndex) {
        // §6.2.2 with index>0: references a name from the table
        THPackDecoder decoder;

        TString data;
        // :authority is static index 1. Without indexing: 0000_0001 = 0x01
        data.push_back(0x01);
        data.push_back(0x0B); // value length=11
        data.append("example.com");

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(decoder.Decode(data, headers));
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(headers[0].first, ":authority");
        UNIT_ASSERT_VALUES_EQUAL(headers[0].second, "example.com");
    }

    Y_UNIT_TEST(FindHeaderDynamicTableExactMatch) {
        THPackEncoder encoder;

        // Encode a custom header - it gets added to the dynamic table
        TVector<std::pair<TString, TString>> headers = {
            {"x-custom-key", "custom-val"},
        };
        TString firstEncoded = encoder.Encode(headers);

        // Encode again - should find exact match in dynamic table (indexed representation)
        TString secondEncoded = encoder.Encode(headers);
        // Second encoding should be shorter (indexed reference)
        UNIT_ASSERT_LE(secondEncoded.size(), firstEncoded.size());

        // Verify roundtrip
        THPackDecoder decoder;
        TVector<std::pair<TString, TString>> decoded;
        UNIT_ASSERT(decoder.Decode(firstEncoded, decoded));
        decoded.clear();
        UNIT_ASSERT(decoder.Decode(secondEncoded, decoded));
        UNIT_ASSERT_VALUES_EQUAL(decoded.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(decoded[0].first, "x-custom-key");
        UNIT_ASSERT_VALUES_EQUAL(decoded[0].second, "custom-val");
    }

    Y_UNIT_TEST(DecoderRejectsTruncatedLiteral) {
        THPackDecoder decoder;
        // Literal with indexing, new name, but data is truncated
        TString data;
        data.push_back(0x40); // literal with indexing, index=0
        data.push_back(0x05); // name length=5
        data.append("abc"); // only 3 bytes, expected 5

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(!decoder.Decode(data, headers));
    }

    Y_UNIT_TEST(DecoderRejectsInvalidIndexedLookup) {
        THPackDecoder decoder;
        // Indexed header with very large index (beyond static table + empty dynamic table)
        TString data;
        // Indexed: 1_xxxxxxx. Index = 100 which is beyond static table (61 entries)
        // 7-bit prefix: maxPrefix = 127. 100 < 127, so single byte: 0x80 | 100 = 0xE4
        data.push_back(static_cast<char>(0x80 | 100));

        TVector<std::pair<TString, TString>> headers;
        UNIT_ASSERT(!decoder.Decode(data, headers));
    }
}

// ============== CONTINUATION Frame Tests ==============

Y_UNIT_TEST_SUITE(Http2ContinuationFrames) {
    // Helper struct for session pair in this suite
    struct TSessionPair {
        TString ServerRecvBuf;
        TString ClientRecvBuf;
        TString ServerError;
        TString ClientError;

        struct TReceivedRequest {
            uint32_t StreamId;
            TString Method;
            TString Path;
            TVector<std::pair<TString, TString>> Headers;
        };

        struct TReceivedResponse {
            uint32_t StreamId;
            TString Status;
            TString Body;
            TVector<std::pair<TString, TString>> Headers;
        };

        TVector<TReceivedRequest> ServerRequests;
        TVector<TReceivedResponse> ClientResponses;

        std::unique_ptr<TSession> Server;
        std::unique_ptr<TSession> Client;

        TSessionPair() {
            TSessionCallbacks serverCb;
            serverCb.OnRequest = [this](uint32_t streamId, TStream& stream) {
                TReceivedRequest req;
                req.StreamId = streamId;
                req.Method = stream.Method;
                req.Path = stream.Path;
                req.Headers = stream.Headers;
                ServerRequests.push_back(std::move(req));
            };
            serverCb.OnSend = [this](TString data) { ClientRecvBuf.append(data); };
            serverCb.OnError = [this](TString e) { ServerError = e; };
            serverCb.OnGoaway = [](uint32_t, EErrorCode) {};

            TSessionCallbacks clientCb;
            clientCb.OnResponse = [this](uint32_t streamId, TStream& stream) {
                TReceivedResponse resp;
                resp.StreamId = streamId;
                resp.Status = stream.Status;
                resp.Body = stream.Body;
                resp.Headers = stream.Headers;
                ClientResponses.push_back(std::move(resp));
            };
            clientCb.OnSend = [this](TString data) { ServerRecvBuf.append(data); };
            clientCb.OnError = [this](TString e) { ClientError = e; };
            clientCb.OnGoaway = [](uint32_t, EErrorCode) {};

            Server = std::make_unique<TSession>(TSession::ERole::Server, std::move(serverCb));
            Client = std::make_unique<TSession>(TSession::ERole::Client, std::move(clientCb));
        }

        void Initialize() {
            Client->Initialize();
            Server->Initialize();
            Pump();
        }

        void Pump() {
            for (int i = 0; i < 10; ++i) {
                bool progress = false;
                if (!ServerRecvBuf.empty()) {
                    TString data;
                    data.swap(ServerRecvBuf);
                    Server->Feed(data.data(), data.size());
                    progress = true;
                }
                if (!ClientRecvBuf.empty()) {
                    TString data;
                    data.swap(ClientRecvBuf);
                    Client->Feed(data.data(), data.size());
                    progress = true;
                }
                if (!progress) break;
            }
        }
    };

    Y_UNIT_TEST(ReceiveContinuation) {
        // Send HEADERS without END_HEADERS, then CONTINUATION with END_HEADERS
        TRawServerSession srv;

        // Encode a simple request
        THPackEncoder encoder;
        TVector<std::pair<TString, TString>> headers = {
            {":method", "GET"},
            {":path", "/continuation-test"},
            {":scheme", "https"},
            {":authority", "example.com"},
        };
        TString headerBlock = encoder.Encode(headers);

        // Split header block in half
        size_t half = headerBlock.size() / 2;
        TStringBuf firstPart(headerBlock.data(), half);
        TStringBuf secondPart(headerBlock.data() + half, headerBlock.size() - half);

        // HEADERS without END_HEADERS, with END_STREAM (no body)
        uint8_t headersFlags = NFrameFlag::END_STREAM; // no END_HEADERS
        TString headersFrame = TFrameBuilder::BuildHeaders(1, headersFlags, firstPart);
        srv.Feed(headersFrame);

        // Should not have received request yet (waiting for CONTINUATION)
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 0u);
        UNIT_ASSERT(srv.Error.empty());

        // CONTINUATION with END_HEADERS
        TString contFrame = TFrameBuilder::BuildContinuation(1, NFrameFlag::END_HEADERS, secondPart);
        srv.Feed(contFrame);

        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Path, "/continuation-test");
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Method, "GET");
    }

    Y_UNIT_TEST(ReceiveMultipleContinuations) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TVector<std::pair<TString, TString>> headers = {
            {":method", "POST"},
            {":path", "/multi-cont"},
            {":scheme", "https"},
            {":authority", "example.com"},
            {"x-header-1", "value-1"},
            {"x-header-2", "value-2"},
            {"x-header-3", "value-3"},
        };
        TString headerBlock = encoder.Encode(headers);

        // Split into 3 parts
        size_t partSize = headerBlock.size() / 3;
        TStringBuf part1(headerBlock.data(), partSize);
        TStringBuf part2(headerBlock.data() + partSize, partSize);
        TStringBuf part3(headerBlock.data() + 2 * partSize, headerBlock.size() - 2 * partSize);

        // HEADERS without END_HEADERS
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_STREAM, part1));
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 0u);

        // CONTINUATION without END_HEADERS
        srv.Feed(TFrameBuilder::BuildContinuation(1, NFrameFlag::NONE, part2));
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 0u);

        // CONTINUATION with END_HEADERS
        srv.Feed(TFrameBuilder::BuildContinuation(1, NFrameFlag::END_HEADERS, part3));

        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Path, "/multi-cont");
    }

    Y_UNIT_TEST(NonContinuationWhileExpecting) {
        // Send HEADERS without END_HEADERS, then a DATA frame -> PROTOCOL_ERROR
        TRawServerSession srv;

        THPackEncoder encoder;
        TVector<std::pair<TString, TString>> headers = {
            {":method", "GET"},
            {":path", "/"},
            {":scheme", "https"},
            {":authority", "example.com"},
        };
        TString headerBlock = encoder.Encode(headers);

        // HEADERS without END_HEADERS
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::NONE, headerBlock));
        UNIT_ASSERT(srv.Error.empty());

        // Send DATA instead of CONTINUATION -> error
        srv.Feed(TFrameBuilder::BuildData(1, NFrameFlag::END_STREAM, "body"));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("CONTINUATION"));
    }

    Y_UNIT_TEST(ContinuationForWrongStream) {
        // Send HEADERS without END_HEADERS for stream 1, then CONTINUATION for stream 3
        TRawServerSession srv;

        THPackEncoder encoder;
        TVector<std::pair<TString, TString>> headers = {
            {":method", "GET"},
            {":path", "/"},
            {":scheme", "https"},
            {":authority", "example.com"},
        };
        TString headerBlock = encoder.Encode(headers);

        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::NONE, headerBlock));
        // CONTINUATION for wrong stream
        srv.Feed(TFrameBuilder::BuildContinuation(3, NFrameFlag::END_HEADERS, ""));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(ContinuationForUnknownStream) {
        // CONTINUATION frame for a stream that doesn't exist
        TRawServerSession srv;

        srv.Feed(TFrameBuilder::BuildContinuation(99, NFrameFlag::END_HEADERS, ""));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(SendResponseWithContinuation) {
        // Force CONTINUATION by sending very large headers that exceed MaxFrameSize.
        // We use a session pair and verify the response arrives correctly.
        // Since default MaxFrameSize is 16384, we need headers > 16KB.
        TSessionPair pair;
        pair.Initialize();

        uint32_t streamId = pair.Client->SendRequest("GET", "/large-headers", "example.com", "https", {}, "");
        pair.Pump();

        // Build response with enough headers to exceed 16384 bytes
        TVector<std::pair<TString, TString>> respHeaders;
        for (int i = 0; i < 500; ++i) {
            respHeaders.emplace_back("x-large-header-" + ToString(i), TString(30, 'A' + (i % 26)));
        }

        pair.Server->SendResponse(streamId, "200", respHeaders, "ok");
        pair.Pump();

        UNIT_ASSERT_C(pair.ClientError.empty(), pair.ClientError);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(pair.ClientResponses[0].Body, "ok");
    }

    Y_UNIT_TEST(SendRequestWithContinuation) {
        // Force CONTINUATION on client side with large headers
        TSessionPair pair;
        pair.Initialize();

        TVector<std::pair<TString, TString>> reqHeaders;
        for (int i = 0; i < 500; ++i) {
            reqHeaders.emplace_back("x-large-req-" + ToString(i), TString(30, 'a' + (i % 26)));
        }

        pair.Client->SendRequest("GET", "/", "example.com", "https", reqHeaders, "");

        // Need extra pump iterations for large data
        for (int i = 0; i < 20; ++i) {
            bool progress = false;
            if (!pair.ServerRecvBuf.empty()) {
                TString data;
                data.swap(pair.ServerRecvBuf);
                pair.Server->Feed(data.data(), data.size());
                progress = true;
            }
            if (!pair.ClientRecvBuf.empty()) {
                TString data;
                data.swap(pair.ClientRecvBuf);
                pair.Client->Feed(data.data(), data.size());
                progress = true;
            }
            if (!progress) break;
        }

        UNIT_ASSERT_C(pair.ServerError.empty(), "Server error: " + pair.ServerError);
        UNIT_ASSERT_C(pair.ClientError.empty(), "Client error: " + pair.ClientError);
        UNIT_ASSERT_VALUES_EQUAL_C(pair.ServerRequests.size(), 1u,
            "Server got " + ToString(pair.ServerRequests.size()) + " requests, ServerError=" + pair.ServerError);
        UNIT_ASSERT_VALUES_EQUAL(pair.ServerRequests[0].Path, "/");
    }
}

// ============== Error Handling Tests ==============

Y_UNIT_TEST_SUITE(Http2ErrorHandling) {
    Y_UNIT_TEST(DataFrameOnStream0) {
        TRawServerSession srv;
        srv.Feed(TFrameBuilder::BuildData(0, NFrameFlag::NONE, "payload"));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("DATA"));
    }

    Y_UNIT_TEST(DataFrameOnUnknownStream) {
        TRawServerSession srv;
        // DATA on stream 99 which doesn't exist - should be silently ignored
        srv.Feed(TFrameBuilder::BuildData(99, NFrameFlag::END_STREAM, "data"));
        UNIT_ASSERT(srv.Error.empty()); // Not an error, just ignored
    }

    Y_UNIT_TEST(HeadersFrameOnStream0) {
        TRawServerSession srv;
        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({{":method", "GET"}, {":path", "/"}});
        srv.Feed(TFrameBuilder::BuildHeaders(0, NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, headerBlock));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("HEADERS"));
    }

    Y_UNIT_TEST(PriorityFrameOnStream0) {
        TRawServerSession srv;
        // PRIORITY frame needs exactly 5 bytes of payload
        TString payload(5, '\0');
        srv.Feed(BuildRawFrame(EFrameType::PRIORITY, NFrameFlag::NONE, 0, payload));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("PRIORITY"));
    }

    Y_UNIT_TEST(PriorityFrameWrongSize) {
        TRawServerSession srv;
        TString payload(3, '\0'); // wrong size, should be 5
        srv.Feed(BuildRawFrame(EFrameType::PRIORITY, NFrameFlag::NONE, 1, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(PriorityFrameValid) {
        TRawServerSession srv;
        TString payload(5, '\0'); // correct size
        srv.Feed(BuildRawFrame(EFrameType::PRIORITY, NFrameFlag::NONE, 1, payload));
        UNIT_ASSERT(srv.Error.empty()); // ignored but no error
    }

    Y_UNIT_TEST(RstStreamOnStream0) {
        TRawServerSession srv;
        srv.Feed(TFrameBuilder::BuildRstStream(0, EErrorCode::CANCEL));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(RstStreamWrongSize) {
        TRawServerSession srv;
        // RST_STREAM should be exactly 4 bytes
        TString payload(8, '\0');
        srv.Feed(BuildRawFrame(EFrameType::RST_STREAM, NFrameFlag::NONE, 1, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(RstStreamClosesStream) {
        TRawServerSession srv;

        // First create a stream by sending HEADERS
        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "GET"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, headerBlock));
        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);

        // Now send RST_STREAM for stream 1
        srv.Feed(TFrameBuilder::BuildRstStream(1, EErrorCode::CANCEL));
        UNIT_ASSERT(srv.Error.empty()); // Not a connection error
    }

    Y_UNIT_TEST(SettingsOnNonZeroStream) {
        TRawServerSession srv;
        srv.Feed(TFrameBuilder::BuildSettings(1, NFrameFlag::NONE, {}));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("SETTINGS"));
    }

    Y_UNIT_TEST(SettingsAckWithPayload) {
        TRawServerSession srv;
        // Build SETTINGS ACK with non-empty payload
        TVector<TSettingsEntry> entries = {{ESettingsId::MAX_CONCURRENT_STREAMS, 100}};
        srv.Feed(TFrameBuilder::BuildSettings(0, NFrameFlag::ACK, entries));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("SETTINGS ACK"));
    }

    Y_UNIT_TEST(SettingsBadPayloadSize) {
        TRawServerSession srv;
        // Payload size not multiple of 6
        TString payload(7, '\0');
        srv.Feed(BuildRawFrame(EFrameType::SETTINGS, NFrameFlag::NONE, 0, payload));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("multiple of 6"));
    }

    Y_UNIT_TEST(SettingsInvalidInitialWindowSize) {
        TRawServerSession srv;
        // INITIAL_WINDOW_SIZE (0x3) with value > MAX_WINDOW_SIZE (0x7FFFFFFF)
        TVector<TSettingsEntry> entries = {{ESettingsId::INITIAL_WINDOW_SIZE, 0x80000000u}};
        srv.Feed(TFrameBuilder::BuildSettings(0, NFrameFlag::NONE, entries));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(SettingsHeaderTableSizeUpdate) {
        TRawServerSession srv;
        // Change HEADER_TABLE_SIZE
        TVector<TSettingsEntry> entries = {{ESettingsId::HEADER_TABLE_SIZE, 2048}};
        srv.Feed(TFrameBuilder::BuildSettings(0, NFrameFlag::NONE, entries));
        UNIT_ASSERT(srv.Error.empty());
    }

    Y_UNIT_TEST(SettingsInitialWindowSizeAdjustsStreams) {
        TRawServerSession srv;

        // Create a stream first
        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "POST"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);

        // Now change INITIAL_WINDOW_SIZE - should adjust existing stream
        TVector<TSettingsEntry> entries = {{ESettingsId::INITIAL_WINDOW_SIZE, 32768}};
        srv.Feed(TFrameBuilder::BuildSettings(0, NFrameFlag::NONE, entries));
        UNIT_ASSERT(srv.Error.empty());
    }

    Y_UNIT_TEST(PingOnNonZeroStream) {
        TRawServerSession srv;
        char data[8] = {};
        // Build PING on non-zero stream
        srv.Feed(BuildRawFrame(EFrameType::PING, NFrameFlag::NONE, 1, TStringBuf(data, 8)));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("PING"));
    }

    Y_UNIT_TEST(PingWrongSize) {
        TRawServerSession srv;
        char data[4] = {};
        srv.Feed(BuildRawFrame(EFrameType::PING, NFrameFlag::NONE, 0, TStringBuf(data, 4)));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("PING"));
    }

    Y_UNIT_TEST(PingAckIgnored) {
        TRawServerSession srv;
        char data[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        srv.Feed(BuildRawFrame(EFrameType::PING, NFrameFlag::ACK, 0, TStringBuf(data, 8)));
        UNIT_ASSERT(srv.Error.empty()); // ACK should be silently ignored
    }

    Y_UNIT_TEST(GoawayOnNonZeroStream) {
        TRawServerSession srv;
        // Build GOAWAY on non-zero stream (normally BuildGoaway uses stream 0)
        TString goawayPayload(8, '\0'); // minimum GOAWAY payload: 4 bytes last-stream-id + 4 bytes error code
        srv.Feed(BuildRawFrame(EFrameType::GOAWAY, NFrameFlag::NONE, 1, goawayPayload));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("GOAWAY"));
    }

    Y_UNIT_TEST(GoawayTooSmall) {
        TRawServerSession srv;
        TString payload(4, '\0'); // only 4 bytes, needs at least 8
        srv.Feed(BuildRawFrame(EFrameType::GOAWAY, NFrameFlag::NONE, 0, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(GoawayValid) {
        TRawServerSession srv;
        srv.Feed(TFrameBuilder::BuildGoaway(0, EErrorCode::SUCCESS));
        UNIT_ASSERT(srv.Error.empty());
        UNIT_ASSERT(srv.GotGoaway);
    }

    Y_UNIT_TEST(WindowUpdateWrongSize) {
        TRawServerSession srv;
        TString payload(8, '\0'); // should be exactly 4
        srv.Feed(BuildRawFrame(EFrameType::WINDOW_UPDATE, NFrameFlag::NONE, 0, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(WindowUpdateZeroIncrementConnection) {
        TRawServerSession srv;
        srv.Feed(TFrameBuilder::BuildWindowUpdate(0, 0));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("WINDOW_UPDATE") || srv.Error.Contains("Zero"));
    }

    Y_UNIT_TEST(WindowUpdateZeroIncrementStream) {
        TRawServerSession srv;

        // Create stream 1 first
        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "GET"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        srv.Error.clear();

        // Zero increment on stream 1 → RST_STREAM (not connection error)
        srv.Feed(TFrameBuilder::BuildWindowUpdate(1, 0));
        // This sends RST_STREAM, not a connection error
        UNIT_ASSERT(srv.Error.empty());
    }

    Y_UNIT_TEST(WindowUpdateOverflowConnection) {
        TRawServerSession srv;
        // Send a WINDOW_UPDATE that would overflow the connection window
        srv.Feed(TFrameBuilder::BuildWindowUpdate(0, MAX_WINDOW_SIZE));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(WindowUpdateOverflowStream) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "GET"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        srv.Error.clear();

        // Overflow stream window
        srv.Feed(TFrameBuilder::BuildWindowUpdate(1, MAX_WINDOW_SIZE));
        // This should send RST_STREAM + close the stream
        UNIT_ASSERT(srv.Error.empty()); // Stream-level, not connection error
    }

    Y_UNIT_TEST(PushPromiseRejected) {
        TRawServerSession srv;
        TString payload(4, '\0'); // minimal PUSH_PROMISE payload
        srv.Feed(BuildRawFrame(EFrameType::PUSH_PROMISE, NFrameFlag::NONE, 1, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(HpackDecodeFailure) {
        TRawServerSession srv;
        // Feed HEADERS with garbage header block → COMPRESSION_ERROR
        TString garbage = "\xFF\xFF\xFF\xFF";
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, garbage));
        UNIT_ASSERT(!srv.Error.empty());
        UNIT_ASSERT(srv.Error.Contains("COMPRESSION") || srv.Error.Contains("HPACK"));
    }

    Y_UNIT_TEST(UnknownFrameTypeIgnored) {
        TRawServerSession srv;
        // Unknown frame type (0xFF) should be ignored per RFC 7540 Section 4.1
        TString payload = "whatever";
        srv.Feed(BuildRawFrame(static_cast<EFrameType>(0xFF), NFrameFlag::NONE, 0, payload));
        UNIT_ASSERT(srv.Error.empty());
    }
}

// ============== Padded Frame Tests ==============

Y_UNIT_TEST_SUITE(Http2PaddedFrames) {
    Y_UNIT_TEST(PaddedDataFrame) {
        TRawServerSession srv;

        // First, create stream 1 with headers
        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "POST"}, {":path", "/padded"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        srv.Error.clear();

        // Build PADDED DATA frame manually
        // Payload: [pad_length=3] [actual_data="hello"] [padding="\0\0\0"]
        TString payload;
        payload.push_back(3); // pad length
        payload.append("hello");
        payload.append(TString(3, '\0')); // padding

        srv.Feed(BuildRawFrame(EFrameType::DATA, NFrameFlag::PADDED | NFrameFlag::END_STREAM, 1, payload));
        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Body, "hello");
    }

    Y_UNIT_TEST(PaddedDataFrameEmptyPayload) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "POST"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        srv.Error.clear();

        // PADDED DATA with empty payload → error (needs at least 1 byte for pad length)
        srv.Feed(BuildRawFrame(EFrameType::DATA, NFrameFlag::PADDED, 1, ""));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(PaddedDataFramePadOverflow) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "POST"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        srv.Error.clear();

        // Pad length exceeds payload: [pad_length=10] [only 3 bytes of data]
        TString payload;
        payload.push_back(10); // pad length = 10
        payload.append("abc"); // only 3 bytes after pad length
        srv.Feed(BuildRawFrame(EFrameType::DATA, NFrameFlag::PADDED | NFrameFlag::END_STREAM, 1, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(PaddedHeadersFrame) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "GET"}, {":path", "/padded-headers"}, {":scheme", "https"}, {":authority", "host"},
        });

        // Build PADDED HEADERS: [pad_length=2] [header_block] [padding="\0\0"]
        TString payload;
        payload.push_back(2); // pad length
        payload.append(headerBlock);
        payload.append(TString(2, '\0')); // padding

        srv.Feed(BuildRawFrame(EFrameType::HEADERS,
            NFrameFlag::PADDED | NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, 1, payload));

        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Path, "/padded-headers");
    }

    Y_UNIT_TEST(PaddedHeadersWithPriority) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "GET"}, {":path", "/padded-prio"}, {":scheme", "https"}, {":authority", "host"},
        });

        // Build PADDED+PRIORITY HEADERS:
        // [pad_length=1] [stream_dep(4 bytes)] [weight(1 byte)] [header_block] [padding]
        TString payload;
        payload.push_back(1); // pad length
        // Stream dependency (4 bytes): 0
        payload.append(TString(4, '\0'));
        // Weight (1 byte): 15
        payload.push_back(15);
        payload.append(headerBlock);
        payload.append(TString(1, '\0')); // padding

        srv.Feed(BuildRawFrame(EFrameType::HEADERS,
            NFrameFlag::PADDED | NFrameFlag::PRIORITY | NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM,
            1, payload));

        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Path, "/padded-prio");
    }

    Y_UNIT_TEST(PaddedHeadersEmptyPayload) {
        TRawServerSession srv;
        // PADDED HEADERS with empty payload → FRAME_SIZE_ERROR
        srv.Feed(BuildRawFrame(EFrameType::HEADERS,
            NFrameFlag::PADDED | NFrameFlag::END_HEADERS, 1, ""));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(PaddedHeadersPadOverflow) {
        TRawServerSession srv;

        // Pad length exceeds available header block space
        TString payload;
        payload.push_back(100); // pad length = 100
        payload.append("abc"); // only 3 bytes
        srv.Feed(BuildRawFrame(EFrameType::HEADERS,
            NFrameFlag::PADDED | NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, 1, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(HeadersWithPriorityTooSmall) {
        TRawServerSession srv;
        // PRIORITY flag requires 5 extra bytes. Payload is too small.
        TString payload(3, '\0');
        srv.Feed(BuildRawFrame(EFrameType::HEADERS,
            NFrameFlag::PRIORITY | NFrameFlag::END_HEADERS | NFrameFlag::END_STREAM, 1, payload));
        UNIT_ASSERT(!srv.Error.empty());
    }

    Y_UNIT_TEST(PaddedDataFrameZeroPadding) {
        TRawServerSession srv;

        THPackEncoder encoder;
        TString headerBlock = encoder.Encode({
            {":method", "POST"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"},
        });
        srv.Feed(TFrameBuilder::BuildHeaders(1, NFrameFlag::END_HEADERS, headerBlock));
        srv.Error.clear();

        // PADDED with pad_length=0 (valid, just the pad_length byte)
        TString payload;
        payload.push_back(0); // pad length = 0
        payload.append("data");
        srv.Feed(BuildRawFrame(EFrameType::DATA, NFrameFlag::PADDED | NFrameFlag::END_STREAM, 1, payload));
        UNIT_ASSERT_C(srv.Error.empty(), srv.Error);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(srv.Requests[0].Body, "data");
    }
}

// ============== Status Code Mapping Tests ==============

Y_UNIT_TEST_SUITE(Http2StatusCodeMapping) {
    Y_UNIT_TEST(AllStatusCodes) {
        // Test all status code → reason phrase mappings in StreamToIncomingResponse
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/");

        struct StatusTest {
            TString Code;
            TString ExpectedPhrase;
        };
        TVector<StatusTest> tests = {
            {"200", "OK"},
            {"201", "Created"},
            {"204", "No Content"},
            {"301", "Moved Permanently"},
            {"302", "Found"},
            {"304", "Not Modified"},
            {"400", "Bad Request"},
            {"401", "Unauthorized"},
            {"403", "Forbidden"},
            {"404", "Not Found"},
            {"500", "Internal Server Error"},
            {"502", "Bad Gateway"},
            {"503", "Service Unavailable"},
            {"504", "Gateway Timeout"},
            {"418", "Unknown"}, // not in the switch
        };

        for (const auto& test : tests) {
            TStream stream;
            stream.Status = test.Code;
            stream.Headers = {{"content-type", "text/plain"}};
            stream.Body = "body";

            auto response = StreamToIncomingResponse(stream, request);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL_C(TString(response->Status), test.Code,
                "Status mismatch for " + test.Code);
            UNIT_ASSERT_C(TString(response->Message).Contains(test.ExpectedPhrase),
                "Expected phrase '" + test.ExpectedPhrase + "' for " + test.Code +
                " but got '" + TString(response->Message) + "'");
        }
    }

    Y_UNIT_TEST(StreamToIncomingResponseWithBody) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/");

        TStream stream;
        stream.Status = "200";
        stream.Headers = {{"content-type", "application/json"}};
        stream.Body = R"({"key":"value"})";

        auto response = StreamToIncomingResponse(stream, request);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Status), "200");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Body), stream.Body);
    }
}

// Helper: run a raw HTTP/2 server on the given port.
// Accepts one TCP connection, performs HTTP/2 handshake, receives requests,
// calls requestHandler for each, then closes.
// requestHandler returns {status, headers, body} for each request.
struct TH2ServerRequest {
    TString Method;
    TString Path;
    TString Authority;
    TString Body;
};

struct TH2ServerResponse {
    TString Status;
    TVector<std::pair<TString, TString>> Headers;
    TString Body;
};

static void RunH2Server(
    TIpPort port,
    std::function<TH2ServerResponse(const TH2ServerRequest&)> requestHandler,
    TString& serverError,
    std::atomic<bool>& serverReady,
    std::atomic<bool>& serverDone,
    int expectedRequests = 1)
{
    int listenFd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (listenFd < 0) {
        serverError = "socket() failed";
        serverReady = true;
        serverDone = true;
        return;
    }
    int one = 1;
    ::setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in6 addr = {};
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_loopback;
    if (::bind(listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        serverError = TStringBuilder() << "bind() failed: " << strerror(errno);
        ::close(listenFd);
        serverReady = true;
        serverDone = true;
        return;
    }
    if (::listen(listenFd, 1) < 0) {
        serverError = "listen() failed";
        ::close(listenFd);
        serverReady = true;
        serverDone = true;
        return;
    }
    serverReady = true;

    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    ::setsockopt(listenFd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int fd = ::accept(listenFd, nullptr, nullptr);
    ::close(listenFd);
    if (fd < 0) {
        serverError = "accept() timed out";
        serverDone = true;
        return;
    }
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Set up HTTP/2 server session.
    // We need to capture a pointer to the session from inside the OnRequest callback,
    // so we use a raw pointer that is set after construction.
    TString outputData;
    int responsesCount = 0;
    NHttp::NHttp2::TSession* sessionPtr = nullptr;

    NHttp::NHttp2::TSessionCallbacks callbacks;
    callbacks.OnRequest = [&](uint32_t streamId, NHttp::NHttp2::TStream& stream) {
        TH2ServerRequest req;
        req.Method = stream.Method;
        req.Path = stream.Path;
        req.Authority = stream.Authority;
        req.Body = stream.Body;

        TH2ServerResponse resp = requestHandler(req);
        sessionPtr->SendResponse(streamId, resp.Status, resp.Headers, resp.Body);
        responsesCount++;
    };
    callbacks.OnSend = [&](TString data) {
        outputData.append(data);
    };
    callbacks.OnError = [&](TString error) {
        serverError = "H2 server error: " + error;
    };

    NHttp::NHttp2::TSession session(NHttp::NHttp2::TSession::ERole::Server, std::move(callbacks));
    sessionPtr = &session;
    session.Initialize();

    // Flush server preface (SETTINGS frame is queued during Initialize)
    // — actually for server role, no preface is sent until client preface arrives.
    // But the OnSend callback fires with SETTINGS during Initialize.
    if (!outputData.empty()) {
        const char* buf = outputData.data();
        size_t remaining = outputData.size();
        while (remaining > 0) {
            ssize_t sent = ::send(fd, buf, remaining, 0);
            if (sent <= 0) { serverError = "send() failed"; ::close(fd); serverDone = true; return; }
            buf += sent;
            remaining -= sent;
        }
        outputData.clear();
    }

    // Read/write loop
    auto deadline = TInstant::Now() + TDuration::Seconds(5);
    char readBuf[16384];
    while (responsesCount < expectedRequests && TInstant::Now() < deadline && serverError.empty()) {
        ssize_t n = ::recv(fd, readBuf, sizeof(readBuf), 0);
        if (n > 0) {
            session.Feed(readBuf, static_cast<size_t>(n));

            // Flush any pending output
            if (!outputData.empty()) {
                const char* buf = outputData.data();
                size_t remaining = outputData.size();
                while (remaining > 0) {
                    ssize_t sent = ::send(fd, buf, remaining, 0);
                    if (sent <= 0) break;
                    buf += sent;
                    remaining -= sent;
                }
                outputData.clear();
            }
        } else if (n == 0) {
            break;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            break;
        }
    }

    ::close(fd);
    serverDone = true;
}

Y_UNIT_TEST_SUITE(Http2OutgoingIntegration) {

    Y_UNIT_TEST(OutgoingH2cPriorKnowledge) {
        // Test outgoing HTTP/2 over cleartext TCP (h2c prior-knowledge).
        // Actor system sends TEvHttpOutgoingRequest with UseHttp2=true.
        // A raw HTTP/2 server thread accepts the connection and responds.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        TString serverError;
        std::atomic<bool> serverReady{false};
        std::atomic<bool> serverDone{false};

        std::thread serverThread([port, &serverError, &serverReady, &serverDone]() {
            RunH2Server(port, [](const TH2ServerRequest& req) -> TH2ServerResponse {
                UNIT_ASSERT_VALUES_EQUAL(req.Method, "GET");
                UNIT_ASSERT_VALUES_EQUAL(req.Path, "/h2out");
                return {"200", {{"content-type", "text/plain"}}, "h2c-response"};
            }, serverError, serverReady, serverDone);
        });

        // Wait for server to be ready
        while (!serverReady) {
            Sleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(serverError.empty(), "Server error: " + serverError);

        // Send outgoing HTTP/2 request through actor system
        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/h2out");
        auto* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->UseHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_C(response->Error.empty(), "Response error: " + response->Error);
        UNIT_ASSERT(response->Response);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Status), "200");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Body), "h2c-response");

        serverThread.join();
        UNIT_ASSERT_C(serverError.empty(), "Server error after join: " + serverError);
    }

    Y_UNIT_TEST(OutgoingH2cWithBody) {
        // Test outgoing HTTP/2 POST with request body.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        TString serverError;
        std::atomic<bool> serverReady{false};
        std::atomic<bool> serverDone{false};

        TString capturedBody;
        std::thread serverThread([port, &serverError, &serverReady, &serverDone, &capturedBody]() {
            RunH2Server(port, [&capturedBody](const TH2ServerRequest& req) -> TH2ServerResponse {
                UNIT_ASSERT_VALUES_EQUAL(req.Method, "POST");
                UNIT_ASSERT_VALUES_EQUAL(req.Path, "/h2post");
                capturedBody = req.Body;
                return {"201", {{"content-type", "text/plain"}}, "created"};
            }, serverError, serverReady, serverDone);
        });

        while (!serverReady) {
            Sleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(serverError.empty(), "Server error: " + serverError);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost("http://[::1]:" + ToString(port) + "/h2post", "text/plain", "hello-h2-body");
        auto* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->UseHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_C(response->Error.empty(), "Response error: " + response->Error);
        UNIT_ASSERT(response->Response);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Status), "201");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Body), "created");
        UNIT_ASSERT_VALUES_EQUAL(capturedBody, "hello-h2-body");

        serverThread.join();
        UNIT_ASSERT_C(serverError.empty(), "Server error after join: " + serverError);
    }

    Y_UNIT_TEST(OutgoingH2cWithLargeResponseBody) {
        // Test outgoing HTTP/2 request receiving a large response body.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        TString serverError;
        std::atomic<bool> serverReady{false};
        std::atomic<bool> serverDone{false};

        TString largeBody(32768, 'X'); // 32KB response
        std::thread serverThread([port, &serverError, &serverReady, &serverDone, &largeBody]() {
            RunH2Server(port, [&largeBody](const TH2ServerRequest& /*req*/) -> TH2ServerResponse {
                return {"200", {{"content-type", "application/octet-stream"}}, largeBody};
            }, serverError, serverReady, serverDone);
        });

        while (!serverReady) {
            Sleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(serverError.empty(), "Server error: " + serverError);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/large");
        auto* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->UseHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_C(response->Error.empty(), "Response error: " + response->Error);
        UNIT_ASSERT(response->Response);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Status), "200");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Body), largeBody);

        serverThread.join();
        UNIT_ASSERT_C(serverError.empty(), "Server error after join: " + serverError);
    }

    Y_UNIT_TEST(OutgoingH2cEndToEndThroughActorSystem) {
        // End-to-end test: outgoing request with UseHttp2=true goes through
        // the actor system to an HTTP/2-enabled actor-system server.
        // Both client and server are actors — verifies the full round-trip.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        // Start a server with AllowHttp2 = true
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->AllowHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), add.Release()), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/e2e", serverId)), 0, true);

        // Send outgoing HTTP/2 request through the same proxy
        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/e2e");
        auto* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->UseHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        // Grab incoming request on server side
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_VALUES_EQUAL(TString(request->Request->URL), "/e2e");
        UNIT_ASSERT_VALUES_EQUAL(TString(request->Request->Method), "GET");

        // Send response
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseOK("e2e-ok", "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        // Grab response on client side
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_C(response->Error.empty(), "Response error: " + response->Error);
        UNIT_ASSERT(response->Response);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Status), "200");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Body), "e2e-ok");
    }

    Y_UNIT_TEST(OutgoingH1FallbackWhenHttp2NotRequested) {
        // Verify that when UseHttp2 is NOT set, the outgoing request uses HTTP/1.1
        // even if the server supports HTTP/2.
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        // Server with AllowHttp2 = true (supports both)
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->AllowHttp2 = true;
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), add.Release()), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/h1fall", serverId)), 0, true);

        // Send outgoing request WITHOUT UseHttp2 — should use HTTP/1.1
        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/h1fall");
        auto* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        // UseHttp2 defaults to false
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        // Grab incoming request on server side (should arrive as HTTP/1.1)
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_VALUES_EQUAL(TString(request->Request->URL), "/h1fall");
        UNIT_ASSERT_VALUES_EQUAL(TString(request->Request->Method), "GET");

        // Respond
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseOK("h1-ok", "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_C(response->Error.empty(), "Response error: " + response->Error);
        UNIT_ASSERT(response->Response);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Status), "200");
        UNIT_ASSERT_VALUES_EQUAL(TString(response->Response->Body), "h1-ok");
    }
}
