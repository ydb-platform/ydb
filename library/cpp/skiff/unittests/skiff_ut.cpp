#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/stream/buffer.h>
#include <util/stream/buffered.h>
#include <util/stream/mem.h>
#include <util/string/hex.h>

#include <limits>

using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

static TString HexEncode(const TBuffer& buffer)
{
    auto result = HexEncode(buffer.Data(), buffer.Size());
    result.to_lower();
    return result;
}

Y_UNIT_TEST_SUITE(Skiff)
{
    Y_UNIT_TEST(TestInt8)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int8);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteInt8(42);
        tokenWriter.WriteInt8(-42);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "2a"
            "d6");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt8(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt8(), -42);
    }

    Y_UNIT_TEST(TestInt16)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int16);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteInt16(0x1234);
        tokenWriter.WriteInt16(-0x1234);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "3412"
            "cced");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt16(), 0x1234);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt16(), -0x1234);
    }

    Y_UNIT_TEST(TestInt32)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int32);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteInt32(0x12345678);
        tokenWriter.WriteInt32(-0x12345678);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "78563412"
            "88a9cbed");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt32(), 0x12345678);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt32(), -0x12345678);
    }

    Y_UNIT_TEST(TestInt64)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int64);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteInt64(-42);
        tokenWriter.WriteInt64(100500);
        tokenWriter.WriteInt64(-0x123456789abcdef0);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "d6ffffffffffffff"
            "9488010000000000"
            "1021436587a9cbed");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), -42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), -0x123456789abcdef0);
    }

    Y_UNIT_TEST(TestUint8)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint8);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteUint8(42);
        tokenWriter.WriteUint8(200);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "2a"
            "c8");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint8(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint8(), 200);
    }

    Y_UNIT_TEST(TestUint16)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint16);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteUint16(0x1234);
        tokenWriter.WriteUint16(0xfedc);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "3412"
            "dcfe");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint16(), 0x1234);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint16(), 0xfedc);
    }

    Y_UNIT_TEST(TestUint32)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint32);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteUint32(0x12345678);
        tokenWriter.WriteUint32(0x87654321);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "78563412"
            "21436587");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint32(), 0x12345678);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint32(), 0x87654321);
    }


    Y_UNIT_TEST(TestUint64)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint64);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteUint64(42);
        tokenWriter.WriteUint64(100500);
        tokenWriter.WriteUint64(0x123456789abcdef0);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "2a00000000000000"
            "9488010000000000"
            "f0debc9a78563412");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 0x123456789abcdef0);
    }

    Y_UNIT_TEST(TestInt128)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int128);

        const TInt128 val1 = {0x1924cd4aeb9ced82,  0x0885e83f456d6a7e};
        const TInt128 val2 = {0xe9ba36585eccae1a, -0x7854b6f9ce448be9};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        writer.WriteInt128(val1);
        writer.WriteInt128(val2);
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "82ed9ceb4acd2419" "7e6a6d453fe88508"
            "1aaecc5e5836bae9" "1774bb310649ab87");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_EQUAL(parser.ParseInt128(), val1);
        UNIT_ASSERT_EQUAL(parser.ParseInt128(), val2);
    }

    Y_UNIT_TEST(TestInt256)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int256);

        const TInt256 val1 = {0x1924cd4aeb9ced82,  0x0885e83f456d6a7e, 0xe9ba36585eccae1a, 0x7854b6f9ce448be9};
        const TInt256 val2 = {0xe9ba36585eccae1a, 0x1924cd4aeb9ced82, 0x0885e83f456d6a7e, static_cast<ui64>(-0x7854b6f9ce448be9)};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        writer.WriteInt256(val1);
        writer.WriteInt256(val2);
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "82ed9ceb4acd2419" "7e6a6d453fe88508" "1aaecc5e5836bae9" "e98b44cef9b65478"
            "1aaecc5e5836bae9" "82ed9ceb4acd2419" "7e6a6d453fe88508" "1774bb310649ab87");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_EQUAL(parser.ParseInt256(), val1);
        UNIT_ASSERT_EQUAL(parser.ParseInt256(), val2);
    }

    Y_UNIT_TEST(TestUint128)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint128);

        const auto val1 = TUint128{0x1924cd4aeb9ced82,  0x0885e83f456d6a7e};
        const auto val2 = TUint128{0xe9ba36585eccae1a,  0x8854b6f9ce448be9};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        writer.WriteUint128(val1);
        writer.WriteUint128(val2);
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "82ed9ceb4acd2419" "7e6a6d453fe88508"
            "1aaecc5e5836bae9" "e98b44cef9b65488");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_EQUAL(parser.ParseUint128(), val1);
        UNIT_ASSERT_EQUAL(parser.ParseUint128(), val2);
    }

    Y_UNIT_TEST(TestUint256)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint256);

        const auto val1 = TUint256{0x1924cd4aeb9ced82,  0x7854b6f9ce448be9, 0x8854b6f9ce448be9, 0x0885e83f456d6a7e};
        const auto val2 = TUint256{0xe9ba36585eccae1a,  0x8854b6f9ce448be9, 0x1924cd4aeb9ced82, 0xabacabadabacaba0};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        writer.WriteUint256(val1);
        writer.WriteUint256(val2);
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "82ed9ceb4acd2419" "e98b44cef9b65478" "e98b44cef9b65488" "7e6a6d453fe88508"
            "1aaecc5e5836bae9" "e98b44cef9b65488" "82ed9ceb4acd2419" "a0abacabadabacab");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_EQUAL(parser.ParseUint256(), val1);
        UNIT_ASSERT_EQUAL(parser.ParseUint256(), val2);
    }

    Y_UNIT_TEST(TestVarInt32)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::VarInt32);

        const auto values = std::vector<i32>{
            0, -1, 32, -32, 64, -128,
            16384, -16384, 2147483647, -2147483648};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        for (auto value : values) {
            writer.WriteVarInt32(value);
        }
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "0001403f8001ff01" "808002ffff01feff" "ffff0fffffffff0f");

        TCheckedSkiffParser parser(schema, &bufferStream);
        for (auto value : values) {
            UNIT_ASSERT_EQUAL(parser.ParseVarInt32(), value);
        }

        for (size_t chunkSize : {1, 3, 7}) {
            TBufferInput bufferInput(bufferStream.Buffer());
            TBufferedInput chunkedInput(&bufferInput, chunkSize);
            TUncheckedSkiffParser chunkedParser(&chunkedInput);
            for (auto value : values) {
                UNIT_ASSERT_VALUES_EQUAL(chunkedParser.ParseVarInt32(), value);
            }
            UNIT_ASSERT(!chunkedParser.HasMoreData());
        }
    }

    Y_UNIT_TEST(TestVarInt64)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::VarInt64);

        const auto values = std::vector<i64>{
            0, -1, 32, -32, 64, -128, 16384, -16384,
            2147483647, -2147483648, 4294967295, -4294967296,
            0x1924cd4aeb9ced82, -0x1924cd4aeb9ced82,
            std::numeric_limits<i64>::max(), std::numeric_limits<i64>::min(), 0x4000000000000000};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        for (auto value : values) {
            writer.WriteVarInt64(value);
        }
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "0001403f8001ff01" "808002ffff01feff" "ffff0fffffffff0f"
            "feffffff1fffffff" "ff1f84b6e7b9ddd2" "e6a43283b6e7b9dd" "d2e6a432feffffff"
            "ffffffffff01ffff" "ffffffffffffff01" "8080808080808080" "8001");

        TCheckedSkiffParser parser(schema, &bufferStream);
        for (auto value : values) {
            UNIT_ASSERT_EQUAL(parser.ParseVarInt64(), value);
        }

        for (size_t chunkSize : {1, 3, 7}) {
            TBufferInput bufferInput(bufferStream.Buffer());
            TBufferedInput chunkedInput(&bufferInput, chunkSize);
            TUncheckedSkiffParser chunkedParser(&chunkedInput);
            for (auto value : values) {
                UNIT_ASSERT_VALUES_EQUAL(chunkedParser.ParseVarInt64(), value);
            }
            UNIT_ASSERT(!chunkedParser.HasMoreData());
        }
    }

    Y_UNIT_TEST(TestMalformedVarInt)
    {
        // The first value of a fresh parser is always read byte-by-byte; the prefix loads the buffer.
        auto parseVarInt32 = [] (TStringBuf hex, size_t chunkSize = 0) {
            auto data = HexDecode(TString::Join("00", hex));
            TMemoryInput memoryInput(data);
            TBufferedInput chunkedInput(&memoryInput, chunkSize ? chunkSize : data.size());
            TUncheckedSkiffParser parser(&chunkedInput);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVarInt32(), 0);
            return parser.ParseVarInt32();
        };
        auto parseVarInt64 = [] (TStringBuf hex, size_t chunkSize = 0) {
            auto data = HexDecode(TString::Join("00", hex));
            TMemoryInput memoryInput(data);
            TBufferedInput chunkedInput(&memoryInput, chunkSize ? chunkSize : data.size());
            TUncheckedSkiffParser parser(&chunkedInput);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVarInt64(), 0);
            return parser.ParseVarInt64();
        };

        // More than 10 bytes with the continuation bit set.
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt64("ffffffffffffffffffffff"), TSkiffException, "Value is too big for varuint64");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffffffffffffffffff"), TSkiffException, "Value is too big for varuint64");

        // Same, fed one byte at a time.
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt64("ffffffffffffffffffffff", 1), TSkiffException, "Value is too big for varuint64");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffffffffffffffffff", 1), TSkiffException, "Value is too big for varuint64");

        // Exactly 10 bytes with the continuation bit set.
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt64("ffffffffffffffffffff"), TSkiffException, "Value is too big for varuint64");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt64("ffffffffffffffffffff", 1), TSkiffException, "Premature end of stream");

        // Maximum varint32 length, never terminated.
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffffff"), TSkiffException, "Premature end of data");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffffff", 1), TSkiffException, "Premature end of stream");

        // Well-formed varints whose value does not fit into 32 bits.
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffff7f"), TSkiffException, "Value is too big for varint32");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffffff01"), TSkiffException, "Value is too big for varint32");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffff7f", 1), TSkiffException, "Value is too big for varint32");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffffffffff01", 1), TSkiffException, "Value is too big for varint32");

        // Stream ends inside the varint.
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt32("ffff"), TSkiffException, "Premature end of stream");
        UNIT_ASSERT_EXCEPTION_CONTAINS(parseVarInt64("ffff"), TSkiffException, "Premature end of stream");
    }

    Y_UNIT_TEST(TestBoolean)
    {
        auto schema = CreateSimpleTypeSchema(EWireType::Boolean);

        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteBoolean(true);
        tokenWriter.WriteBoolean(false);
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseBoolean(), true);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseBoolean(), false);

        {
            TBufferStream bufferStream;
            bufferStream.Write('\x02');

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION(parser.ParseBoolean(), std::exception);
        }
    }

    Y_UNIT_TEST(TestFloat)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Float);

        const auto values = std::vector<float>{
            0.0f, 0.00000000001f, 1.0f, 1.3f, 0.5f, 3.1415926f, -1.0f,
            std::numeric_limits<float>::infinity(), std::numeric_limits<float>::quiet_NaN()};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        for (auto value : values) {
            writer.WriteFloat(value);
        }
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "00000000ffeb2f2d" "0000803f6666a63f" "0000003fda0f4940" "000080bf0000807f" "0000c07f");

        TCheckedSkiffParser parser(schema, &bufferStream);
        for (auto value : values) {
            auto parsed = parser.ParseFloat();
            if (std::isnan(value)) {
                UNIT_ASSERT(std::isnan(parsed));
            } else {
                UNIT_ASSERT_EQUAL(parsed, value);
            }
        }
    }

    Y_UNIT_TEST(TestDouble)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Double);

        const auto values = std::vector<double>{
            0.0, 0.00000000001, 1.0, 1.3, 0.5, 3.1415926, -1.0,
            std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()};

        TCheckedSkiffWriter writer(schema, &bufferStream);
        for (auto value : values) {
            writer.WriteDouble(value);
        }
        writer.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "0000000000000000" "956479e17ffda53d" "000000000000f03f" "cdccccccccccf43f"
            "000000000000e03f" "4ad8124dfb210940" "000000000000f0bf" "000000000000f07f" "000000000000f87f");

        TCheckedSkiffParser parser(schema, &bufferStream);
        for (auto value : values) {
            auto parsed = parser.ParseDouble();
            if (std::isnan(value)) {
                UNIT_ASSERT(std::isnan(parsed));
            } else {
                UNIT_ASSERT_EQUAL(parsed, value);
            }
        }
    }

    Y_UNIT_TEST(TestVariant8)
    {
        auto schema = CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        });

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(42), std::exception);
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariant8Tag(0);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(42), std::exception);
        }
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariant8Tag(1);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteInt64(42), std::exception);
        }

        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteVariant8Tag(0);
        tokenWriter.WriteVariant8Tag(1);
        tokenWriter.WriteUint64(42);
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 0);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

        parser.ValidateFinished();
    }

    Y_UNIT_TEST(TestVariantVar)
    {
        auto schema = CreateVariantVarSchema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        });

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteUint64(42), TSkiffException, "Unexpected parse/write of \"uint64\" token");
        }
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariantVarTag(0);
            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteUint64(42), TSkiffException, "Unexpected parse/write of \"uint64\" token");
        }
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariantVarTag(1);
            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteInt64(42), TSkiffException, "Unexpected parse/write of \"int64\" token");
        }
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteVariantVarTag(-1), TSkiffException, "Variant tag \"-1\" is out of range");
        }
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariantVarTag(0);
            tokenWriter.WriteVariantVarTag(1);
            tokenWriter.WriteUint64(42);
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariantVarTag(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariantVarTag(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

            parser.ValidateFinished();
        }
    }

    Y_UNIT_TEST(TestTuple)
    {

        auto schema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::String32),
        });

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteInt64(42);
            tokenWriter.WriteString32("foobar");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseString32(), "foobar");
            parser.ValidateFinished();
        }
    }

    Y_UNIT_TEST(TestEmptyTuple)
    {
        {
            auto schema = CreateTupleSchema({});

            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteInt64(42), TSkiffException, "Unexpected parse/write");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            parser.ValidateFinished();
        }

        {
            auto schema = CreateTupleSchema({
                CreateTupleSchema({}),
                CreateSimpleTypeSchema(EWireType::Int64)});

            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteInt64(42);
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);
            parser.ValidateFinished();
        }
    }

    Y_UNIT_TEST(TestString32)
    {
        auto schema = CreateSimpleTypeSchema(EWireType::String32);

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteString32("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseString32(), "foo");

            parser.ValidateFinished();
        }

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteString32("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION(parser.ParseInt64(), std::exception);
        }
    }

    Y_UNIT_TEST(TestStringVar)
    {
        auto schema = CreateSimpleTypeSchema(EWireType::StringVar);

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteStringVar("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseStringVar(), "foo");

            parser.ValidateFinished();
        }

        {
            TBufferStream bufferStream;

            TUncheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVarInt64(-1);
            tokenWriter.Finish();

            TUncheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(parser.ParseStringVar(), TSkiffException, "is out of range");
        }
    }

    Y_UNIT_TEST(TestStringLengthLimit)
    {
        {
            auto schema = CreateSimpleTypeSchema(EWireType::StringVar);
            TBufferStream bufferStream;

            TUncheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVarInt64(MaxStringLength + 1);
            tokenWriter.Finish();

            TUncheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(parser.ParseStringVar(), TSkiffException, "out of range");
        }

        UNIT_ASSERT_NO_EXCEPTION(CreateStringFixedSchema(MaxStringLength));
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateStringFixedSchema(-1), TSkiffException, "out of range");
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateStringFixedSchema(MaxStringLength + 1), TSkiffException, "out of range");
    }

    Y_UNIT_TEST(TestStringFixed)
    {
        auto schema = CreateStringFixedSchema(3);

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteStringFixed("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseStringFixed(3), "foo");

            parser.ValidateFinished();
        }

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteStringFixed("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(parser.ParseStringFixed(4), TSkiffException, "\"string_fixed\" size mismatch: expected 3, actual 4");
        }

        {
            auto emptySchema = CreateStringFixedSchema(0);
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(emptySchema, &bufferStream);
            tokenWriter.WriteStringFixed("");
            tokenWriter.Finish();

            UNIT_ASSERT_VALUES_EQUAL(bufferStream.Buffer().Size(), 0u);

            TCheckedSkiffParser parser(emptySchema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseStringFixed(0), "");

            parser.ValidateFinished();
        }
    }

    Y_UNIT_TEST(TestRepeatedVariant8)
    {

        auto schema = CreateRepeatedVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Uint64),
        });

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            // row 0
            tokenWriter.WriteVariant8Tag(0);
            tokenWriter.WriteInt64(-8);

            // row 2
            tokenWriter.WriteVariant8Tag(1);
            tokenWriter.WriteUint64(42);

            // end
            tokenWriter.WriteVariant8Tag(EndOfSequenceTag<ui8>());

            tokenWriter.Finish();

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                // row 1
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 0);
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), -8);

                // row 2
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 1);
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

                // end
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), EndOfSequenceTag<ui8>());

                parser.ValidateFinished();
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                UNIT_ASSERT_EXCEPTION(parser.ParseInt64(), std::exception);
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseVariant8Tag();
                UNIT_ASSERT_EXCEPTION(parser.ParseUint64(), std::exception);
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseVariant8Tag();
                parser.ParseInt64();

                UNIT_ASSERT_EXCEPTION(parser.ValidateFinished(), std::exception);
            }
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteVariant8Tag(0);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(5), std::exception);
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteVariant8Tag(1);
            tokenWriter.WriteUint64(5);

            UNIT_ASSERT_EXCEPTION(tokenWriter.Finish(), std::exception);
        }
    }

    Y_UNIT_TEST(TestRepeatedVariant16)
    {

        auto schema = CreateRepeatedVariant16Schema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Uint64),
        });

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            // row 0
            tokenWriter.WriteVariant16Tag(0);
            tokenWriter.WriteInt64(-8);

            // row 2
            tokenWriter.WriteVariant16Tag(1);
            tokenWriter.WriteUint64(42);

            // end
            tokenWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            // row 1
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), -8);

            // row 2
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

            // end
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

            parser.ValidateFinished();
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteVariant16Tag(0);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(5), std::exception);
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteVariant16Tag(1);
            tokenWriter.WriteUint64(5);

            UNIT_ASSERT_EXCEPTION(tokenWriter.Finish(), std::exception);
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            // row 0
            tokenWriter.WriteVariant16Tag(0);
            tokenWriter.WriteInt64(-8);

            // row 2
            tokenWriter.WriteVariant16Tag(1);
            tokenWriter.WriteUint64(42);

            // end
            tokenWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

            tokenWriter.Finish();

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                UNIT_ASSERT_EXCEPTION(parser.ParseInt64(), std::exception);
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseVariant16Tag();
                UNIT_ASSERT_EXCEPTION(parser.ParseUint64(), std::exception);
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseVariant16Tag();
                parser.ParseInt64();

                UNIT_ASSERT_EXCEPTION(parser.ValidateFinished(), std::exception);
            }
        }
    }

    Y_UNIT_TEST(TestRepeatedBlockVar)
    {
        auto schema = CreateRepeatedBlockVarSchema({
            CreateSimpleTypeSchema(EWireType::Uint64),
        });

        // All good, one block.
        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 2});

            tokenWriter.WriteUint64(0);
            tokenWriter.WriteUint64(42);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 0});

            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 2);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 0);

            parser.ValidateFinished();
        }

        // All good, block with byte size.
        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 2, .ByteSize = 16});

            tokenWriter.WriteUint64(0);
            tokenWriter.WriteUint64(42);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 0});

            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            auto header = parser.ParseBlockVarHeader();
            UNIT_ASSERT_VALUES_EQUAL(header.Count, 2);
            UNIT_ASSERT_VALUES_EQUAL(header.ByteSize, 16);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

            auto lastHeader = parser.ParseBlockVarHeader();
            UNIT_ASSERT_VALUES_EQUAL(lastHeader.Count, 0);
            UNIT_ASSERT(!lastHeader.ByteSize);

            parser.ValidateFinished();
        }

        // Invalid headers.
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 0, .ByteSize = 16}),
                TSkiffException,
                "Block with zero count must not have byte size");
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = -1, .ByteSize = 16}),
                TSkiffException,
                "Block count must be nonnegative");
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 1, .ByteSize = -1}),
                TSkiffException,
                "Block byte size must be nonnegative");
        }

        // INT64_MIN count is rejected by the writer.
        {
            TBufferStream bufferStream;
            TUncheckedSkiffWriter tokenWriter(&bufferStream);

            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = std::numeric_limits<i64>::min(), .ByteSize = 16}),
                TSkiffException,
                "INT64_MIN is not allowed");
        }

        // INT64_MIN count is rejected by the parser.
        {
            TBufferStream bufferStream;
            TUncheckedSkiffWriter tokenWriter(&bufferStream);
            tokenWriter.WriteVarInt64(std::numeric_limits<i64>::min());
            tokenWriter.Finish();

            TUncheckedSkiffParser parser(&bufferStream);
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                parser.ParseBlockVarHeader(),
                TSkiffException,
                "INT64_MIN is not allowed");
        }

        // All good, multiple blocks.
        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 2});

            tokenWriter.WriteUint64(0);
            tokenWriter.WriteUint64(42);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 3});

            tokenWriter.WriteUint64(0);
            tokenWriter.WriteUint64(42);
            tokenWriter.WriteUint64(1242);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 0});

            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 2);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 3);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 1242);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 0);

            parser.ValidateFinished();
        }

        // Wrong type.
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 1});

            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteInt64(42), TSkiffException, "Unexpected parse/write of \"int64\" token");
        }

        // Didn't write the final BlockVarHeader.
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 1});

            tokenWriter.WriteUint64(42);

            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.Finish(), TSkiffException, "Parse/write is not finished");
        }

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 2});

            tokenWriter.WriteUint64(0);
            tokenWriter.WriteUint64(42);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 0});

            tokenWriter.Finish();

            // Didn't parse BlockVarHeader.
            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                UNIT_ASSERT_EXCEPTION_CONTAINS(parser.ParseUint64(), TSkiffException, "Unexpected parse/write of \"uint64\" token");
            }

            // Wrong type.
            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseBlockVarHeader();
                UNIT_ASSERT_EXCEPTION_CONTAINS(parser.ParseInt64(), TSkiffException, "Unexpected parse/write of \"int64\" token");
            }

            // Didn't parse the second value.
            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseBlockVarHeader();
                parser.ParseUint64();

                UNIT_ASSERT_EXCEPTION_CONTAINS(parser.ValidateFinished(), TSkiffException, "Parse/write is not finished");
            }
        }

        // Negative Count.
        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = -2}),
                TSkiffException,
                "Block count must be nonnegative");
        }

        // Negative ByteSize.
        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 2, .ByteSize = -1}),
                TSkiffException,
                "Block byte size must be nonnegative");
        }

        // Too many values.
        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 2});

            tokenWriter.WriteUint64(0);
            tokenWriter.WriteUint64(42);
            UNIT_ASSERT_EXCEPTION_CONTAINS(tokenWriter.WriteUint64(1242), TSkiffException, "Unexpected parse/write of \"uint64\" token");
        }
    }

    Y_UNIT_TEST(TestUnboundedRecursion)
    {
        {
            auto schemas = std::vector{
                CreateRepeatedBlockVarSchema({
                    CreateSimpleTypeSchema(EWireType::Nothing)}),

                CreateRepeatedBlockVarSchema({
                    CreateTupleSchema({
                        CreateSimpleTypeSchema(EWireType::Nothing),
                        CreateTupleSchema({
                            CreateTupleSchema({}),
                            CreateSimpleTypeSchema(EWireType::Nothing),
                            CreateTupleSchema({
                                CreateSimpleTypeSchema(EWireType::Nothing)})}),
                        CreateSimpleTypeSchema(EWireType::Nothing)})})};

            for (const auto& schema : schemas) {
                TBufferStream bufferStream;

                TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

                // This used to cause a stack overflow due to deep recursion in the validator.
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 100'000'000});
                tokenWriter.WriteBlockVarHeader(TBlockVarHeader{.Count = 0});
                tokenWriter.Finish();

                TCheckedSkiffParser parser(schema, &bufferStream);
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 100'000'000);
                UNIT_ASSERT_VALUES_EQUAL(parser.ParseBlockVarHeader().Count, 0);
                parser.ValidateFinished();
            }
        }

        {
            auto children = TSkiffSchemaList(1'000'000, CreateSimpleTypeSchema(EWireType::Nothing));
            children.push_back(CreateSimpleTypeSchema(EWireType::Int64));
            auto schema = CreateTupleSchema(children);

            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            // This used to cause a stack overflow due to deep recursion in the validator.
            tokenWriter.WriteInt64(42);
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);
        }
    }

    Y_UNIT_TEST(TestStruct)
    {
        TBufferStream bufferStream;

        auto schema = CreateRepeatedVariant16Schema(
            {
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateTupleSchema({
                    CreateVariant8Schema({
                        CreateSimpleTypeSchema(EWireType::Nothing),
                        CreateSimpleTypeSchema(EWireType::Int64)
                    }),
                    CreateSimpleTypeSchema(EWireType::Uint64),
                })
            }
        );

        {
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            // row 0
            tokenWriter.WriteVariant16Tag(0);

            // row 1
            tokenWriter.WriteVariant16Tag(1);
            tokenWriter.WriteVariant8Tag(0);
            tokenWriter.WriteUint64(1);

            // row 2
            tokenWriter.WriteVariant16Tag(1);
            tokenWriter.WriteVariant8Tag(1);
            tokenWriter.WriteInt64(2);
            tokenWriter.WriteUint64(3);

            // end
            tokenWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

            tokenWriter.Finish();
        }

        TCheckedSkiffParser parser(schema, &bufferStream);

        // row 0
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 0);

        // row 1
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 0);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 1);

        // row 2
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 2);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 3);

        // end
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

        parser.ValidateFinished();
    }

    Y_UNIT_TEST(TestSimpleOutputStream)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Int8);

        TCheckedSkiffWriter tokenWriter(schema, static_cast<IOutputStream*>(&bufferStream));
        tokenWriter.WriteInt8(42);
        tokenWriter.WriteInt8(-42);
        tokenWriter.Finish();

        UNIT_ASSERT_VALUES_EQUAL(HexEncode(bufferStream.Buffer()),
            "2a"
            "d6");

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt8(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt8(), -42);
    }
}

////////////////////////////////////////////////////////////////////////////////
