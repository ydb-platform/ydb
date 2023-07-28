#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/stream/buffer.h>
#include <util/string/hex.h>

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

    Y_UNIT_TEST(TestString)
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
