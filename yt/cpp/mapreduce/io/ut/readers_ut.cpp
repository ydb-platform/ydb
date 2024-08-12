#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/proto_table_reader.h>
#include <yt/cpp/mapreduce/io/skiff_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>
#include <yt/cpp/mapreduce/io/yamr_table_reader.h>

#include <yt/cpp/mapreduce/io/ut/ut_row.pb.h>

#include <yt/cpp/mapreduce/skiff/checked_parser.h>
#include <yt/cpp/mapreduce/skiff/skiff_schema.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NSkiff;

////////////////////////////////////////////////////////////////////

class TRetryEmulatingRawTableReader
    : public TRawTableReader
{
public:
    TRetryEmulatingRawTableReader(const TString& string)
        : String_(string)
        , Stream_(String_)
    { }

    bool Retry(
        const TMaybe<ui32>& /*rangeIndex*/,
        const TMaybe<ui64>& /*rowIndex*/,
        const std::exception_ptr& /*error*/) override
    {
        if (RetriesLeft_ == 0) {
            return false;
        }
        Stream_ = TStringStream(String_);
        --RetriesLeft_;
        return true;
    }

    void ResetRetries() override
    {
        RetriesLeft_ = 10;
    }

    bool HasRangeIndices() const override
    {
        return false;
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        switch (DoReadCallCount_++) {
            case 0:
                return Stream_.Read(buf, std::min(len, String_.size() / 2));
            case 1:
                ythrow yexception() << "Just wanted to test you, first fail";
            case 2:
                ythrow yexception() << "Just wanted to test you, second fail";
            default:
                return Stream_.Read(buf, len);
        }
    }

private:
    const TString String_;
    TStringStream Stream_;
    int RetriesLeft_ = 10;
    int DoReadCallCount_ = 0;
};

////////////////////////////////////////////////////////////////////

TEST(TReadersTest, YsonGood)
{
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>("{a=13;b = \"string\"}; {c = {d=12}}");

    TNodeTableReader reader(proxy);
    TVector<TNode> expectedRows = {TNode()("a", 13)("b", "string"), TNode()("c", TNode()("d", 12))};
    for (const auto& expectedRow : expectedRows) {
        EXPECT_TRUE(reader.IsValid());
        EXPECT_TRUE(!reader.IsRawReaderExhausted());
        EXPECT_EQ(reader.GetRow(), expectedRow);
        reader.Next();
    }
    EXPECT_TRUE(!reader.IsValid());
    EXPECT_TRUE(reader.IsRawReaderExhausted());
}

TEST(TReadersTest, YsonBad)
{
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>("{a=13;-b := \"string\"}; {c = {d=12}}");
    EXPECT_THROW(TNodeTableReader(proxy).GetRow(), yexception);
}

TEST(TReadersTest, SkiffGood)
{
    const char arr[] = "\x00\x00" "\x94\x88\x01\x00\x00\x00\x00\x00" "\x06\x00\x00\x00""foobar" "\x01"
                       "\x00\x00" "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF" "\x03\x00\x00\x00""abc"    "\x00";
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

    TSkiffSchemaPtr schema = CreateVariant16Schema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("a"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("b"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("c")
        })
    });

    TSkiffTableReader reader(proxy, schema);
    TVector<TNode> expectedRows = {
        TNode()("a", 100500)("b", "foobar")("c", true),
        TNode()("a", -1)("b", "abc")("c", false),
    };
    for (const auto& expectedRow : expectedRows) {
        EXPECT_TRUE(reader.IsValid());
        EXPECT_TRUE(!reader.IsRawReaderExhausted());
        EXPECT_EQ(reader.GetRow(), expectedRow);
        reader.Next();
    }
    EXPECT_TRUE(!reader.IsValid());
    EXPECT_TRUE(reader.IsRawReaderExhausted());
}

TEST(TReadersTest, SkiffExtraColumns)
{
    const char arr[] = "\x00\x00" "\x7B\x00\x00\x00\x00\x00\x00\x00";
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

    TSkiffSchemaPtr schema = CreateVariant16Schema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$timestamp")
        })
    });

    TSkiffTableReader reader(proxy, schema);
    TVector<TNode> expectedRows = {
        TNode()("$timestamp", 123u),
    };
    for (const auto& expectedRow : expectedRows) {
        EXPECT_TRUE(reader.IsValid());
        EXPECT_TRUE(!reader.IsRawReaderExhausted());
        EXPECT_EQ(reader.GetRow(), expectedRow);
        reader.Next();
    }
    EXPECT_TRUE(!reader.IsValid());
    EXPECT_TRUE(reader.IsRawReaderExhausted());
}

TEST(TReadersTest, SkiffBad)
{
    const char arr[] = "\x00\x00" "\x94\x88\x01\x00\x00\x00\x00\x00" "\xFF\x00\x00\x00""foobar" "\x01";
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

    TSkiffSchemaPtr schema = CreateVariant16Schema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("a"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("b"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("c")
        })
    });

    EXPECT_THROW(TSkiffTableReader(proxy, schema).GetRow(), yexception);
}

TEST(TReadersTest, SkiffBadFormat)
{
    const char arr[] = "\x00\x00" "\x12" "\x23\x34\x00\x00";
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

    TSkiffSchemaPtr schema = CreateVariant16Schema({
        CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Int32)
            })
        })
    });

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TSkiffTableReader(proxy, schema).GetRow(),
        yexception,
        "Tag for 'variant8<nothing,int32>' expected to be 0 or 1");
}

TEST(TReadersTest, ProtobufGood)
{
    using NYT::NTesting::TRow;

    const char arr[] = "\x13\x00\x00\x00" "\x0A""\x06""foobar" "\x10""\x0F" "\x19""\x94\x88\x01\x00\x00\x00\x00\x00"
                       "\x10\x00\x00\x00" "\x0A""\x03""abc"    "\x10""\x1F" "\x19""\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

    TLenvalProtoTableReader reader(proxy, {TRow::descriptor()});
    TRow row1, row2;
    row1.set_string_field("foobar");
    row1.set_int32_field(15);
    row1.set_fixed64_field(100500);

    row2.set_string_field("abc");
    row2.set_int32_field(31);
    row2.set_fixed64_field(-1);

    TVector<TRow> expectedRows = {row1, row2};
    for (const auto& expectedRow : expectedRows) {
        TRow row;
        EXPECT_TRUE(reader.IsValid());
        EXPECT_TRUE(!reader.IsRawReaderExhausted());
        reader.ReadRow(&row);
        EXPECT_EQ(row.string_field(), expectedRow.string_field());
        EXPECT_EQ(row.int32_field(), expectedRow.int32_field());
        EXPECT_EQ(row.fixed64_field(), expectedRow.fixed64_field());
        reader.Next();
    }
    EXPECT_TRUE(!reader.IsValid());
    EXPECT_TRUE(reader.IsRawReaderExhausted());
}

TEST(TReadersTest, ProtobufBad)
{
    const char arr[] = "\x13\x00\x00\x00" "\x0F""\x06""foobar" "\x10""\x0F" "\x19""\x94\x88\x01\x00\x00\x00\x00\x00";
    auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

    NYT::NTesting::TRow row;
    EXPECT_THROW(TLenvalProtoTableReader(proxy, { NYT::NTesting::TRow::descriptor() }).ReadRow(&row), yexception);
}

////////////////////////////////////////////////////////////////////
