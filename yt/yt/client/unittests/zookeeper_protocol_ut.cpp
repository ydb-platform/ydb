#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/zookeeper/protocol.h>

namespace NYT::NZookeeper {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TZookeeperProtocolTest, Simple)
{
    const TString LongString{100'000, 'a'};

    auto writer = CreateZookeeperProtocolWriter();
    writer->WriteByte('$');
    writer->WriteInt(42);
    writer->WriteInt(std::numeric_limits<int>::min());
    writer->WriteInt(std::numeric_limits<int>::max());
    writer->WriteLong(123);
    writer->WriteLong(std::numeric_limits<i64>::min());
    writer->WriteLong(std::numeric_limits<i64>::max());
    writer->WriteBool(false);
    writer->WriteBool(true);
    writer->WriteString("abacaba");
    writer->WriteString(LongString);
    writer->WriteString("");

    auto data = writer->Finish();
    auto reader = CreateZookeeperProtocolReader(std::move(data));
    EXPECT_EQ('$', reader->ReadByte());
    EXPECT_EQ(42, reader->ReadInt());
    EXPECT_EQ(std::numeric_limits<int>::min(), reader->ReadInt());
    EXPECT_EQ(std::numeric_limits<int>::max(), reader->ReadInt());
    EXPECT_EQ(123, reader->ReadLong());
    EXPECT_EQ(std::numeric_limits<i64>::min(), reader->ReadLong());
    EXPECT_EQ(std::numeric_limits<i64>::max(), reader->ReadLong());
    EXPECT_EQ(false, reader->ReadBool());
    EXPECT_EQ(true, reader->ReadBool());
    EXPECT_EQ("abacaba", reader->ReadString());
    EXPECT_EQ(LongString, reader->ReadString());

    EXPECT_FALSE(reader->IsFinished());
    EXPECT_THROW_WITH_SUBSTRING(reader->ValidateFinished(), "Expected end of stream");

    TString longString;
    longString.resize(1'000'000);
    reader->ReadString(&longString);
    EXPECT_EQ(LongString, LongString);

    EXPECT_TRUE(reader->IsFinished());
    reader->ValidateFinished();
}

TEST(TZookeeperProtocolTest, WriterReallocation)
{
    constexpr int Count = 954023;

    auto writer = CreateZookeeperProtocolWriter();
    for (int i = 0; i < Count; ++i) {
        writer->WriteLong(i);
    }

    auto data = writer->Finish();
    EXPECT_EQ(8u * Count, data.Size());

    auto reader = CreateZookeeperProtocolReader(std::move(data));
    for (int i = 0; i < Count; ++i) {
        EXPECT_EQ(i, reader->ReadLong());
    }
    reader->ValidateFinished();
}

TEST(TZookeeperProtocolTest, CorruptedInput)
{
    {
        auto reader = CreateZookeeperProtocolReader(TSharedRef::MakeEmpty());
        EXPECT_THROW_WITH_SUBSTRING(reader->ReadInt(), "Premature end of stream");
        EXPECT_THROW_WITH_SUBSTRING(reader->ReadString(), "Premature end of stream");
    }
    {
        auto writer = CreateZookeeperProtocolWriter();
        writer->WriteString(TString{100'000, 'a'});
        auto data = writer->Finish();
        data = data.Slice(0, 12345);
        auto reader = CreateZookeeperProtocolReader(std::move(data));
        EXPECT_THROW_WITH_SUBSTRING(reader->ReadString(), "Premature end of stream");
    }
    {
        auto writer = CreateZookeeperProtocolWriter();
        writer->WriteLong(1234);
        writer->WriteLong(std::numeric_limits<i64>::max());
        writer->WriteLong('a');
        writer->WriteLong('b');

        auto data = writer->Finish();
        auto reader = CreateZookeeperProtocolReader(std::move(data));
        EXPECT_EQ(1234, reader->ReadLong());
        // NB: Offset + Size may overflow here if implemented carelessly.
        EXPECT_THROW_WITH_SUBSTRING(reader->ReadString(), "Premature end of stream");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NZookeeper
