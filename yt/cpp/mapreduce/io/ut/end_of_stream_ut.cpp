#include <yt/cpp/mapreduce/io/node_table_reader.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

class TStringRawTableReader
    : public TRawTableReader
{
public:
    TStringRawTableReader(const TString& string)
        : String_(string)
        , Stream_(String_)
    { }

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override
    {
        return false;
    }

    void ResetRetries() override
    { }

    bool HasRangeIndices() const override
    {
        return false;
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        return Stream_.Read(buf, len);
    }

private:
    const TString String_;
    TStringStream Stream_;
};

////////////////////////////////////////////////////////////////////////////////

class TEndOfStreamTest
    : public ::testing::TestWithParam<bool>
{ };

TEST_P(TEndOfStreamTest, Eos)
{
    bool addEos = GetParam();
    auto proxy = ::MakeIntrusive<TStringRawTableReader>(TString::Join(
        "{a=13;b = \"string\"}; {c = {d=12}};",
        "<key_switch=%true>#; {e = 42};",
        addEos ? "<end_of_stream=%true>#" : ""
    ));

    TNodeTableReader reader(proxy);
    TVector<TNode> expectedRows = {TNode()("a", 13)("b", "string"), TNode()("c", TNode()("d", 12))};
    for (const auto& expectedRow : expectedRows) {
        EXPECT_TRUE(reader.IsValid());
        EXPECT_TRUE(!reader.IsEndOfStream());
        EXPECT_TRUE(!reader.IsRawReaderExhausted());
        EXPECT_EQ(reader.GetRow(), expectedRow);
        reader.Next();
    }

    EXPECT_TRUE(!reader.IsValid());
    EXPECT_TRUE(!reader.IsEndOfStream());
    EXPECT_TRUE(!reader.IsRawReaderExhausted());

    reader.NextKey();
    reader.Next();
    expectedRows = {TNode()("e", 42)};
    for (const auto& expectedRow : expectedRows) {
        EXPECT_TRUE(reader.IsValid());
        EXPECT_TRUE(!reader.IsEndOfStream());
        EXPECT_TRUE(!reader.IsRawReaderExhausted());
        EXPECT_EQ(reader.GetRow(), expectedRow);
        reader.Next();
    }

    EXPECT_TRUE(!reader.IsValid());
    if (addEos) {
        EXPECT_TRUE(reader.IsEndOfStream());
    } else {
        EXPECT_TRUE(!reader.IsEndOfStream());
    }
    EXPECT_TRUE(reader.IsRawReaderExhausted());
}

INSTANTIATE_TEST_SUITE_P(WithEos, TEndOfStreamTest, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(WithoutEos, TEndOfStreamTest, ::testing::Values(false));

////////////////////////////////////////////////////////////////////////////////
