#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/blob.h>

#include <yt/yt/core/misc/checksum.h>

#include <util/random/random.h>
#include <util/stream/mem.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCrcTestCase
{
    ui64 Iso;
    ui64 Ecma;
    ui64 Ours;
    TString Data;
};

static std::vector<TCrcTestCase> Cases = {
    {
        0x0000000000000000, 0x0000000000000000, 0x0000000000000000,
        ""
    }, {
        0x3420000000000000, 0x330284772e652b05, 0x74b42565ce6232d5,
        "a"
    }, {
        0x36c4200000000000, 0xbc6573200e84b046, 0x5f02be5e81cf7b1c,
        "ab"
    }, {
        0x3776c42000000000, 0x2cd8094a1a277627, 0xaadaac6d7d340c20,
        "abc"
    }, {
        0x336776c420000000, 0x3c9d28596e5960ba, 0xd35b54234f7f70a0,
        "abcd"
    }, {
        0x32d36776c4200000, 0x040bdf58fb0895f2, 0xe729d85f050fa861,
        "abcde"
    }, {
        0x3002d36776c42000, 0xd08e9f8545a700f4, 0x4852bb31b666ae4f,
        "abcdef"
    }, {
        0x31b002d36776c420, 0xec20a3a8cc710e66, 0xab31ee2e0fe39abb,
        "abcdefg"
    }, {
        0xe21b002d36776c40, 0x67b4f30a647a0c59, 0x3dc543531acca62b,
        "abcdefgh"
    }, {
        0x8b6e21b002d36776, 0x9966f6c89d56ef8e, 0x43c501e26fc35778,
        "abcdefghi"
    }, {
        0x7f5b6e21b002d367, 0x32093a2ecd5773f4, 0x4cc4843d59c1373e,
        "abcdefghij"
    }, {
        0xa7b9d53ea87eb82f, 0x561cc0cfa235ac68, 0x481ac76eee0d3ebd,
        "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"
    },
};

////////////////////////////////////////////////////////////////////////////////

TEST(TChecksumTest, TestStreams)
{
    auto size = 0;
    for (const auto& test : Cases) {
        size += test.Data.Size();
    }
    TBlob blob(GetRefCountedTypeCookie<TDefaultBlobTag>(), size);

    auto memoryOutputStream = TMemoryOutput(blob.Begin(), blob.Size());
    auto outputStream = TChecksumOutput(&memoryOutputStream);
    for (const auto& test : Cases) {
        outputStream.Write(TStringBuf(test.Data));
    }
    auto outputChecksum = outputStream.GetChecksum();

    auto memoryInputStream = TMemoryInput(blob.Begin(), blob.Size());
    auto inputStream = TChecksumInput(&memoryInputStream);
    char v;
    while (inputStream.Read(&v, 1)) { }
    auto inputChecksum = inputStream.GetChecksum();

    EXPECT_EQ(outputChecksum, inputChecksum);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
