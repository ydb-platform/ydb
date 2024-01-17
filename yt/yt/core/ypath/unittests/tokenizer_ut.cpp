#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/ypath/helpers.h>

#include <util/string/vector.h>

namespace NYT::NYPath {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TYPathTokenizerTest
    : public ::testing::Test
{
private:
    std::unique_ptr<TTokenizer> Tokenizer;
    std::vector<ETokenType> TokenTypes;
    std::vector<TString> Literals;

public:
    void Prepare(const char* input)
    {
        Tokenizer.reset(new TTokenizer(input));
        TokenTypes.clear();
        Literals.clear();
    }

    bool Tokenize()
    {
        if (Tokenizer->Advance() == ETokenType::EndOfStream) {
            return false;
        }

        TokenTypes.push_back(Tokenizer->GetType());
        if (Tokenizer->GetType() == ETokenType::Literal) {
            Literals.push_back(Tokenizer->GetLiteralValue());
        }
        return true;
    }

    void PrepareAndTokenize(const char* input)
    {
        Prepare(input);
        while (Tokenize());
    }

    TString GetFlattenedTokens()
    {
        TString result;
        result.reserve(TokenTypes.size());
        for (auto type : TokenTypes) {
            switch (type) {
                case ETokenType::Literal:   result.append('L'); break;
                case ETokenType::Slash:     result.append('/'); break;
                case ETokenType::Ampersand: result.append('&'); break;
                case ETokenType::At:        result.append('@'); break;
                default:                                        break;
            }
        }
        return result;
    }

    const std::vector<TString>& GetLiterals()
    {
        return Literals;
    }
};

TEST_F(TYPathTokenizerTest, SimpleCase1)
{
    PrepareAndTokenize("hello");
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, "hello"), GetLiterals());
}

TEST_F(TYPathTokenizerTest, SimpleCase2)
{
    PrepareAndTokenize("/");
    EXPECT_EQ("/", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(), GetLiterals());
}

TEST_F(TYPathTokenizerTest, SimpleCase3)
{
    PrepareAndTokenize("&");
    EXPECT_EQ("&", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(), GetLiterals());
}

TEST_F(TYPathTokenizerTest, SimpleCase4)
{
    PrepareAndTokenize("@");
    EXPECT_EQ("@", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(), GetLiterals());
}

TEST_F(TYPathTokenizerTest, SimpleCase5)
{
    PrepareAndTokenize("&//@@&&@/&"); // There are all pairs within this string.
    EXPECT_EQ("&//@@&&@/&", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(), GetLiterals());
}

TEST_F(TYPathTokenizerTest, SimpleCase6)
{
    PrepareAndTokenize("hello/cruel@world&");
    EXPECT_EQ("L/L@L&", GetFlattenedTokens());

    std::vector<TString> expectedLiterals;
    expectedLiterals.push_back("hello");
    expectedLiterals.push_back("cruel");
    expectedLiterals.push_back("world");
    EXPECT_EQ(expectedLiterals, GetLiterals());
}

TEST_F(TYPathTokenizerTest, SeeminglyImpossibleLiteral)
{
    const char* string = "Hello, cruel world; I am here to destroy you.";
    PrepareAndTokenize(string);
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, string), GetLiterals());
}

TEST_F(TYPathTokenizerTest, IntegersAndDoubles)
{
    PrepareAndTokenize("0123456789.01234567890");
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, "0123456789.01234567890"), GetLiterals());
}

TEST_F(TYPathTokenizerTest, GUID)
{
    PrepareAndTokenize("c61834-c8f650dc-90b0dfdc-21c02eed");
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, "c61834-c8f650dc-90b0dfdc-21c02eed"), GetLiterals());

    PrepareAndTokenize("000000-11111111-22222222-33333333");
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, "000000-11111111-22222222-33333333"), GetLiterals());
}

TEST_F(TYPathTokenizerTest, EscapedSpecial)
{
    PrepareAndTokenize("\\@\\&\\/\\\\\\[\\{");
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, "@&/\\[{"), GetLiterals());
}

TEST_F(TYPathTokenizerTest, EscapedHex)
{
    PrepareAndTokenize(
"\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09\\x0a\\x0b\\x0c\\x0d\\x0e\\x0f"
"\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1a\\x1b\\x1c\\x1d\\x1e\\x1f"
"\\x20\\x21\\x22\\x23\\x24\\x25\\x26\\x27\\x28\\x29\\x2a\\x2b\\x2c\\x2d\\x2e\\x2f"
"\\x30\\x31\\x32\\x33\\x34\\x35\\x36\\x37\\x38\\x39\\x3a\\x3b\\x3c\\x3d\\x3e\\x3f"
"\\x40\\x41\\x42\\x43\\x44\\x45\\x46\\x47\\x48\\x49\\x4a\\x4b\\x4c\\x4d\\x4e\\x4f"
"\\x50\\x51\\x52\\x53\\x54\\x55\\x56\\x57\\x58\\x59\\x5a\\x5b\\x5c\\x5d\\x5e\\x5f"
"\\x60\\x61\\x62\\x63\\x64\\x65\\x66\\x67\\x68\\x69\\x6a\\x6b\\x6c\\x6d\\x6e\\x6f"
"\\x70\\x71\\x72\\x73\\x74\\x75\\x76\\x77\\x78\\x79\\x7a\\x7b\\x7c\\x7d\\x7e\\x7f"
"\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8a\\x8b\\x8c\\x8d\\x8e\\x8f"
"\\x90\\x91\\x92\\x93\\x94\\x95\\x96\\x97\\x98\\x99\\x9a\\x9b\\x9c\\x9d\\x9e\\x9f"
"\\xa0\\xa1\\xa2\\xa3\\xa4\\xa5\\xa6\\xa7\\xa8\\xa9\\xaa\\xab\\xac\\xad\\xae\\xaf"
"\\xb0\\xb1\\xb2\\xb3\\xb4\\xb5\\xb6\\xb7\\xb8\\xb9\\xba\\xbb\\xbc\\xbd\\xbe\\xbf"
"\\xc0\\xc1\\xc2\\xc3\\xc4\\xc5\\xc6\\xc7\\xc8\\xc9\\xca\\xcb\\xcc\\xcd\\xce\\xcf"
"\\xd0\\xd1\\xd2\\xd3\\xd4\\xd5\\xd6\\xd7\\xd8\\xd9\\xda\\xdb\\xdc\\xdd\\xde\\xdf"
"\\xe0\\xe1\\xe2\\xe3\\xe4\\xe5\\xe6\\xe7\\xe8\\xe9\\xea\\xeb\\xec\\xed\\xee\\xef"
"\\xf0\\xf1\\xf2\\xf3\\xf4\\xf5\\xf6\\xf7\\xf8\\xf9\\xfa\\xfb\\xfc\\xfd\\xfe\\xff"
);
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(static_cast<size_t>(1), GetLiterals().size());

    const auto& string = GetLiterals()[0];
    for (int i = 0; i < 256; ++i) {
        // NB: Google Test is quite strict when checking integral types.
        // Hence the static casts.
        EXPECT_EQ(
            static_cast<unsigned char>(i),
            static_cast<unsigned char>(string[i]));
    }
}

TEST_F(TYPathTokenizerTest, EscapedAsInRealWorld)
{
    PrepareAndTokenize("Madness\\x3f This is Sparta\\x21");
    EXPECT_EQ("L", GetFlattenedTokens());
    EXPECT_EQ(std::vector<TString>(1, "Madness? This is Sparta!"), GetLiterals());
}

TEST_F(TYPathTokenizerTest, InvalidEscapeSequences)
{
    EXPECT_THROW({ PrepareAndTokenize("This is \\"); }, std::exception);
    EXPECT_THROW({ PrepareAndTokenize("This is \\w"); }, std::exception);
    EXPECT_THROW({ PrepareAndTokenize("This is \\x"); }, std::exception);
    EXPECT_THROW({ PrepareAndTokenize("This is \\x0"); }, std::exception);
    EXPECT_THROW({ PrepareAndTokenize("This is \\xx0"); }, std::exception);
    EXPECT_THROW({ PrepareAndTokenize("This is \\x0x"); }, std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathHelpersTest, DirNameAndBaseName)
{
    auto toPair = [] (auto lhs, auto rhs) {
        return std::pair(TString(lhs), TString(rhs));
    };

    EXPECT_EQ(DirNameAndBaseName("//path/to/smth"), toPair("//path/to", "smth"));
    EXPECT_EQ(DirNameAndBaseName("//path/to/smth/@"), toPair("//path/to/smth", "@"));
    EXPECT_EQ(DirNameAndBaseName("//path/to/smth/@foo"), toPair("//path/to/smth/@", "foo"));
    EXPECT_EQ(DirNameAndBaseName("//path"), toPair("/", "path"));
    EXPECT_EQ(DirNameAndBaseName("#123-456-789-abc"), toPair("", "#123-456-789-abc"));
    EXPECT_EQ(DirNameAndBaseName("/path"), toPair("", "path"));
}

TEST(TYPathHelpersTest, YPathJoin)
{
    EXPECT_EQ(
        YPathJoin("//path/prefix/to/some_table"),
        "//path/prefix/to/some_table");
    EXPECT_EQ(
        YPathJoin("//path/prefix/to", "some_table"),
        "//path/prefix/to/some_table");

    EXPECT_EQ(
        YPathJoin("//path", "prefix", "to", "some_table"),
        "//path/prefix/to/some_table");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYPath
