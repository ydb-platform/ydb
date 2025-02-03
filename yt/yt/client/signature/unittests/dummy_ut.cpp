#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

const auto YsonSignature = TYsonString(
    R"({"header"="DummySignature";"payload"="payload";"signature"="";})"_sb);

////////////////////////////////////////////////////////////////////////////////

TEST(TDummySignatureGeneratorTest, Generate)
{
    auto generator = CreateDummySignatureGenerator();
    auto signature = generator->Sign(TYsonString("payload"_sb));
    EXPECT_EQ(ConvertToYsonString(signature, EYsonFormat::Text), YsonSignature);
    generator->Sign(signature);
    EXPECT_EQ(ConvertToYsonString(signature, EYsonFormat::Text), YsonSignature);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDummySignatureValidatorTest, ValidateGood)
{
    auto signature = ConvertTo<TSignaturePtr>(YsonSignature);
    auto validator = CreateDummySignatureValidator();
    EXPECT_TRUE(validator->Validate(signature).Get().Value());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDummySignatureValidatorTest, GenerateValidate)
{
    auto generator = CreateDummySignatureGenerator();
    auto validator = CreateDummySignatureValidator();
    auto signature = generator->Sign(TYsonString("payload"_sb));
    EXPECT_TRUE(validator->Validate(signature).Get().Value());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAlwaysThrowingSignatureGeneratorTest, Generate)
{
    auto generator = CreateAlwaysThrowingSignatureGenerator();
    EXPECT_THROW_WITH_SUBSTRING(generator->Sign(New<TSignature>()), "unsupported");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAlwaysThrowingSignatureValidatorTest, Validate)
{
    auto validator = CreateAlwaysThrowingSignatureValidator();
    EXPECT_THROW_WITH_SUBSTRING(
        YT_UNUSED_FUTURE(validator->Validate(New<TSignature>())),
        "unsupported");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
