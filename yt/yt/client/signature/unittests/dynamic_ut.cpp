#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/signature/dynamic.h>
#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDynamicSignatureGeneratorTest, SetUnderlying)
{
    auto dynamicGenerator = New<TDynamicSignatureGenerator>(CreateDummySignatureGenerator());
    EXPECT_EQ(dynamicGenerator->Sign("payload")->Payload(), "payload");

    dynamicGenerator->SetUnderlying(CreateAlwaysThrowingSignatureGenerator());
    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(dynamicGenerator->Sign("payload")), "unsupported");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDynamicSignatureValidatorTest, SetUnderlying)
{
    auto dynamicValidator = New<TDynamicSignatureValidator>(CreateDummySignatureValidator());
    auto signature = New<TSignature>();

    EXPECT_TRUE(dynamicValidator->Validate(signature).Get().Value());

    dynamicValidator->SetUnderlying(CreateAlwaysThrowingSignatureValidator());

    EXPECT_THROW_WITH_SUBSTRING(
        dynamicValidator->Validate(signature).Get(),
        "unsupported");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
