#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/signature/provided.h>
#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TProvidedSignatureGeneratorTest, DelegateToProvider)
{
    auto dummyGenerator = CreateDummySignatureGenerator();
    auto providedGenerator = New<TProvidedSignatureGenerator>(BIND([dummyGenerator] {
        return dummyGenerator;
    }));

    EXPECT_EQ(providedGenerator->Sign("payload")->Payload(), "payload");
}

TEST(TProvidedSignatureGeneratorTest, ProviderCalledOnEachResign)
{
    int callCount = 0;
    auto dummyGenerator = CreateDummySignatureGenerator();
    auto providedGenerator = New<TProvidedSignatureGenerator>(BIND([&callCount, dummyGenerator] {
        ++callCount;
        return dummyGenerator;
    }));

    Y_UNUSED(providedGenerator->Sign("payload1"));
    Y_UNUSED(providedGenerator->Sign("payload2"));

    EXPECT_EQ(callCount, 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
