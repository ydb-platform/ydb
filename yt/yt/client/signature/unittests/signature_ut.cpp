#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureTest, PayloadConstruct)
{
    TSignature signature(TYsonString("payload"_sb));
    EXPECT_EQ(signature.Payload().ToString(), "payload");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureTest, DeserializeSerialize)
{
    // SignatureSize bytes.
    TYsonString ysonOK(R"({"header"="header";"payload"="payload";"signature"="signature";})"_sb);

    TSignaturePtr signature;
    EXPECT_NO_THROW(signature = ConvertTo<TSignaturePtr>(ysonOK));
    EXPECT_EQ(signature->Payload().ToString(), "payload");

    EXPECT_EQ(ConvertToYsonString(signature, EYsonFormat::Text).ToString(), ysonOK.ToString());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureTest, DeserializeFail)
{
    {
        TYsonString ysonFail(
            R"({"header"="header";"buddy"="payload";"signature"="abacaba";})"_sb
        );
        EXPECT_THROW_WITH_SUBSTRING(ConvertTo<TSignaturePtr>(ysonFail), "no child with key \"payload\"");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
