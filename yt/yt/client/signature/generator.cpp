#include "generator.h"

#include "signature.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSignaturePtr ISignatureGenerator::Sign(std::string payload)
{
    auto signature = New<TSignature>();
    signature->Payload_ = std::move(payload);
    Sign(signature);
    return signature;
}

////////////////////////////////////////////////////////////////////////////////

struct TDummySignatureGenerator
    : public ISignatureGenerator
{
    void Sign(const TSignaturePtr& signature) override
    {
        signature->Header_ = NYson::TYsonString("DummySignature"_sb);
    }
};

ISignatureGeneratorPtr CreateDummySignatureGenerator()
{
    return New<TDummySignatureGenerator>();
}

////////////////////////////////////////////////////////////////////////////////

struct TAlwaysThrowingSignatureGenerator
    : public ISignatureGenerator
{
    void Sign(const TSignaturePtr& /*signature*/) override
    {
        THROW_ERROR_EXCEPTION("Signature generation is unsupported");
    }
};

ISignatureGeneratorPtr CreateAlwaysThrowingSignatureGenerator()
{
    return New<TAlwaysThrowingSignatureGenerator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
