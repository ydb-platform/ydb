#include "generator.h"

#include "signature.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSignaturePtr ISignatureGenerator::Sign(TYsonString data)
{
    auto signature = New<TSignature>();
    signature->Payload_ = std::move(data);
    Sign(signature);
    return signature;
}

////////////////////////////////////////////////////////////////////////////////

class TDummySignatureGenerator
    : public ISignatureGenerator
{
public:
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

class TAlwaysThrowingSignatureGenerator
    : public ISignatureGenerator
{
public:
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
