#include "generator.h"

#include "signature.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSignaturePtr ISignatureGenerator::Sign(std::string payload) const
{
    auto signature = New<TSignature>();
    signature->Payload_ = std::move(payload);
    Resign(signature);
    return signature;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TDummySignatureGenerator
    : public ISignatureGenerator
{
    void Resign(const TSignaturePtr& /*signature*/) const final
    { }
};

struct TAlwaysThrowingSignatureGenerator
    : public ISignatureGenerator
{
    void Resign(const TSignaturePtr& /*signature*/) const final
    {
        THROW_ERROR_EXCEPTION("Signature generation is unsupported");
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISignatureGeneratorPtr CreateDummySignatureGenerator()
{
    return New<TDummySignatureGenerator>();
}

const ISignatureGeneratorPtr& GetDummySignatureGenerator()
{
    static ISignatureGeneratorPtr signatureGenerator = CreateDummySignatureGenerator();
    return signatureGenerator;
}

ISignatureGeneratorPtr CreateAlwaysThrowingSignatureGenerator()
{
    return New<TAlwaysThrowingSignatureGenerator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
