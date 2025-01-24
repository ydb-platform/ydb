#include "generator.h"

#include "signature.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSignaturePtr TSignatureGeneratorBase::Sign(TYsonString data)
{
    auto signature = New<TSignature>();
    signature->Payload_ = std::move(data);
    Sign(signature);
    return signature;
}

////////////////////////////////////////////////////////////////////////////////

TYsonString& TSignatureGeneratorBase::GetHeader(const TSignaturePtr& signature)
{
    return signature->Header_;
}

std::vector<std::byte>& TSignatureGeneratorBase::GetSignature(const TSignaturePtr& signature)
{
    return signature->Signature_;
}

////////////////////////////////////////////////////////////////////////////////

class TDummySignatureGenerator
    : public TSignatureGeneratorBase
{
public:
    void Sign(const TSignaturePtr& signature) override
    {
        GetHeader(signature) = NYson::TYsonString("DummySignature"_sb);
    }
};

TSignatureGeneratorBasePtr CreateDummySignatureGenerator()
{
    return New<TDummySignatureGenerator>();
}

////////////////////////////////////////////////////////////////////////////////

class TAlwaysThrowingSignatureGenerator
    : public TSignatureGeneratorBase
{
public:
    void Sign(const TSignaturePtr& /*signature*/) override
    {
        THROW_ERROR_EXCEPTION("Signature generation is unsupported");
    }
};

TSignatureGeneratorBasePtr CreateAlwaysThrowingSignatureGenerator()
{
    return New<TAlwaysThrowingSignatureGenerator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
