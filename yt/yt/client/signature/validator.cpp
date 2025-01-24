#include "validator.h"

#include "signature.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

const NYson::TYsonString& TSignatureValidatorBase::GetHeader(const TSignaturePtr& signature)
{
    return signature->Header_;
}

const std::vector<std::byte>& TSignatureValidatorBase::GetSignature(const TSignaturePtr& signature)
{
    return signature->Signature_;
}

////////////////////////////////////////////////////////////////////////////////

class TDummySignatureValidator
    : public TSignatureValidatorBase
{
public:
    TFuture<bool> Validate(const TSignaturePtr& signature) override
    {
        YT_VERIFY(GetHeader(signature).ToString() == "DummySignature");
        return TrueFuture;
    }
};

TSignatureValidatorBasePtr CreateDummySignatureValidator()
{
    return New<TDummySignatureValidator>();
}

////////////////////////////////////////////////////////////////////////////////

class TAlwaysThrowingSignatureValidator
    : public TSignatureValidatorBase
{
public:
    TFuture<bool> Validate(const TSignaturePtr& /*signature*/) override
    {
        THROW_ERROR_EXCEPTION("Signature validation is unsupported");
    }
};

TSignatureValidatorBasePtr CreateAlwaysThrowingSignatureValidator()
{
    return New<TAlwaysThrowingSignatureValidator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

