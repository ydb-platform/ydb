#include "validator.h"

#include "signature.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TDummySignatureValidator
    : public ISignatureValidator
{
public:
    TFuture<bool> Validate(const TSignaturePtr& signature) override
    {
        YT_VERIFY(signature->Header_.ToString() == "DummySignature");
        return TrueFuture;
    }
};

ISignatureValidatorPtr CreateDummySignatureValidator()
{
    return New<TDummySignatureValidator>();
}

////////////////////////////////////////////////////////////////////////////////

class TAlwaysThrowingSignatureValidator
    : public ISignatureValidator
{
public:
    TFuture<bool> Validate(const TSignaturePtr& /*signature*/) override
    {
        THROW_ERROR_EXCEPTION("Signature validation is unsupported");
    }
};

ISignatureValidatorPtr CreateAlwaysThrowingSignatureValidator()
{
    return New<TAlwaysThrowingSignatureValidator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

