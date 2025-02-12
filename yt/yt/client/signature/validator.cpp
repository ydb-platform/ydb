#include "validator.h"

#include "signature.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TDummySignatureValidator
    : public TSignatureValidatorBase
{
public:
    TFuture<bool> Validate(const TSignaturePtr& signature) override
    {
        YT_VERIFY(signature->Header_.ToString() == "DummySignature");
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

