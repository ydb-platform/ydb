#include "validator.h"

#include "signature.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TDummySignatureValidator
    : public ISignatureValidator
{
    TFuture<bool> Validate(const TSignaturePtr& /*signature*/) const final
    {
        return TrueFuture;
    }
};

struct TAlwaysThrowingSignatureValidator
    : public ISignatureValidator
{
    TFuture<bool> Validate(const TSignaturePtr& /*signature*/) const final
    {
        THROW_ERROR_EXCEPTION("Signature validation is unsupported");
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISignatureValidatorPtr CreateDummySignatureValidator()
{
    return New<TDummySignatureValidator>();
}

ISignatureValidatorPtr CreateAlwaysThrowingSignatureValidator()
{
    return New<TAlwaysThrowingSignatureValidator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

