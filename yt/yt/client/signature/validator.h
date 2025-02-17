#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class ISignatureValidator
    : public TRefCounted
{
public:
    virtual TFuture<bool> Validate(const TSignaturePtr& signature) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISignatureValidator)

////////////////////////////////////////////////////////////////////////////////

ISignatureValidatorPtr CreateDummySignatureValidator();

ISignatureValidatorPtr CreateAlwaysThrowingSignatureValidator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
