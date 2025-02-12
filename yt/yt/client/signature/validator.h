#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidatorBase
    : public TRefCounted
{
public:
    virtual TFuture<bool> Validate(const TSignaturePtr& signature) = 0;
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidatorBase)

////////////////////////////////////////////////////////////////////////////////

TSignatureValidatorBasePtr CreateDummySignatureValidator();

TSignatureValidatorBasePtr CreateAlwaysThrowingSignatureValidator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
