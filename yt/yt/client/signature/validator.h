#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/actions/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidatorBase
    : public TRefCounted
{
public:
    virtual TFuture<bool> Validate(const TSignaturePtr& signature) = 0;

    virtual ~TSignatureValidatorBase() = default;

protected:
    const NYson::TYsonString& GetHeader(const TSignaturePtr& signature);

    const std::vector<std::byte>& GetSignature(const TSignaturePtr& signature);
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidatorBase)

////////////////////////////////////////////////////////////////////////////////

TSignatureValidatorBasePtr CreateDummySignatureValidator();

TSignatureValidatorBasePtr CreateAlwaysThrowingSignatureValidator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
