#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/actions/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class ISignatureValidator
    : public TRefCounted
{
public:
    virtual TFuture<bool> Validate(const TSignaturePtr& signature) = 0;

    virtual ~ISignatureValidator() = default;

protected:
    const NYson::TYsonString& GetHeader(const TSignaturePtr& signature);

    const std::vector<std::byte>& GetSignature(const TSignaturePtr& signature);
};

DEFINE_REFCOUNTED_TYPE(ISignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
