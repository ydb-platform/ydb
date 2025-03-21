#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct ISignatureGenerator
    : public TRefCounted
{
    //! Fills out the Signature_ and Header_ fields in a given TSignature
    //! based on its payload.
    virtual void Sign(const TSignaturePtr& signature) = 0;

    [[nodiscard]] TSignaturePtr Sign(std::string payload);
};

DEFINE_REFCOUNTED_TYPE(ISignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

ISignatureGeneratorPtr CreateDummySignatureGenerator();

ISignatureGeneratorPtr CreateAlwaysThrowingSignatureGenerator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
