#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct ISignatureGenerator
    : public TRefCounted
{
    [[nodiscard]] TSignaturePtr Sign(std::string payload) const;

private:
    //! Fills out the Signature_ and Header_ fields in a given TSignature
    //! based on its payload.
    virtual void DoSign(const TSignaturePtr& signature) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

ISignatureGeneratorPtr CreateDummySignatureGenerator();
const ISignatureGeneratorPtr& GetDummySignatureGenerator();

ISignatureGeneratorPtr CreateAlwaysThrowingSignatureGenerator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
