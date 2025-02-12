#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureGeneratorBase
    : public TRefCounted
{
public:
    //! Fills out the Signature_ and Header_ fields in a given TSignature
    //! based on its payload.
    virtual void Sign(const TSignaturePtr& signature) = 0;

    [[nodiscard]] TSignaturePtr Sign(NYson::TYsonString data);

private:
    friend class TSignatureGenerator;
    friend class TDummySignatureGenerator;
    friend class TAlwaysThrowingSignatureGenerator;
};

DEFINE_REFCOUNTED_TYPE(TSignatureGeneratorBase)

////////////////////////////////////////////////////////////////////////////////

TSignatureGeneratorBasePtr CreateDummySignatureGenerator();

TSignatureGeneratorBasePtr CreateAlwaysThrowingSignatureGenerator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
