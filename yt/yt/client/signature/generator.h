#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class ISignatureGenerator
    : public TRefCounted
{
public:
    //! Fills out the Signature_ and Header_ fields in a given TSignature
    //! based on its payload.
    virtual void Sign(const TSignaturePtr& signature) = 0;

    virtual ~ISignatureGenerator() = default;

protected:
    NYson::TYsonString& GetHeader(const TSignaturePtr& signature);

    std::vector<std::byte>& GetSignature(const TSignaturePtr& signature);
};

DEFINE_REFCOUNTED_TYPE(ISignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
