#include "validator.h"

#include "signature.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

const NYson::TYsonString& ISignatureValidator::GetHeader(const TSignaturePtr& signature)
{
    return signature->Header_;
}

const std::vector<std::byte>& ISignatureValidator::GetSignature(const TSignaturePtr& signature)
{
    return signature->Signature_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

