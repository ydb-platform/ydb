#include "generator.h"

#include "signature.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString& ISignatureGenerator::GetHeader(const TSignaturePtr& signature)
{
    return signature->Header_;
}

std::vector<std::byte>& ISignatureGenerator::GetSignature(const TSignaturePtr& signature)
{
    return signature->Signature_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
