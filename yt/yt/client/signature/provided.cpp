#include "provided.h"

#include "private.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TProvidedSignatureGenerator::TProvidedSignatureGenerator(TSignatureGeneratorProvider provider)
    : Provider_(std::move(provider))
{
    YT_VERIFY(Provider_);
    YT_LOG_DEBUG("Provided signature generator initialized");
}

void TProvidedSignatureGenerator::Resign(const TSignaturePtr& signature) const
{
    Provider_()->Resign(signature);
}

////////////////////////////////////////////////////////////////////////////////

TProvidedSignatureValidator::TProvidedSignatureValidator(TSignatureValidatorProvider provider)
    : Provider_(std::move(provider))
{
    YT_VERIFY(Provider_);
    YT_LOG_DEBUG("Provided signature validator initialized");
}

TFuture<bool> TProvidedSignatureValidator::Validate(const TSignaturePtr& signature) const
{
    return Provider_()->Validate(signature);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
