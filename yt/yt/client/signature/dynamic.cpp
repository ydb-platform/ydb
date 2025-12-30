#include "dynamic.h"

#include "private.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TDynamicSignatureGenerator::TDynamicSignatureGenerator(ISignatureGeneratorPtr underlying)
    : Underlying_(std::move(underlying))
{
    YT_LOG_INFO("Dynamic signature generator initialized");
}

void TDynamicSignatureGenerator::SetUnderlying(ISignatureGeneratorPtr underlying)
{
    YT_LOG_INFO("Updating dynamic signature generator");
    Underlying_.Store(std::move(underlying));
}

void TDynamicSignatureGenerator::Resign(const TSignaturePtr& signature) const
{
    Underlying_.Acquire()->Resign(signature);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicSignatureValidator::TDynamicSignatureValidator(ISignatureValidatorPtr underlying)
    : Underlying_(std::move(underlying))
{
    YT_LOG_INFO("Dynamic signature validator initialized");
}

void TDynamicSignatureValidator::SetUnderlying(ISignatureValidatorPtr underlying)
{
    YT_LOG_INFO("Updating dynamic signature validator");
    Underlying_.Store(std::move(underlying));
}

TFuture<bool> TDynamicSignatureValidator::Validate(const TSignaturePtr& signature) const
{
    return Underlying_.Acquire()->Validate(signature);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
