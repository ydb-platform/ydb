#pragma once

#include "public.h"

#include "generator.h"
#include "validator.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using TSignatureGeneratorProvider = TCallback<ISignatureGeneratorPtr()>;
using TSignatureValidatorProvider = TCallback<ISignatureValidatorPtr()>;

////////////////////////////////////////////////////////////////////////////////

class TProvidedSignatureGenerator
    : public ISignatureGenerator
{
public:
    explicit TProvidedSignatureGenerator(TSignatureGeneratorProvider provider);

    void Resign(const TSignaturePtr& signature) const final;

private:
    const TSignatureGeneratorProvider Provider_;
};

DEFINE_REFCOUNTED_TYPE(TProvidedSignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

class TProvidedSignatureValidator
    : public ISignatureValidator
{
public:
    explicit TProvidedSignatureValidator(TSignatureValidatorProvider provider);

    TFuture<bool> Validate(const TSignaturePtr& signature) const final;

private:
    const TSignatureValidatorProvider Provider_;
};

DEFINE_REFCOUNTED_TYPE(TProvidedSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
