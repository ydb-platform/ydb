#pragma once

#include <yql/essentials/core/yql_data_provider.h>

namespace NKikimr::NKqp {

// Register both endpoints without constructing their shared state. The factory
// initializes both endpoints before returning them. Provider references,
// cluster registration and provider-specific callables activate the pair;
// enumerating providers, running unrelated phases and rewinding do not.
// List provider helpers outside callablePrefix explicitly in additionalCallables.
NYql::TDataProviderInfo CreateLazyProvider(TStringBuf name, TStringBuf callablePrefix,
    std::function<NYql::TDataProviderInfo()> factory, bool hasLayers = false,
    TVector<TString> additionalCallables = {});

// Select integrations at the first Transform, after metadata has activated the
// required providers. Rewind before the first use must not invoke the factory.
TAutoPtr<NYql::IGraphTransformer> CreateLazyTransformer(
    std::function<TAutoPtr<NYql::IGraphTransformer>()> factory);

} // namespace NKikimr::NKqp
