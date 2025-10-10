#pragma once

#include "layers_fwd.h"
#include <library/cpp/threading/future/core/future.h>

namespace NYql::NLayers {
class IRemoteLayerProvider: public TThrRefBase {
public:
    virtual NThreading::TFuture<TLayerInfo> GetLayerInfo(const TMaybe<TString>& parent, const TString& url) const = 0;
    virtual ~IRemoteLayerProvider() = default;
};

using IRemoteLayerProviderPtr = TIntrusivePtr<IRemoteLayerProvider>;
} // namespace NYql::NLayers
