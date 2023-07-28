#pragma once

#include <yt/yt/core/http/public.h>

#include <memory>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

/// @brief Wrapper for THeaderPtr which allows to hide NYT::IntrusivePtr from interfaces.
struct THeadersPtrWrapper
{
    THeadersPtrWrapper(THeadersPtr ptr)
        : Ptr(std::make_shared<THeadersPtr>(std::move(ptr)))
    { }

    THeadersPtr Get() {
        return *Ptr;
    }

    std::shared_ptr<THeadersPtr> Ptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
