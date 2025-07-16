#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>

namespace NSQLComplete {

    using TClusterList = TVector<TString>;

    class IClusterDiscovery: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IClusterDiscovery>;

        ~IClusterDiscovery() override = default;
        virtual NThreading::TFuture<TClusterList> Query() const = 0;
    };

} // namespace NSQLComplete
