#pragma once

#include "iface.h"

#include <util/generic/ptr.h>
#include <util/generic/ylimits.h>

class TContExecutor;

namespace NAsyncDns {
    class TContResolver {
    public:
        TContResolver(TContExecutor* e, const TOptions& opts = TOptions());
        ~TContResolver();

        void Resolve(const TNameRequest& hreq);

    private:
        class TImpl;
        THolder<TImpl> I_;
    };
}
