#pragma once

#include <util/generic/fwd.h>

namespace NYT {
    class IRequestRetryPolicy;
    using IRequestRetryPolicyPtr = ::TIntrusivePtr<IRequestRetryPolicy>;

    class IClientRetryPolicy;
    using IClientRetryPolicyPtr = ::TIntrusivePtr<IClientRetryPolicy>;
}
