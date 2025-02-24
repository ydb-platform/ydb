#pragma once

#include <util/generic/ptr.h>

namespace NYql {

class IRunnable: public TThrRefBase {
public:
    using TPtr = THolder<IRunnable>;

    virtual ~IRunnable() = default;

    virtual void Start() = 0;

    virtual void Stop() = 0;
};

} // namespace NYql
