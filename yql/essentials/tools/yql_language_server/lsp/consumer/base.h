#pragma once

#include <util/generic/maybe.h>

namespace NLsp {

template <typename T>
class IConsumer: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IConsumer<T>>;

    virtual void Receive(T value) = 0;
    virtual void Stop() = 0;
};

} // namespace NLsp
