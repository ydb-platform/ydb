#pragma once

#include <functional>
#include <ydb/library/actors/core/events.h>

namespace NActors {
    class TSharedDescriptor: public TThrRefBase {
    public:
        virtual int GetDescriptor() = 0;
    };

    using TDelegate = std::function<void()>;
    using TFDDelegate = std::function<TDelegate(const TIntrusivePtr<TSharedDescriptor>&)>;

    class IPoller: public TThrRefBase {
    public:
        virtual ~IPoller() = default;

        virtual void StartRead(const TIntrusivePtr<TSharedDescriptor>& s, TFDDelegate&& operation) = 0;
        virtual void StartWrite(const TIntrusivePtr<TSharedDescriptor>& s, TFDDelegate&& operation) = 0;
    };

}
