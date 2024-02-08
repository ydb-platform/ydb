#pragma once

#include "grpc_server.h"

namespace NYdbGrpc {

enum class EQueueEventStatus {
    OK,
    ERROR
};

template<class TCallback>
class TQueueEventCallback: public IQueueEvent {
public:
    TQueueEventCallback(const TCallback& callback)
        : Callback(callback)
    {}

    TQueueEventCallback(TCallback&& callback)
        : Callback(std::move(callback))
    {}

    bool Execute(bool ok) override {
        Callback(ok ? EQueueEventStatus::OK : EQueueEventStatus::ERROR);
        return false;
    }

    void DestroyRequest() override {
        delete this;
    }

private:
    TCallback Callback;
};

// Implementation of IQueueEvent that reduces allocations
template<class TSelf>
class TQueueFixedEvent: private IQueueEvent {
    using TCallback = void (TSelf::*)(EQueueEventStatus);

public:
    TQueueFixedEvent(TSelf* self, TCallback callback)
        : Self(self)
        , Callback(callback)
    { }

    IQueueEvent* Prepare() {
        Self->Ref();
        return this;
    }

private:
    bool Execute(bool ok) override {
        ((*Self).*Callback)(ok ? EQueueEventStatus::OK : EQueueEventStatus::ERROR);
        return false;
    }

    void DestroyRequest() override {
        Self->UnRef();
    }

private:
    TSelf* const Self;
    TCallback const Callback;
};

template<class TCallback>
inline IQueueEvent* MakeQueueEventCallback(TCallback&& callback) {
    return new TQueueEventCallback<TCallback>(std::forward<TCallback>(callback));
}

template<class T>
inline IQueueEvent* MakeQueueEventCallback(T* self, void (T::*method)(EQueueEventStatus)) {
    using TPtr = TIntrusivePtr<T>;
    return MakeQueueEventCallback([self = TPtr(self), method] (EQueueEventStatus status) {
        ((*self).*method)(status);
    });
}

} // namespace NYdbGrpc
