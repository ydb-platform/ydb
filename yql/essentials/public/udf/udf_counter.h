#pragma once
#include "udf_types.h"
#include "udf_type_size_check.h"
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NYql {
namespace NUdf {

class TCounter {
public:
    TCounter(i64* ptr = nullptr)
        : Ptr_(ptr)
    {}

    void Inc() {
        if (Ptr_) {
            AtomicIncrement(AsAtomic());
        }
    }

    void Dec() {
        if (Ptr_) {
            AtomicDecrement(AsAtomic());
        }
    }

    void Add(i64 delta) {
        if (Ptr_) {
            AtomicAdd(AsAtomic(), delta);
        }
    }

    void Sub(i64 delta) {
        if (Ptr_) {
            AtomicSub(AsAtomic(), delta);
        }
    }

    void Set(i64 value) {
        if (Ptr_) {
            AtomicSet(AsAtomic(), value);
        }
    }

private:
    TAtomic& AsAtomic() {
        return *reinterpret_cast<TAtomic*>(Ptr_);
    }

private:
    i64* Ptr_;
};

UDF_ASSERT_TYPE_SIZE(TCounter, 8);

class IScopedProbeHost {
public:
    virtual ~IScopedProbeHost() = default;

    virtual void Acquire(void* cookie) = 0;

    virtual void Release(void* cookie) = 0;
};

UDF_ASSERT_TYPE_SIZE(IScopedProbeHost, 8);

class TScopedProbe {
public:
    TScopedProbe(IScopedProbeHost* host = nullptr, void* cookie = nullptr)
        : Host_(host ? host : &NullHost_)
        , Cookie_(cookie)
    {}

    void Acquire() {
        Host_->Acquire(Cookie_);
    }

    void Release() {
        Host_->Release(Cookie_);
    }

private:
    class TNullHost : public IScopedProbeHost {
    public:
        void Acquire(void* cookie) override {
            Y_UNUSED(cookie);
        }

        void Release(void* cookie) override {
            Y_UNUSED(cookie);
        }
    };

    IScopedProbeHost* Host_;
    void* Cookie_;
    static TNullHost NullHost_;
};

UDF_ASSERT_TYPE_SIZE(TScopedProbe, 16);

class ICountersProvider {
public:
    virtual ~ICountersProvider() = default;

    virtual TCounter GetCounter(const TStringRef& module, const TStringRef& name, bool deriv) = 0;

    virtual TScopedProbe GetScopedProbe(const TStringRef& module, const TStringRef& name) = 0;
};

UDF_ASSERT_TYPE_SIZE(ICountersProvider, 8);

} // namspace NUdf
} // namspace NYql
