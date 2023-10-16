#pragma once

#include "defs.h"

template<typename TValue>
class TIndirectReferable {
    friend class TPtr;
    TValue *Current;

public:
    class TPtr {
        const TValue *Object = nullptr;

    public:
        TPtr() = default;

        TPtr(const TValue *object)
            : Object(object)
        {}

        operator const TValue*() const {
            return Get();
        }

        const TValue *operator ->() const {
            return *this;
        }

        const TValue& operator *() const {
            const TValue *ptr = Get();
            Y_DEBUG_ABORT_UNLESS(ptr);
            return *ptr;
        }

        // MUST be called from OnCommit() only
        TValue& Mutable() const {
            TValue *ptr = Get();
            Y_DEBUG_ABORT_UNLESS(ptr);
            return *ptr;
        }

    private:
        TValue *Get() const {
            return Object ? Object->Current : nullptr;
        }
    };

public:
    TIndirectReferable()
        : Current(static_cast<TValue*>(this))
    {}

    TIndirectReferable(const TIndirectReferable&)
        : TIndirectReferable() // prevent from copying link -- set it up for outselves
    {}

    TIndirectReferable& operator =(const TIndirectReferable&) = delete;
    TIndirectReferable& operator =(TIndirectReferable&&) = delete;

    // TOverlayMap machinery
    void OnClone(const THolder<TValue>& clone) {
        Y_DEBUG_ABORT_UNLESS(Current == this);
        Current = clone.Get();
    }

    std::pair<void**, void*> Preserve() {
        Y_DEBUG_ABORT_UNLESS(Current != this);
        return std::make_pair((void**)&Current, (void*)std::exchange(Current, static_cast<TValue*>(this)));
    }

    void OnRollback() {
        Current = static_cast<TValue*>(this);
    }
};
