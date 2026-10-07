#pragma once

#include "bridge_abi.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <utility>

namespace NYdb::NUdfStore::NAbi {

//! Move-only RAII handle into the host bridge node table.
class TBridgeValue {
public:
    TBridgeValue() = default;

    explicit TBridgeValue(TBridgeHandle handle, bool owned = true)
        : Handle_(handle)
        , Owned_(owned && handle != 0)
    {
    }

    TBridgeValue(const TBridgeValue&) = delete;
    TBridgeValue& operator=(const TBridgeValue&) = delete;

    TBridgeValue(TBridgeValue&& other) noexcept
        : Handle_(other.Handle_)
        , Owned_(other.Owned_)
    {
        other.Handle_ = 0;
        other.Owned_ = false;
    }

    TBridgeValue& operator=(TBridgeValue&& other) noexcept {
        if (this != &other) {
            Reset();
            Handle_ = other.Handle_;
            Owned_ = other.Owned_;
            other.Handle_ = 0;
            other.Owned_ = false;
        }
        return *this;
    }

    ~TBridgeValue() {
        Reset();
    }

    TBridgeHandle Get() const {
        return Handle_;
    }

    explicit operator bool() const {
        return Handle_ != 0 && !BridgeIsNull(Handle_);
    }

    int32_t Kind() const {
        return BridgeGetKind(Handle_);
    }

    //! Guest state cached for this value; 0 until SetUserData.
    uint64_t UserData() const {
        return BridgeGetUserData(Handle_);
    }

    void SetUserData(uint64_t value) const {
        BridgeSetUserData(Handle_, value);
    }

    void Reset() {
        if (Owned_ && Handle_ != 0) {
            BridgeUnref(Handle_);
        }
        Handle_ = 0;
        Owned_ = false;
    }

    //! Stop owning the handle and return it raw. Use this to hand a value to
    //! the `result` slot of a UDF entry point: the host reads that slot after
    //! the call returns, and the run-scope reference it holds is the only one
    //! keeping the node alive, so letting the destructor drop it would leave
    //! the host reading a handle whose node is already gone.
    TBridgeHandle Release() {
        const TBridgeHandle handle = Handle_;
        Handle_ = 0;
        Owned_ = false;
        return handle;
    }

protected:
    TBridgeHandle Handle_ = 0;
    bool Owned_ = false;
};

class TBridgeString: public TBridgeValue {
public:
    using TBridgeValue::TBridgeValue;

    int64_t Len() const {
        return BridgeGetStringLen(Handle_);
    }

    //! Copy into a caller-provided buffer; returns bytes copied.
    int64_t CopyTo(char* dst, int64_t cap) const {
        return BridgeCopyString(Handle_, reinterpret_cast<uint64_t>(dst), cap);
    }

    //! Lazily pin into compartment LM; same offset on repeat for this handle.
    uint64_t EnsureInCompartment() const {
        return BridgeEnsureString(Handle_);
    }
};

class TBridgeList: public TBridgeValue {
public:
    using TBridgeValue::TBridgeValue;

    int64_t Length() const {
        return BridgeListLength(Handle_);
    }

    bool HasItems() const {
        return BridgeListHasItems(Handle_) != 0;
    }

    class TIterator {
    public:
        explicit TIterator(TBridgeHandle listHandle)
            : Iter_(BridgeListMakeIterator(listHandle), true)
        {
            Advance();
        }

        bool AtEnd() const {
            return !Has_;
        }

        TBridgeValue Take() {
            Has_ = false;
            return std::move(Current_);
        }

        void Next() {
            Advance();
        }

    private:
        void Advance() {
            TBridgeHandle item = 0;
            Has_ = BridgeListIterNext(Iter_.Get(), &item) != 0;
            if (Has_) {
                Current_ = TBridgeValue(item, true);
            } else {
                Current_.Reset();
            }
        }

        TBridgeValue Iter_;
        TBridgeValue Current_;
        bool Has_ = false;
    };

    //! range-for over list items; TIterator must be complete before TIt embeds it.
    class TRange {
    public:
        explicit TRange(TBridgeHandle list)
            : List_(list)
        {
        }

        struct TSentinel {};

        class TIt {
        public:
            explicit TIt(TBridgeHandle list)
                : Impl_(list)
            {
            }

            bool operator!=(TSentinel) const {
                return !Impl_.AtEnd();
            }

            TBridgeValue operator*() {
                return Impl_.Take();
            }

            void operator++() {
                Impl_.Next();
            }

        private:
            TIterator Impl_;
        };

        TIt begin() const {
            return TIt(List_);
        }

        TSentinel end() const {
            return {};
        }

    private:
        TBridgeHandle List_;
    };

    TRange Items() const {
        return TRange(Handle_);
    }
};

class TBridgeDict: public TBridgeValue {
public:
    using TBridgeValue::TBridgeValue;

    int64_t Length() const {
        return BridgeDictLength(Handle_);
    }

    bool HasItems() const {
        return BridgeDictHasItems(Handle_) != 0;
    }

    bool Contains(const TBridgeValue& key) const {
        return BridgeDictContains(Handle_, key.Get()) != 0;
    }

    //! Owned payload handle, or an empty value when the key is missing. A key
    //! holding a null payload gives a live handle that still tests false, so
    //! compare Get() against 0 (or ask Contains) to tell the two apart.
    TBridgeValue Lookup(const TBridgeValue& key) const {
        const auto h = BridgeDictLookup(Handle_, key.Get());
        return TBridgeValue(h, h != 0);
    }
};

inline TBridgeValue MakeNull() {
    return TBridgeValue(BridgeMakeNull(), true);
}

inline TBridgeValue MakeInt64(int64_t v) {
    return TBridgeValue(BridgeMakeInt64(v), true);
}

inline TBridgeValue MakeUint64(uint64_t v) {
    return TBridgeValue(BridgeMakeUint64(v), true);
}

inline TBridgeValue MakeDouble(double v) {
    return TBridgeValue(BridgeMakeDouble(v), true);
}

inline TBridgeValue MakeBool(bool v) {
    return TBridgeValue(BridgeMakeBool(v ? 1 : 0), true);
}

inline TBridgeValue MakeString(const char* data, int64_t len) {
    return TBridgeValue(
        BridgeMakeString(reinterpret_cast<uint64_t>(data), len),
        true);
}

//! `inner` keeps its own reference: the host may well answer with the very
//! handle it was given, since MiniKQL represents Optional over a boxed value
//! or a refcounted string as the payload itself.
inline TBridgeValue MakeOptional(const TBridgeValue& inner) {
    return TBridgeValue(BridgeMakeOptional(inner.Get()), true);
}

//! `item` keeps its own reference, same as MakeOptional.
inline TBridgeValue MakeVariant(int32_t index, const TBridgeValue& item) {
    return TBridgeValue(BridgeMakeVariant(index, item.Get()), true);
}

//! Build T once per distinct argument value and reuse it on every later row.
//! `build` runs only on the first row for a value; the pointer it returns is
//! kept in that value's user-data slot. Works for any type with identity
//! (String, Dict, List, Resource), not just strings.
template <class T, class TBuild>
inline T* BridgeGetOrBuild(TBridgeHandle handle, TBuild&& build) {
    if (const uint64_t cached = BridgeGetUserData(handle)) {
        return reinterpret_cast<T*>(static_cast<uintptr_t>(cached));
    }
    T* built = build();
    BridgeSetUserData(handle, static_cast<uint64_t>(reinterpret_cast<uintptr_t>(built)));
    return built;
}

//! Free guest state whose value the host stopped tracking. Call it at the top
//! of a UDF entry point; `release` gets the raw user-data values back.
//! A module caching several kinds of state must tag them itself.
template <class TRelease>
inline void BridgeDrainReleasedUserData(TRelease&& release) {
    uint64_t batch[16];
    for (;;) {
        const int32_t taken = BridgeTakeReleasedUserData(
            reinterpret_cast<uint64_t>(batch),
            static_cast<int32_t>(sizeof(batch) / sizeof(batch[0])));
        for (int32_t i = 0; i < taken; ++i) {
            release(batch[i]);
        }
        if (taken < static_cast<int32_t>(sizeof(batch) / sizeof(batch[0]))) {
            return;
        }
    }
}

} // namespace NYdb::NUdfStore::NAbi
