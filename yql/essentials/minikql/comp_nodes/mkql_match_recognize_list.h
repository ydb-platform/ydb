#pragma once

#include "mkql_match_recognize_save_load.h"

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <unordered_map>
#include <utility>

namespace NKikimr::NMiniKQL::NMatchRecognize {

/// Stores only locked items
/// Locks are holds by TRange
/// When all locks on an item are released, the item is removed from the list
class TSparseList {
    struct TItem {
        NUdf::TUnboxedValue Value;
        size_t LockCount = 0;
    };

    class TContainer: public TSimpleRefCount<TContainer> {
    public:
        using TPtr = TIntrusivePtr<TContainer>;
        // TODO consider to replace hash table with contiguous chunks
        using TStorage = TMKQLHashMap<size_t, TItem>;
        using iterator = TStorage::const_iterator;

        [[nodiscard]] iterator Begin() const noexcept {
            return Storage_.begin();
        }

        [[nodiscard]] iterator End() const noexcept {
            return Storage_.end();
        }

        [[nodiscard]] size_t Size() const noexcept {
            return Storage_.size();
        }

        [[nodiscard]] size_t Empty() const noexcept {
            return Storage_.empty();
        }

        [[nodiscard]] bool Contains(size_t i) const noexcept {
            return Storage_.find(i) != Storage_.cend();
        }

        [[nodiscard]] NUdf::TUnboxedValue Get(size_t i) const {
            if (const auto it = Storage_.find(i); it != Storage_.cend()) {
                return it->second.Value;
            } else {
                return NUdf::TUnboxedValue{};
            }
        }

        void Add(size_t index, NUdf::TUnboxedValue&& value) {
            const auto& [iter, newOne] = Storage_.emplace(index, TItem{.Value = std::move(value), .LockCount = 1});
            MKQL_ENSURE(newOne, "Internal logic error");
        }

        void LockRange(size_t from, size_t to) {
            for (auto i = from; i <= to; ++i) {
                const auto it = Storage_.find(i);
                MKQL_ENSURE(it != Storage_.cend(), "Internal logic error");
                ++it->second.LockCount;
            }
        }

        void UnlockRange(size_t from, size_t to) {
            for (auto i = from; i <= to; ++i) {
                const auto it = Storage_.find(i);
                MKQL_ENSURE(it != Storage_.cend(), "Internal logic error");
                auto lockCount = --it->second.LockCount;
                if (0 == lockCount) {
                    Storage_.erase(it);
                }
            }
        }

        void Save(TMrOutputSerializer& serializer) const {
            serializer(Storage_.size());
            for (const auto& [key, item] : Storage_) {
                serializer(key, item.Value, item.LockCount);
            }
        }

        void Load(TMrInputSerializer& serializer) {
            auto size = serializer.Read<TStorage::size_type>();
            Storage_.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                TStorage::key_type key;
                NUdf::TUnboxedValue row;
                decltype(TItem::LockCount) lockCount;
                serializer(key, row, lockCount);
                Storage_.emplace(key, TItem{.Value = row, .LockCount = lockCount});
            }
        }

    private:
        TStorage Storage_;
    };

public:
    /// Range that includes starting and ending points
    /// Holds a lock on items in the list
    /// Can not be empty, but can be in invalid state, with no container set
    class TRange {
        friend class TSparseList;

    public:
        TRange()
            : Container_()
            , FromIndex_(Max())
            , ToIndex_(Max())
            , NfaIndex_(Max())
        {
        }

        TRange(const TRange& other)
            : Container_(other.Container_)
            , FromIndex_(other.FromIndex_)
            , ToIndex_(other.ToIndex_)
            , NfaIndex_(other.NfaIndex_)
        {
            LockRange(FromIndex_, ToIndex_);
        }

        TRange(TRange&& other)
            : Container_(std::move(other.Container_))
            , FromIndex_(other.FromIndex_)
            , ToIndex_(other.ToIndex_)
            , NfaIndex_(other.NfaIndex_)
        {
            other.Reset();
        }

        ~TRange() {
            Release();
        }

        TRange& operator=(const TRange& other) {
            if (&other == this) {
                return *this;
            }
            // TODO(zverevgeny): optimize for overlapped source and destination
            Release();
            Container_ = other.Container_;
            FromIndex_ = other.FromIndex_;
            ToIndex_ = other.ToIndex_;
            NfaIndex_ = other.NfaIndex_;
            LockRange(FromIndex_, ToIndex_);
            return *this;
        }

        TRange& operator=(TRange&& other) {
            if (&other == this) {
                return *this;
            }
            Release();
            Container_ = other.Container_;
            FromIndex_ = other.FromIndex_;
            ToIndex_ = other.ToIndex_;
            NfaIndex_ = other.NfaIndex_;
            other.Reset();
            return *this;
        }

        friend inline bool operator==(const TRange& lhs, const TRange& rhs) {
            return std::tie(lhs.FromIndex_, lhs.ToIndex_, lhs.NfaIndex_) == std::tie(rhs.FromIndex_, rhs.ToIndex_, rhs.NfaIndex_);
        }

        friend inline bool operator<(const TRange& lhs, const TRange& rhs) {
            return std::tie(lhs.FromIndex_, lhs.ToIndex_, lhs.NfaIndex_) < std::tie(rhs.FromIndex_, rhs.ToIndex_, rhs.NfaIndex_);
        }

        bool IsValid() const {
            return static_cast<bool>(Container_) && FromIndex_ != Max<size_t>() && ToIndex_ != Max<size_t>();
        }

        size_t From() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return FromIndex_;
        }

        size_t To() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return ToIndex_;
        }

        [[nodiscard]] size_t NfaIndex() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return NfaIndex_;
        }

        void NfaIndex(size_t index) {
            NfaIndex_ = index;
        }

        size_t Size() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return ToIndex_ - FromIndex_ + 1;
        }

        void Extend() {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            ++ToIndex_;
            LockRange(ToIndex_, ToIndex_);
        }

        void Release() {
            UnlockRange(FromIndex_, ToIndex_);
            Container_.Reset();
            FromIndex_ = Max();
            ToIndex_ = Max();
            NfaIndex_ = Max();
        }

        void Save(TMrOutputSerializer& serializer) const {
            serializer(Container_, FromIndex_, ToIndex_, NfaIndex_);
        }

        void Load(TMrInputSerializer& serializer) {
            serializer(Container_, FromIndex_, ToIndex_);
            if (serializer.GetStateVersion() >= 2U) {
                serializer(NfaIndex_);
            }
        }

    private:
        TRange(TContainer::TPtr container, size_t index)
            : Container_(std::move(container))
            , FromIndex_(index)
            , ToIndex_(index)
            , NfaIndex_(Max())
        {
        }

        void LockRange(size_t from, size_t to) {
            if (Container_) {
                Container_->LockRange(from, to);
            }
        }

        void UnlockRange(size_t from, size_t to) {
            if (Container_) {
                Container_->UnlockRange(from, to);
            }
        }

        void Reset() {
            Container_.Reset();
            FromIndex_ = Max();
            ToIndex_ = Max();
            NfaIndex_ = Max();
        }

        TContainer::TPtr Container_;
        size_t FromIndex_;
        size_t ToIndex_;
        size_t NfaIndex_;
    };

    TRange Append(NUdf::TUnboxedValue&& value) {
        const auto index = ListSize_++;
        Container_->Add(index, std::move(value));
        return TRange(Container_, index);
    }

    using iterator = TContainer::iterator;

    [[nodiscard]] iterator Begin() const noexcept {
        return Container_->Begin();
    }

    [[nodiscard]] iterator End() const noexcept {
        return Container_->End();
    }

    /// Return total size of sparse list including absent values
    size_t LastRowIndex() const noexcept {
        return ListSize_;
    }

    /// Return number of present values in sparse list
    size_t Size() const noexcept {
        return Container_->Size();
    }

    [[nodiscard]] bool Empty() const noexcept {
        return Container_->Empty();
    }

    [[nodiscard]] bool Contains(size_t i) const noexcept {
        return Container_->Contains(i);
    }

    [[nodiscard]] NUdf::TUnboxedValue Get(size_t i) const {
        return Container_->Get(i);
    }

    void Save(TMrOutputSerializer& serializer) const {
        serializer(Container_, ListSize_);
    }

    void Load(TMrInputSerializer& serializer) {
        serializer(Container_, ListSize_);
    }

private:
    TContainer::TPtr Container_ = MakeIntrusive<TContainer>();
    size_t ListSize_ = 0; // impl: max index ever stored + 1
};

class TListValue final: public TComputationValue<TListValue> {
public:
    TListValue(TMemoryUsageInfo* memUsage, TSparseList list);

    bool HasFastListLength() const final;
    ui64 GetListLength() const final;
    ui64 GetEstimatedListLength() const final;
    NUdf::TUnboxedValue GetListIterator() const final;
    bool HasListItems() const final;

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const final;

    ui64 GetDictLength() const final;
    NUdf::TUnboxedValue GetDictIterator() const final;
    NUdf::TUnboxedValue GetKeysIterator() const final;
    NUdf::TUnboxedValue GetPayloadsIterator() const final;
    bool Contains(const NUdf::TUnboxedValuePod& key) const final;
    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final;
    bool HasDictItems() const final;

private:
    TSparseList List_;
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
