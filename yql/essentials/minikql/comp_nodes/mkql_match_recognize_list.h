#pragma once

#include "mkql_match_recognize_save_load.h"

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <unordered_map>

namespace NKikimr::NMiniKQL::NMatchRecognize {

///Stores only locked items
///Locks are holds by TRange
///When all locks on an item are released, the item is removed from the list
class TSparseList {
    struct TItem {
        NUdf::TUnboxedValue Value;
        size_t LockCount = 0;
    };

    class TContainer: public TSimpleRefCount<TContainer> {
    public:
        using TPtr = TIntrusivePtr<TContainer>;
        //TODO consider to replace hash table with contiguous chunks
        using TStorage = TMKQLHashMap<size_t, TItem>;
        using iterator = TStorage::const_iterator;

        [[nodiscard]] iterator Begin() const noexcept {
            return Storage.begin();
        }

        [[nodiscard]] iterator End() const noexcept {
            return Storage.end();
        }

        [[nodiscard]] size_t Size() const noexcept {
            return Storage.size();
        }

        [[nodiscard]] size_t Empty() const noexcept {
            return Storage.empty();
        }

        [[nodiscard]] bool Contains(size_t i) const noexcept {
            return Storage.find(i) != Storage.cend();
        }

        [[nodiscard]] NUdf::TUnboxedValue Get(size_t i) const {
            if (const auto it = Storage.find(i); it != Storage.cend()) {
                return it->second.Value;
            } else {
                return NUdf::TUnboxedValue{};
            }
        }

        void Add(size_t index, NUdf::TUnboxedValue&& value) {
            const auto& [iter, newOne] = Storage.emplace(index, TItem{std::move(value), 1});
            MKQL_ENSURE(newOne, "Internal logic error");
        }

        void LockRange(size_t from, size_t to) {
            for (auto i = from; i <= to; ++i) {
                const auto it = Storage.find(i);
                MKQL_ENSURE(it != Storage.cend(), "Internal logic error");
                ++it->second.LockCount;
            }
        }

        void UnlockRange(size_t from, size_t to) {
            for (auto i = from; i <= to; ++i) {
                const auto it = Storage.find(i);
                MKQL_ENSURE(it != Storage.cend(), "Internal logic error");
                auto lockCount = --it->second.LockCount;
                if (0 == lockCount) {
                    Storage.erase(it);
                }
            }
        }

        void Save(TMrOutputSerializer& serializer) const {
            serializer(Storage.size());
            for (const auto& [key, item]: Storage) {
                serializer(key, item.Value, item.LockCount);
            }
        }

        void Load(TMrInputSerializer& serializer) {
            auto size = serializer.Read<TStorage::size_type>();
            Storage.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                TStorage::key_type key;
                NUdf::TUnboxedValue row;
                decltype(TItem::LockCount) lockCount;
                serializer(key, row, lockCount);
                Storage.emplace(key, TItem{row, lockCount});
            }
        }

    private:
        TStorage Storage;
    };

public:
    ///Range that includes starting and ending points
    ///Holds a lock on items in the list
    ///Can not be empty, but can be in invalid state, with no container set
    class TRange{
        friend class TSparseList;
    public:
        TRange()
            : Container()
            , FromIndex(Max())
            , ToIndex(Max())
            , NfaIndex_(Max())
        {
        }

        TRange(const TRange& other)
            : Container(other.Container)
            , FromIndex(other.FromIndex)
            , ToIndex(other.ToIndex)
            , NfaIndex_(other.NfaIndex_)
        {
            LockRange(FromIndex, ToIndex);
        }

        TRange(TRange&& other)
            : Container(other.Container)
            , FromIndex(other.FromIndex)
            , ToIndex(other.ToIndex)
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
            //TODO(zverevgeny): optimize for overlapped source and destination
            Release();
            Container = other.Container;
            FromIndex = other.FromIndex;
            ToIndex = other.ToIndex;
            NfaIndex_ = other.NfaIndex_;
            LockRange(FromIndex, ToIndex);
            return *this;
        }

        TRange& operator=(TRange&& other) {
            if (&other == this) {
                return *this;
            }
            Release();
            Container = other.Container;
            FromIndex = other.FromIndex;
            ToIndex = other.ToIndex;
            NfaIndex_ = other.NfaIndex_;
            other.Reset();
            return *this;
        }

        friend inline bool operator==(const TRange& lhs, const TRange& rhs) {
            return std::tie(lhs.FromIndex, lhs.ToIndex, lhs.NfaIndex_) == std::tie(rhs.FromIndex, rhs.ToIndex, rhs.NfaIndex_);
        }

        friend inline bool operator<(const TRange& lhs, const TRange& rhs) {
            return std::tie(lhs.FromIndex, lhs.ToIndex, lhs.NfaIndex_) < std::tie(rhs.FromIndex, rhs.ToIndex, rhs.NfaIndex_);
        }

        bool IsValid() const {
            return static_cast<bool>(Container) && FromIndex != Max<size_t>() && ToIndex != Max<size_t>();
        }

        size_t From() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return FromIndex;
        }

        size_t To() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return ToIndex;
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
            return ToIndex - FromIndex + 1;
        }

        void Extend() {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            ++ToIndex;
            LockRange(ToIndex, ToIndex);
        }

        void Release() {
            UnlockRange(FromIndex, ToIndex);
            Container.Reset();
            FromIndex = Max();
            ToIndex = Max();
            NfaIndex_ = Max();
        }

        void Save(TMrOutputSerializer& serializer) const {
            serializer(Container, FromIndex, ToIndex, NfaIndex_);
       }

        void Load(TMrInputSerializer& serializer) {
            serializer(Container, FromIndex, ToIndex);
            if (serializer.GetStateVersion() >= 2U) {
                serializer(NfaIndex_);
            }
        }

    private:
        TRange(TContainer::TPtr container, size_t index)
            : Container(container)
            , FromIndex(index)
            , ToIndex(index)
            , NfaIndex_(Max())
        {}

        void LockRange(size_t from, size_t to) {
            if (Container) {
                Container->LockRange(from, to);
            }
        }

        void UnlockRange(size_t from, size_t to) {
            if (Container) {
                Container->UnlockRange(from, to);
            }
        }

        void Reset() {
            Container.Reset();
            FromIndex = Max();
            ToIndex = Max();
            NfaIndex_ = Max();
        }

        TContainer::TPtr Container;
        size_t FromIndex;
        size_t ToIndex;
        size_t NfaIndex_;
    };

    TRange Append(NUdf::TUnboxedValue&& value) {
        const auto index = ListSize++;
        Container->Add(index, std::move(value));
        return TRange(Container, index);
    }

    using iterator = TContainer::iterator;

    [[nodiscard]] iterator Begin() const noexcept {
        return Container->Begin();
    }

    [[nodiscard]] iterator End() const noexcept {
        return Container->End();
    }

    ///Return total size of sparse list including absent values
    size_t LastRowIndex() const noexcept {
        return ListSize;
    }

    ///Return number of present values in sparse list
    size_t Size() const noexcept {
        return Container->Size();
    }

    [[nodiscard]] bool Empty() const noexcept {
        return Container->Empty();
    }

    [[nodiscard]] bool Contains(size_t i) const noexcept {
        return Container->Contains(i);
    }

    [[nodiscard]] NUdf::TUnboxedValue Get(size_t i) const {
        return Container->Get(i);
    }

    void Save(TMrOutputSerializer& serializer) const {
        serializer(Container, ListSize);
    }

    void Load(TMrInputSerializer& serializer) {
        serializer(Container, ListSize);
    }

private:
    TContainer::TPtr Container = MakeIntrusive<TContainer>();
    size_t ListSize = 0; //impl: max index ever stored + 1
};

class TListValue final : public TComputationValue<TListValue> {
public:
    TListValue(TMemoryUsageInfo* memUsage, const TSparseList& list);

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
    TSparseList List;
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
