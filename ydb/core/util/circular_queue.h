#pragma once

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/vector.h>

namespace NKikimr {

template <typename T>
class TFifoQueue {
public:
    struct TItem : public TIntrusiveListItem<TItem> {
        T Data;

        explicit TItem(const T& data)
            : Data(data)
        {}

        explicit TItem(T&& data)
            : Data(std::move(data))
        {}

        bool operator == (const TItem& other) const {
            return Data == other.Data;
        }

        bool operator == (const T& other) const {
            return Data == other;
        }

        struct THash {
            size_t operator() (const TItem& item) const {
                return ::THash<T>()(item.Data);
            }

            size_t operator() (const T& item) const {
                return ::THash<T>()(item);
            }
        };

        struct TEqual {
            template<typename T1, typename T2>
            bool operator() (const T1& l, const T2& r) const {
                return l == r;
            }
        };
    };

private:
    using TList = TIntrusiveList<TItem>;
    using TIndex = THashSet<TItem, typename TItem::THash, typename TItem::TEqual>;

private:
    TList List;
    TIndex Index;

public:
    TFifoQueue() = default;

    template<typename T2>
    bool Enqueue(T2&& item) {
        return PushBack(std::forward<T2>(item));
    }

    template<typename T2>
    bool PushBack(T2&& item) {
        typename TIndex::insert_ctx insert_ctx;
        auto it = Index.find(item, insert_ctx);
        if (it != Index.end()) {
            // seems to be ok to simply ignore
            return false;
        }

        auto indexIt = Index.emplace_direct(insert_ctx, std::forward<T2>(item));
        List.PushBack(const_cast<TItem*>(&*indexIt));

        return true;
    }

    bool Remove(const T& item) {
        auto it = Index.find(item);
        if (it == Index.end())
            return false;

        const_cast<TItem&>(*it).Unlink();
        Index.erase(it);
        return true;
    }

    bool UpdateIfFound(const T& item) {
        auto it = Index.find(item);
        if (it == Index.end())
            return false;

        const_cast<TItem&>(*it).Data = item;
        return true;
    }

    // item contains just key, the rest
    // is copied if item found
    bool CopyAndRemove(T& item) {
        auto it = Index.find(item);
        if (it == Index.end())
            return false;

        item = std::move(it->Data);

        const_cast<TItem&>(*it).Unlink();
        Index.erase(it);
        return true;
    }

    void Clear() {
        List.Clear();
        Index.clear();
    }

    const T& Front() const {
        return List.Front()->Data;
    }

    void PopFront() {
        Remove(Front());
    }

    bool Empty() const {
        return Index.empty();
    }

    size_t Size() const {
        return Index.size();
    }

    // copies items, should be used in tests only
    TVector<T> GetQueue() const {
        TVector<T> result;
        result.reserve(Index.size());
        for (const auto& item: List) {
            result.push_back(item.Data);
        }
        return result;
    }

    // used to implement TCircularQueue over TFifoQueue
    void PopFrontToBack() {
        if (Index.size() > 1)
            List.PushBack(List.Front());
    }
};

template <typename T, typename TQueue>
class TCircularQueue {
private:
    TQueue Queue;

public:
    template<typename T2>
    bool Enqueue(T2&& item) {
        return Queue.Enqueue(std::forward<T2>(item));
    }

    template<typename T2>
    bool PushBack(T2&& item) {
        return Queue.PushBack(std::forward<T2>(item));
    }

    bool Remove(const T& item) {
        return Queue.Remove(item);
    }

    void Clear() {
        Queue.Clear();
    }

    const T &Front() const {
        return Queue.Front();
    }

    void PopFront() {
        Queue.PopFrontToBack();
    }

    bool Empty() const {
        return Queue.Empty();
    }

    size_t Size() const {
        return Queue.Size();
    }

    // copies items, should be used in tests only
    TVector<T> GetQueue() const {
        return Queue.GetQueue();
    }
};

} // NKikimr
