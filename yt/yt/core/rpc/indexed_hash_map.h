#pragma once

#include <util/generic/hash.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! A hash map wrapper that supports random access to some ordering of its elements.
//! NB: Note that the order of elements in the container can change after an element is erased.
template <class TKey, class TValue>
class TIndexedHashMap
{
public:
    using value_type = std::pair<TKey, TValue>;
    using TUnderlyingStorage = std::vector<value_type>;
    using iterator = typename TUnderlyingStorage::iterator;
    using const_iterator = typename TUnderlyingStorage::const_iterator;

    //! Returns true if the key has been inserted and false if it already existed and was overridden.
    bool Set(const TKey& key, const TValue& value)
    {
        if (auto it = KeyToIndex_.find(key); it != KeyToIndex_.end()) {
            Data_[it->second].second = value;
            return false;
        } else {
            KeyToIndex_[key] = Data_.size();
            Data_.push_back({key, value});
            return true;
        }
    }

    //! Returns true if an element was actually erased.
    bool Erase(const TKey& key)
    {
        if (auto it = KeyToIndex_.find(key); it != KeyToIndex_.end()) {
            Erase(it->second);
            return true;
        }
        return false;
    }

    //! NB: Index must be in range [0, Size()).
    void Erase(int index)
    {
        YT_VERIFY(0 <= index && index < Size());

        int endIndex = Size() - 1;
        if (index != endIndex) {
            std::swap(Data_[index], Data_[endIndex]);
            KeyToIndex_[Data_[index].first] = index;
        }

        KeyToIndex_.erase(Data_.back().first);
        Data_.pop_back();
    }

    //! NB: Key must be present in set.
    const TValue& Get(const TKey& key) const
    {
        auto it = KeyToIndex_.find(key);
        YT_VERIFY(it != KeyToIndex_.end());

        return Data_[it->second].second;
    }

    //! NB: Index must be in range [0, Size()).
    const std::pair<TKey, TValue>& operator[](int index) const
    {
        YT_VERIFY(0 <= index && index < Size());

        return Data_[index];
    }

    const std::pair<TKey, TValue>& GetRandomElement() const
    {
        YT_VERIFY(Size() > 0);

        return (*this)[RandomNumber<size_t>(Size())];
    }

    int Size() const
    {
        return Data_.size();
    }

    void Clear()
    {
        Data_.clear();
        KeyToIndex_.clear();
    }

    // Iterator support.

    iterator begin()
    {
        return Data_.begin();
    }

    const_iterator begin() const
    {
        return Data_.begin();
    }

    iterator end()
    {
        return Data_.end();
    }

    const_iterator end() const
    {
        return Data_.end();
    }

    iterator find(const TKey& key)
    {
        auto it = KeyToIndex_.find(key);
        if (it != KeyToIndex_.end()) {
            return Data_.begin() + it->second;
        }
        return Data_.end();
    }

    const_iterator find(const TKey& key) const
    {
        auto it = KeyToIndex_.find(key);
        if (it != KeyToIndex_.end()) {
            return Data_.begin() + it->second;
        }
        return Data_.end();
    }

private:
    // TODO(achulkov2): Change this to deque + hashable pointer wrapper to avoid storing the key twice.
    THashMap<TKey, int> KeyToIndex_;
    TUnderlyingStorage Data_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
