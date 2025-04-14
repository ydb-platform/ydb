/*******************************************************************************
 * tlx/container/lru_cache.hpp
 *
 * An expected O(1) LRU cache which contains a set of key -> value associations.
 * Loosely based on https://github.com/lamerman/cpp-lru-cache by Alexander
 * Ponomarev. Rewritten for Thrill's block pool, then generalized for tlx.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_LRU_CACHE_HEADER
#define TLX_CONTAINER_LRU_CACHE_HEADER

#include <cassert>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace tlx {

//! \addtogroup tlx_container
//! \{

/*!
 * This is an expected O(1) LRU cache which contains a set of key-only
 * elements. Elements can be put() into LRU cache, and tested for existence
 * using exists(). Insertion and touch() will remark the elements as most
 * recently used, pushing all other back in priority. The LRU cache itself does
 * not limit the number of items, because it has no eviction mechanism. Instead,
 * the user program must check size() after an insert and may extract the least
 * recently used element.
 */
template <typename Key, typename Alloc = std::allocator<Key> >
class LruCacheSet
{
protected:
    using List = typename std::list<Key, Alloc>;
    using ListIterator = typename List::iterator;

    using Map = typename std::unordered_map<
        Key, ListIterator, std::hash<Key>, std::equal_to<Key>,
        typename std::allocator_traits<Alloc>::template rebind_alloc<
            std::pair<const Key, ListIterator> > >;

public:
    explicit LruCacheSet(const Alloc& alloc = Alloc())
        : list_(alloc),
          map_(0, std::hash<Key>(), std::equal_to<Key>(), alloc) { }

    //! clear LRU
    void clear() {
        list_.clear();
        map_.clear();
    }

    //! put or replace/touch item in LRU cache
    void put(const Key& key) {
        // first try to find an existing key
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
        }

        // insert key into linked list at the front (most recently used)
        list_.push_front(key);
        // store iterator to linked list entry in map
        map_.insert(std::make_pair(key, list_.begin()));
    }

    //! touch value from LRU cache for key.
    void touch(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {

            list_.splice(list_.begin(), list_, it->second);
        }
    }

    //! touch value from LRU cache for key.
    bool touch_if_exists(const Key& key) noexcept {
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.splice(list_.begin(), list_, it->second);
            return true;
        }
        return false;
    }

    //! remove key from LRU cache
    void erase(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.erase(it->second);
            map_.erase(it);
        }
    }

    //! remove key from LRU cache
    bool erase_if_exists(const Key& key) noexcept {
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
            return true;
        }
        return false;
    }

    //! test if key exists in LRU cache
    bool exists(const Key& key) const {
        return map_.find(key) != map_.end();
    }

    //! return number of items in LRU cache
    size_t size() const noexcept {
        return map_.size();
    }

    //! return the least recently used key value pair
    Key pop() {
        assert(size());
        typename List::iterator last = list_.end();
        --last;
        Key out = *last;
        map_.erase(out);
        list_.pop_back();
        return out;
    }

private:
    //! list of entries in least-recently used order.
    List list_;
    //! map for accelerated access to keys
    Map map_;
};

/*!
 * This is an expected O(1) LRU cache which contains a map of (key -> value)
 * elements. Elements can be put() by key into LRU cache, and later retrieved
 * with get() using the same key. Insertion and retrieval will remark the
 * elements as most recently used, pushing all other back in priority. The LRU
 * cache itself does not limit the number of items, because it has no eviction
 * mechanism. Instead, the user program must check size() before or after an
 * insert and may extract the least recently used element.
 */
template <typename Key, typename Value,
          typename Alloc = std::allocator<std::pair<Key, Value> > >
class LruCacheMap
{
public:
    using KeyValuePair = typename std::pair<Key, Value>;

protected:
    using List = typename std::list<KeyValuePair, Alloc>;
    using ListIterator = typename List::iterator;

    using Map = typename std::unordered_map<
        Key, ListIterator, std::hash<Key>, std::equal_to<Key>,
        typename std::allocator_traits<Alloc>::template rebind_alloc<
            std::pair<const Key, ListIterator> > >;

public:
    explicit LruCacheMap(const Alloc& alloc = Alloc())
        : list_(alloc),
          map_(0, std::hash<Key>(), std::equal_to<Key>(), alloc) { }

    //! clear LRU
    void clear() {
        list_.clear();
        map_.clear();
    }

    //! put or replace/touch item in LRU cache
    void put(const Key& key, const Value& value) {
        // first try to find an existing key
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
        }

        // insert key into linked list at the front (most recently used)
        list_.push_front(KeyValuePair(key, value));
        // store iterator to linked list entry in map
        map_.insert(std::make_pair(key, list_.begin()));
    }

    //! touch pair in LRU cache for key. Throws if it is not in the map.
    void touch(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.splice(list_.begin(), list_, it->second);
        }
    }

    //! touch pair in LRU cache for key. Returns true if it exists.
    bool touch_if_exists(const Key& key) noexcept {
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.splice(list_.begin(), list_, it->second);
            return true;
        }
        return false;
    }

    //! remove key from LRU cache
    void erase(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.erase(it->second);
            map_.erase(it);
        }
    }

    //! remove key from LRU cache
    bool erase_if_exists(const Key& key) noexcept {
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
            return true;
        }
        return false;
    }

    //! get and touch value from LRU cache for key.
    const Value& get(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            return it->second->second;
        }
    }

    //! get and touch value from LRU cache for key.
    const Value& get_touch(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.splice(list_.begin(), list_, it->second);
            return it->second->second;
        }
    }

    //! test if key exists in LRU cache
    bool exists(const Key& key) const {
        return map_.find(key) != map_.end();
    }

    //! return number of items in LRU cache
    size_t size() const noexcept {
        return map_.size();
    }

    //! return the least recently used key value pair
    KeyValuePair pop() {
        assert(size());
        typename List::iterator last = list_.end();
        --last;
        KeyValuePair out = *last;
        map_.erase(last->first);
        list_.pop_back();
        return out;
    }

private:
    //! list of entries in least-recently used order.
    List list_;
    //! map for accelerated access to keys
    Map map_;
};

//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_LRU_CACHE_HEADER

/******************************************************************************/
