//
// LRUCache.h
//
// Library: Foundation
// Package: Cache
// Module:  LRUCache
//
// Definition of the LRUCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_LRUCache_INCLUDED
#define DB_Foundation_LRUCache_INCLUDED


#include "DBPoco/AbstractCache.h"
#include "DBPoco/LRUStrategy.h"


namespace DBPoco
{


template <class TKey, class TValue, class TMutex = FastMutex, class TEventMutex = FastMutex>
class LRUCache : public AbstractCache<TKey, TValue, LRUStrategy<TKey, TValue>, TMutex, TEventMutex>
/// An LRUCache implements Least Recently Used caching. The default size for a cache is 1024 entries.
{
public:
    LRUCache(long size = 1024)
        : AbstractCache<TKey, TValue, LRUStrategy<TKey, TValue>, TMutex, TEventMutex>(LRUStrategy<TKey, TValue>(size))
    {
    }

    ~LRUCache() { }

private:
    LRUCache(const LRUCache & aCache);
    LRUCache & operator=(const LRUCache & aCache);
};


} // namespace DBPoco


#endif // DB_Foundation_LRUCache_INCLUDED
