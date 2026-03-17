//
// AccessExpireStrategy.h
//
// Library: Foundation
// Package: Cache
// Module:  AccessExpireStrategy
//
// Definition of the AccessExpireStrategy class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_AccessExpireStrategy_INCLUDED
#define DB_Foundation_AccessExpireStrategy_INCLUDED


#include <map>
#include <set>
#include "DBPoco/Bugcheck.h"
#include "DBPoco/EventArgs.h"
#include "DBPoco/ExpireStrategy.h"
#include "DBPoco/KeyValueArgs.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/ValidArgs.h"


namespace DBPoco
{


template <class TKey, class TValue>
class AccessExpireStrategy : public ExpireStrategy<TKey, TValue>
/// An AccessExpireStrategy implements time and access based expiration of cache entries
{
public:
    AccessExpireStrategy(Timestamp::TimeDiff expireTimeInMilliSec) : ExpireStrategy<TKey, TValue>(expireTimeInMilliSec)
    /// Create an expire strategy. Note that the smallest allowed caching time is 25ms.
    /// Anything lower than that is not useful with current operating systems.
    {
    }

    ~AccessExpireStrategy() { }

    void onGet(const void *, const TKey & key)
    {
        // get triggers an update to the expiration time
        typename ExpireStrategy<TKey, TValue>::Iterator it = this->_keys.find(key);
        if (it != this->_keys.end())
        {
            if (!it->second->first.isElapsed(this->_expireTime)) // don't extend if already expired
            {
                this->_keyIndex.erase(it->second);
                Timestamp now;
                typename ExpireStrategy<TKey, TValue>::IndexIterator itIdx
                    = this->_keyIndex.insert(typename ExpireStrategy<TKey, TValue>::TimeIndex::value_type(now, key));
                it->second = itIdx;
            }
        }
    }
};


} // namespace DBPoco


#endif // DB_Foundation_AccessExpireStrategy_INCLUDED
