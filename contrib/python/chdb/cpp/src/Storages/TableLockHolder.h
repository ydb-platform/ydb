#pragma once

#include <Common/RWLock.h>

namespace DB_CHDB
{

using TableLockHolder = RWLockImpl::LockHolder;

/// Table exclusive lock, holds both alter and drop locks. Useful for DROP-like
/// queries.
struct TableExclusiveLockHolder
{
    void release() { *this = TableExclusiveLockHolder(); }

private:
    friend class IStorage;

    TableLockHolder drop_lock;
};

}
