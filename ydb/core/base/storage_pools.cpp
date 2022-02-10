#include "storage_pools.h"

NKikimr::TStoragePool::TStoragePool(const NKikimrStoragePool::TStoragePool &pool)
    : TBase(pool.GetName(), pool.GetKind())
{}

NKikimr::TStoragePool::operator NKikimrStoragePool::TStoragePool() const {
    NKikimrStoragePool::TStoragePool pool;
    pool.SetName(GetName());
    pool.SetKind(GetKind());
    return pool;
}
