#include "storage_pools.h"

#include <util/generic/algorithm.h>

NKikimr::TStoragePool::TStoragePool(const NKikimrStoragePool::TStoragePool &pool)
    : TBase(pool.GetName(), pool.GetKind())
{}

NKikimr::TStoragePool::operator NKikimrStoragePool::TStoragePool() const {
    NKikimrStoragePool::TStoragePool pool;
    pool.SetName(GetName());
    pool.SetKind(GetKind());
    return pool;
}

bool NKikimr::BindChannelByPoolKind(TChannelBind& binding, const TStoragePools& pools, TStringBuf kind) {
    const auto it = FindIf(pools, [&](const TStoragePool& pool) {
        return pool.GetKind() == kind;
    });
    if (it == pools.end()) {
        return false;
    }

    binding.SetStoragePoolName(it->GetName());
    binding.SetStoragePoolKind(it->GetKind());
    return true;
}
