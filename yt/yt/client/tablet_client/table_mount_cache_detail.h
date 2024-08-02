#pragma once

#include "table_mount_cache.h"

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletInfoOwnerCache
{
public:
    explicit TTabletInfoOwnerCache(NLogging::TLogger logger);

    void Insert(TTabletId tabletId, TWeakPtr<TTableMountInfo> tableInfo);
    std::vector<TWeakPtr<TTableMountInfo>> GetOwners(TTabletId tabletId);
    void Clear();

private:
    const NLogging::TLogger Logger;

    std::atomic<NProfiling::TCpuInstant> ExpiredEntriesSweepDeadline_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MapLock_);
    THashMap<TTabletId, std::vector<TWeakPtr<TTableMountInfo>>> Map_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, GCLock_);
    std::queue<TTabletId> GCQueue_;
    std::vector<TTabletId> ExpiredTabletIds_;

    void SweepExpiredEntries();
    void ProcessNextGCQueueEntry();

    void DropExpiredOwners(std::vector<TWeakPtr<TTableMountInfo>>* owners);
};

///////////////////////////////////////////////////////////////////////////////

class TTableMountCacheBase
    : public ITableMountCache
    , public TAsyncExpiringCache<NYPath::TYPath, TTableMountInfoPtr>
{
public:
    TTableMountCacheBase(
        TTableMountCacheConfigPtr config,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler = {});

    TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path) override;
    void InvalidateTablet(TTabletId tabletId) override;
    TInvalidationResult InvalidateOnError(
        const TError& error,
        bool forceRetry) override;

    void Clear() override;

    void Reconfigure(TTableMountCacheConfigPtr config) override;

protected:
    const NLogging::TLogger Logger;

    TTabletInfoOwnerCache TabletInfoOwnerCache_;

    virtual void InvalidateTable(const TTableMountInfoPtr& tableInfo) = 0;

    virtual void RegisterCell(NYTree::INodePtr cellDescriptor);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    TTableMountCacheConfigPtr Config_;

    TTabletInfoPtr FindTabletInfo(TTabletId tabletId);

    std::optional<TInvalidationResult> TryHandleServantNotActiveError(
        const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
