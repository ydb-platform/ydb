#include "table_mount_cache_detail.h"

#include "config.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NTabletClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto TabletCacheSweepPeriod = TDuration::Seconds(60);

///////////////////////////////////////////////////////////////////////////////

TTabletInfoCache::TTabletInfoCache(NLogging::TLogger logger)
    : Logger(std::move(logger))
{ }

TTabletInfoPtr TTabletInfoCache::Find(TTabletId tabletId)
{
    SweepExpiredEntries();

    auto guard = ReaderGuard(MapLock_);
    ProcessNextGCQueueEntry();
    auto it = Map_.find(tabletId);
    return it != Map_.end() ? it->second.Lock() : nullptr;
}

TTabletInfoPtr TTabletInfoCache::Insert(const TTabletInfoPtr& tabletInfo)
{
    SweepExpiredEntries();

    auto guard = WriterGuard(MapLock_);
    ProcessNextGCQueueEntry();
    typename decltype(Map_)::insert_ctx context;
    auto it = Map_.find(tabletInfo->TabletId, context);
    if (it != Map_.end()) {
        if (auto existingTabletInfo = it->second.Lock()) {
            if (tabletInfo->MountRevision < existingTabletInfo->MountRevision) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::InvalidMountRevision,
                    "Tablet mount revision %x is outdated",
                    tabletInfo->MountRevision)
                    << TErrorAttribute("tablet_id", tabletInfo->TabletId);
            }

            for (const auto& owner : existingTabletInfo->Owners) {
                if (!owner.IsExpired()) {
                    tabletInfo->Owners.push_back(owner);
                }
            }
        }
        it->second = MakeWeak(tabletInfo);
    } else {
        Map_.emplace_direct(context, tabletInfo->TabletId, tabletInfo);
        guard.Release();

        auto gcGuard = Guard(GCLock_);
        GCQueue_.push(tabletInfo->TabletId);
    }

    return tabletInfo;
}

void TTabletInfoCache::Clear()
{
    decltype(Map_) other;
    {
        auto guard = WriterGuard(MapLock_);
        other = std::move(Map_);
    }
}

void TTabletInfoCache::SweepExpiredEntries()
{
    auto now = NProfiling::GetCpuInstant();
    auto deadline = ExpiredEntriesSweepDeadline_.load(std::memory_order::relaxed);
    if (now < deadline) {
        return;
    }

    if (!ExpiredEntriesSweepDeadline_.compare_exchange_strong(deadline, now + NProfiling::DurationToCpuDuration(TabletCacheSweepPeriod))) {
        return;
    }

    decltype(ExpiredTabletIds_) expiredTabletIds;
    {
        auto gcGuard = Guard(GCLock_);
        expiredTabletIds = std::move(ExpiredTabletIds_);
    }

    if (!expiredTabletIds.empty()) {
        YT_LOG_DEBUG("Start sweeping expired tablet info (ExpiredTabletCount: %v)",
            expiredTabletIds.size());

        for (auto id : expiredTabletIds) {
            auto guard = WriterGuard(MapLock_);
            if (auto it = Map_.find(id); it) {
                if  (it->second.IsExpired()) {
                    Map_.erase(it);
                    continue;
                }

                guard.Release();

                auto gcGuard = Guard(GCLock_);
                GCQueue_.push(id);
            }
        }

        YT_LOG_DEBUG("Finish sweeping expired tablet info");
    }
}

void TTabletInfoCache::ProcessNextGCQueueEntry()
{
    VERIFY_SPINLOCK_AFFINITY(MapLock_);
    auto gcGuard = Guard(GCLock_);
    if (!GCQueue_.empty()) {
        const auto& id = GCQueue_.front();
        if (auto it = Map_.find(id); it) {
            if (it->second.IsExpired()) {
                ExpiredTabletIds_.push_back(id);
            } else {
                GCQueue_.push(id);
            }
        }
        GCQueue_.pop();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableMountCacheBase::TTableMountCacheBase(
    TTableMountCacheConfigPtr config,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(
        config,
        logger.WithTag("Cache: TableMount"),
        profiler)
    , Logger(std::move(logger))
    , TabletInfoCache_(Logger)
    , Config_(std::move(config))
{ }

TFuture<TTableMountInfoPtr> TTableMountCacheBase::GetTableInfo(const NYPath::TYPath& path)
{
    auto [future, requestInitialized] = TAsyncExpiringCache::GetExtended(path);

    bool shouldThrow = false;
    if (!requestInitialized && !future.IsSet()) {
        auto guard = ReaderGuard(SpinLock_);
        shouldThrow = Config_->RejectIfEntryIsRequestedButNotReady;
    }
    if (shouldThrow) {
        // COMPAT(babenko): replace with TransientFailure error code.
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable,
            "Mount info is unavailable, please try again")
            << TError(NTabletClient::EErrorCode::TableMountInfoNotReady,
                "Table mount info is not ready, but has already been requested")
                << TErrorAttribute("path", path);
    }

    return future;
}

TTabletInfoPtr TTableMountCacheBase::FindTabletInfo(TTabletId tabletId)
{
    return TabletInfoCache_.Find(tabletId);
}

void TTableMountCacheBase::InvalidateTablet(TTabletInfoPtr tabletInfo)
{
    for (const auto& weakOwner : tabletInfo->Owners) {
        if (auto owner = weakOwner.Lock()) {
            InvalidateTable(owner);
        }
    }
}

std::pair<std::optional<TErrorCode>, TTabletInfoPtr> TTableMountCacheBase::InvalidateOnError(
    const TError& error,
    bool forceRetry)
{
    static const std::vector<TErrorCode> retriableCodes = {
        NTabletClient::EErrorCode::NoSuchTablet,
        NTabletClient::EErrorCode::TabletNotMounted,
        NTabletClient::EErrorCode::InvalidMountRevision,
        NYTree::EErrorCode::ResolveError
    };

    if (!error.IsOK()) {
        for (auto code : retriableCodes) {
            if (auto retriableError = error.FindMatching(code)) {
                auto tabletId = retriableError->Attributes().Find<TTabletId>("tablet_id");
                if (!tabletId) {
                    continue;
                }

                auto isTabletUnmounted = retriableError->Attributes().Get<bool>("is_tablet_unmounted", false);
                auto tabletInfo = FindTabletInfo(*tabletId);
                if (tabletInfo) {
                    YT_LOG_DEBUG(error,
                        "Invalidating tablet in table mount cache "
                        "(TabletId: %v, CellId: %v, MountRevision: %x, IsTabletUnmounted: %v, Owners: %v)",
                        tabletInfo->TabletId,
                        tabletInfo->CellId,
                        tabletInfo->MountRevision,
                        isTabletUnmounted,
                        MakeFormattableView(tabletInfo->Owners, [] (auto* builder, const auto& weakOwner) {
                            if (auto owner = weakOwner.Lock()) {
                                FormatValue(builder, owner->Path, TStringBuf());
                            } else {
                                builder->AppendString(TStringBuf("<expired>"));
                            }
                        }));

                    InvalidateTablet(tabletInfo);
                }

                std::optional<TErrorCode> retriableErrorCode = code;
                if (code == NTabletClient::EErrorCode::TabletNotMounted &&
                    isTabletUnmounted &&
                    !forceRetry)
                {
                    retriableErrorCode = std::nullopt;
                }

                return std::pair(retriableErrorCode, tabletInfo);
            }
        }
    }

    return std::pair(std::nullopt, nullptr);
}

void TTableMountCacheBase::Clear()
{
    TAsyncExpiringCache::Clear();
    TabletInfoCache_.Clear();
    YT_LOG_DEBUG("Table mount info cache cleared");
}

void TTableMountCacheBase::Reconfigure(TTableMountCacheConfigPtr config)
{
    TAsyncExpiringCache::Reconfigure(config);
    {
        auto guard = WriterGuard(SpinLock_);
        Config_ = config;
    }
    YT_LOG_DEBUG("Table mount info cache reconfigured (NewConfig: %v)",
        NYson::ConvertToYsonString(config, NYson::EYsonFormat::Text).AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
