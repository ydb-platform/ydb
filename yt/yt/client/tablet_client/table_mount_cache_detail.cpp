#include "table_mount_cache_detail.h"

#include "config.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NTabletClient {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto TabletCacheSweepPeriod = TDuration::Seconds(60);

////////////////////////////////////////////////////////////////////////////////

TTabletInfoOwnerCache::TTabletInfoOwnerCache(NLogging::TLogger logger)
    : Logger(std::move(logger))
{ }

void TTabletInfoOwnerCache::DropExpiredOwners(std::vector<TWeakPtr<TTableMountInfo>>* owners)
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(MapLock_);

    std::erase_if(*owners, [] (const auto& owner) {
        return owner.IsExpired();
    });
}

void TTabletInfoOwnerCache::Insert(TTabletId tabletId, TWeakPtr<TTableMountInfo> tableInfo)
{
    SweepExpiredEntries();

    auto guard = WriterGuard(MapLock_);
    ProcessNextGCQueueEntry();

    typename decltype(Map_)::insert_ctx context;
    auto it = Map_.find(tabletId, context);

    if (it == Map_.end()) {
        Map_.emplace_direct(context, tabletId, std::vector{std::move(tableInfo)});
        guard.Release();

        auto gcGuard = Guard(GCLock_);
        GCQueue_.push(tabletId);
    } else {
        DropExpiredOwners(&it->second);
        it->second.push_back(std::move(tableInfo));
    }
}

std::vector<TWeakPtr<TTableMountInfo>> TTabletInfoOwnerCache::GetOwners(TTabletId tabletId)
{
    SweepExpiredEntries();

    auto guard = ReaderGuard(MapLock_);
    ProcessNextGCQueueEntry();

    if (auto it = Map_.find(tabletId); it != Map_.end()) {
        return it->second;
    }

    return {};
}

void TTabletInfoOwnerCache::Clear()
{
    {
        decltype(Map_) other;

        auto guard = WriterGuard(MapLock_);
        other = std::move(Map_);

        // Release guard to avoid destruction under lock.
        guard.Release();
    }

    {
        decltype(GCQueue_) otherQueue;
        decltype(ExpiredTabletIds_) otherTabletIds;

        auto guard = Guard(GCLock_);
        otherQueue = std::move(GCQueue_);
        otherTabletIds = std::move(ExpiredTabletIds_);

        // Release guard to avoid destruction under lock.
        guard.Release();
    }
}

void TTabletInfoOwnerCache::SweepExpiredEntries()
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
                DropExpiredOwners(&it->second);
                if (it->second.empty()) {
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

void TTabletInfoOwnerCache::ProcessNextGCQueueEntry()
{
    VERIFY_SPINLOCK_AFFINITY(MapLock_);

    auto gcGuard = Guard(GCLock_);
    if (!GCQueue_.empty()) {
        auto id = GCQueue_.front();
        if (auto it = Map_.find(id); it) {
            bool allExpired = true;
            for (const auto& weak : it->second) {
                if (!weak.IsExpired()) {
                    allExpired = false;
                    break;
                }
            }
            if (allExpired) {
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
    , TabletInfoOwnerCache_(Logger)
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

void TTableMountCacheBase::InvalidateTablet(TTabletId tabletId)
{
    for (const auto& weakOwner : TabletInfoOwnerCache_.GetOwners(tabletId)) {
        if (auto owner = weakOwner.Lock()) {
            InvalidateTable(owner);
        }
    }
}

TTabletInfoPtr TTableMountCacheBase::FindTabletInfo(TTabletId tabletId)
{
    TTabletInfoPtr result;

    for (auto weakOwner : TabletInfoOwnerCache_.GetOwners(tabletId)) {
        auto owner = weakOwner.Lock();
        if (!owner) {
            continue;
        }

        for (const auto& tabletInfo : owner->Tablets) {
            if (tabletInfo->TabletId == tabletId) {
                if (!result || tabletInfo->MountRevision > result->MountRevision) {
                    result = tabletInfo;
                    break;
                }
            }
        }
    }

    return result;
}

auto TTableMountCacheBase::TryHandleServantNotActiveError(const TError& error)
    -> std::optional<TInvalidationResult>
{
    auto servantNotActiveError = error.FindMatching(NTabletClient::EErrorCode::TabletServantIsNotActive);
    if (!servantNotActiveError) {
        return {};
    }

    const auto& attributes = servantNotActiveError->Attributes();
    auto tabletId = attributes.Find<TTabletId>("tablet_id");
    if (!tabletId) {
        return {};
    }

    auto tabletInfo = FindTabletInfo(*tabletId);
    if (!tabletInfo) {
        return {};
    }

    auto siblingCellId = attributes.Find<NObjectClient::TCellId>("sibling_servant_cell_id");
    auto siblingMountRevision = attributes.Find<NHydra::TRevision>("sibling_servant_mount_revision");

    if (!siblingCellId || !siblingMountRevision) {
        return {};
    }

    if (auto siblingCellDescriptor = attributes.ToMap()->FindChild("sibling_servant_cell_descriptor")) {
        RegisterCell(std::move(siblingCellDescriptor));
    } else {
        return {};
    }

    auto newTabletInfo = tabletInfo->Clone();
    newTabletInfo->CellId = *siblingCellId;
    newTabletInfo->MountRevision = *siblingMountRevision;

    auto owners = TabletInfoOwnerCache_.GetOwners(*tabletId);

    YT_LOG_DEBUG("Switching tablet servant in table mount cache "
        "(TabletId: %v, PreviousCellId: %v, PreviousMountRevision: %x, "
        "NewCellId: %v, NewMountRevision: %x, Owners: %v)",
        tabletId,
        tabletInfo->CellId,
        tabletInfo->MountRevision,
        siblingCellId,
        siblingMountRevision,
        MakeFormattableView(owners, [] (auto* builder, const auto& weakOwner) {
            if (auto owner = weakOwner.Lock()) {
                builder->AppendString(owner->Path);
            }
        }));

    std::vector<TTableMountInfoPtr> clonedTableInfos;

    for (auto weakOwner : TabletInfoOwnerCache_.GetOwners(*tabletId)) {
        auto owner = weakOwner.Lock();
        if (!owner) {
            continue;
        }

        auto clone = owner->Clone();

        for (auto& tableTabletInfo : clone->Tablets) {
            if (tableTabletInfo->TabletId == tabletInfo->TabletId) {
                tableTabletInfo = newTabletInfo;
            }
        }

        for (auto& tableTabletInfo : clone->MountedTablets) {
            if (tableTabletInfo->TabletId == tabletInfo->TabletId) {
                tableTabletInfo = newTabletInfo;
            }
        }

        TabletInfoOwnerCache_.Insert(*tabletId, MakeWeak(clone));
        clonedTableInfos.push_back(std::move(clone));
    }

    for (const auto& tableInfo : clonedTableInfos) {
        TAsyncExpiringCache::Set(tableInfo->Path, tableInfo);
    }

    return {{
        .Retryable = true,
        .ErrorCode = NTabletClient::EErrorCode::TabletServantIsNotActive,
        .TabletInfo = newTabletInfo,
        .TableInfoUpdatedFromError = true,
    }};
}

auto TTableMountCacheBase::InvalidateOnError(const TError& error, bool forceRetry)
    -> TInvalidationResult
{
    static const std::vector<TErrorCode> retryableCodes = {
        NTabletClient::EErrorCode::NoSuchTablet,
        NTabletClient::EErrorCode::TabletNotMounted,
        NTabletClient::EErrorCode::InvalidMountRevision,
        NTabletClient::EErrorCode::TabletServantIsNotActive,
        NYTree::EErrorCode::ResolveError
    };

    if (error.IsOK()) {
        return {};
    }

    if (auto result = TryHandleServantNotActiveError(error)) {
        return *result;
    }

    for (auto code : retryableCodes) {
        if (auto retryableError = error.FindMatching(code)) {
            auto tabletId = retryableError->Attributes().Find<TTabletId>("tablet_id");
            if (!tabletId) {
                continue;
            }

            auto isTabletUnmounted = retryableError->Attributes().Get<bool>("is_tablet_unmounted", false);
            auto tabletInfo = FindTabletInfo(*tabletId);
            if (tabletInfo) {
                YT_LOG_DEBUG(error,
                    "Invalidating tablet in table mount cache "
                    "(TabletId: %v, CellId: %v, MountRevision: %x, IsTabletUnmounted: %v, Owners: %v)",
                    tabletInfo->TabletId,
                    tabletInfo->CellId,
                    tabletInfo->MountRevision,
                    isTabletUnmounted,
                    MakeFormattableView(TabletInfoOwnerCache_.GetOwners(*tabletId), [] (auto* builder, const auto& weakOwner) {
                        if (auto owner = weakOwner.Lock()) {
                            FormatValue(builder, owner->Path, TStringBuf());
                        } else {
                            builder->AppendString(TStringBuf("<expired>"));
                        }
                    }));

                InvalidateTablet(*tabletId);
            }

            if (code == NTabletClient::EErrorCode::TabletNotMounted &&
                isTabletUnmounted &&
                !forceRetry)
            {
                return {};
            }

            return {
                .Retryable = true,
                .ErrorCode = code,
                .TabletInfo = tabletInfo,
                .TableInfoUpdatedFromError = false,
            };
        }
    }

    return {};
}

void TTableMountCacheBase::RegisterCell(INodePtr /*cellDescriptor*/)
{ }

void TTableMountCacheBase::Clear()
{
    TAsyncExpiringCache::Clear();
    TabletInfoOwnerCache_.Clear();
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
