#include "table_mount_cache_detail.h"

#include "config.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/misc/hash.h>

#include <library/cpp/iterator/concatenate.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletClient {

using namespace NConcurrency;
using namespace NHydra;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto TabletCacheSweepPeriod = TDuration::Seconds(60);

////////////////////////////////////////////////////////////////////////////////

TTabletInfoOwnerCache::TTabletInfoOwnerCache(NLogging::TLogger logger)
    : Logger(std::move(logger))
{ }

void TTabletInfoOwnerCache::DropExpiredOwners(std::vector<TWeakPtr<TTableMountInfo>>* owners)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(MapLock_);

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
    YT_ASSERT_SPINLOCK_AFFINITY(MapLock_);

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
        NRpc::TDispatcher::Get()->GetHeavyInvoker(),
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

void TTableMountCacheBase::SetTableInfos(std::vector<TTableMountInfoPtr> clonedTableInfos)
{
    for (const auto& tableInfo : clonedTableInfos) {
        for (const auto& tabletInfo : tableInfo->Tablets) {
            TabletInfoOwnerCache_.Insert(tabletInfo->TabletId, MakeWeak(tableInfo));
        }

        TAsyncExpiringCache::Set(tableInfo->Path, tableInfo);
    }
}

auto TTableMountCacheBase::TryHandleRedirectionError(const TError& error)
    -> std::optional<TInvalidationResult>
{
    auto expectedError = error.FindMatching({
        NTabletClient::EErrorCode::TabletServantIsNotActive,
        NTabletClient::EErrorCode::TabletResharded,
    });

    if (!expectedError) {
        return {};
    }

    const auto& attributes = expectedError->Attributes();
    auto tabletId = attributes.Find<TTabletId>("tablet_id");
    if (!tabletId) {
        return {};
    }

    auto tabletInfo = FindTabletInfo(*tabletId);
    if (!tabletInfo) {
        return {};
    }

    auto redirectionHint = attributes.Find<TTabletRedirectionHint>("redirection_hint");
    if (!redirectionHint) {
        return {};
    }

    switch (static_cast<NTabletClient::EErrorCode>(expectedError->GetCode())) {
        case NTabletClient::EErrorCode::TabletResharded:
            return TryHandleTabletReshardedError(redirectionHint->ReshardRedirectionHint, tabletInfo);

        case NTabletClient::EErrorCode::TabletServantIsNotActive:
            return TryHandleServantNotActiveError(redirectionHint->SmoothMovementRedirectionHint, tabletInfo);

        default:
            return {};
    }
}

auto TTableMountCacheBase::TryHandleServantNotActiveError(
    const TSmoothMovementRedirectionHint& smoothMovementHint,
    const TTabletInfoPtr& tabletInfo)
    -> std::optional<TInvalidationResult>
{
    if (!smoothMovementHint.NewMountRevision ||
        !smoothMovementHint.OldMountRevision ||
        !smoothMovementHint.CellId ||
        !smoothMovementHint.CellDescriptor)
    {
        return {};
    }

    if (tabletInfo->MountRevision != smoothMovementHint.OldMountRevision) {
        return {};
    }

    RegisterCell(std::move(smoothMovementHint.CellDescriptor));

    auto newTabletInfo = tabletInfo->Clone();
    newTabletInfo->CellId = smoothMovementHint.CellId;
    newTabletInfo->MountRevision = smoothMovementHint.NewMountRevision;

    auto owners = TabletInfoOwnerCache_.GetOwners(tabletInfo->TableId);

    YT_LOG_DEBUG("Switching tablet servant in table mount cache "
        "(TabletId: %v, PreviousCellId: %v, PreviousMountRevision: %x, "
        "NewCellId: %v, NewMountRevision: %x, Owners: %v)",
        tabletInfo->TableId,
        tabletInfo->CellId,
        tabletInfo->MountRevision,
        smoothMovementHint.CellId,
        smoothMovementHint.NewMountRevision,
        MakeFormattableView(owners, [] (auto* builder, const auto& weakOwner) {
            if (auto owner = weakOwner.Lock()) {
                builder->AppendString(owner->Path);
            }
        }));

    std::vector<TTableMountInfoPtr> clonedTableInfos;

    for (auto weakOwner : TabletInfoOwnerCache_.GetOwners(tabletInfo->TabletId)) {
        auto owner = weakOwner.Lock();
        if (!owner) {
            continue;
        }

        auto clone = owner->Clone();

        for (auto& tableTabletInfo : Concatenate(clone->Tablets, clone->MountedTablets)) {
            if (tableTabletInfo->TabletId == tabletInfo->TabletId &&
                tableTabletInfo->MountRevision == tabletInfo->MountRevision)
            {
                tableTabletInfo = newTabletInfo;
            }
        }

        clonedTableInfos.push_back(std::move(clone));
    }

    SetTableInfos(std::move(clonedTableInfos));

    return {{
        .Retryable = true,
        .ErrorCode = NTabletClient::EErrorCode::TabletServantIsNotActive,
        .TabletInfo = newTabletInfo,
        .TableInfoUpdatedFromError = true,
    }};
}

auto TTableMountCacheBase::TryHandleTabletReshardedError(
    const TReshardRedirectionHintPtr& reshardHint,
    const TTabletInfoPtr& tabletInfo)
    -> std::optional<TInvalidationResult>
{
    if (!reshardHint) {
        return {};
    }

    auto owners = TabletInfoOwnerCache_.GetOwners(tabletInfo->TabletId);

    const auto& oldTabletIds = reshardHint->OldTabletIds;
    const auto& oldTabletMountRevisions = reshardHint->OldTabletMountRevisions;
    const auto& newTabletIds = reshardHint->NewTabletIds;
    const auto& newTabletPivotKeys = reshardHint->NewTabletPivotKeys;
    const auto& newTabletsMountRevision = reshardHint->NewTabletsMountRevision;

    YT_VERIFY(oldTabletIds.size() == oldTabletMountRevisions.size());
    YT_VERIFY(newTabletIds.size() == newTabletPivotKeys.size());
    if (oldTabletIds.empty() ||
        newTabletIds.empty() ||
        !newTabletsMountRevision)
    {
        return {};
    }

    YT_LOG_DEBUG("Updating info of tablets in table mount cache after reshard "
        "(OldTabletIds: %v, OldTabletMountRevisions: %llx, CellId: %v, "
        "NewTabletIds: %v, NewTabletsMountRevision: %llx, Owners: %v)",
        oldTabletIds,
        oldTabletMountRevisions,
        tabletInfo->CellId,
        newTabletIds,
        newTabletsMountRevision,
        MakeFormattableView(owners, [] (auto* builder, const auto& weakOwner) {
            if (auto owner = weakOwner.Lock()) {
                builder->AppendString(owner->Path);
            }
        }));

    THashSet<TTabletId> ReshardedTabletIds(oldTabletIds.begin(), oldTabletIds.end());

    std::vector<TTabletInfoPtr> newTabletInfos;
    std::vector<TTableMountInfoPtr> clonedTableInfos;
    for (auto weakOwner : owners) {
        auto owner = weakOwner.Lock();
        if (!owner) {
            continue;
        }

        int relativeOldTabletIndex = 0;
        int firstTabletInfoOffset = 0;

        for (auto tabletInfoIt = owner->Tablets.begin(); tabletInfoIt != owner->Tablets.end(); ++tabletInfoIt) {
            if (relativeOldTabletIndex < std::ssize(oldTabletIds) &&
                oldTabletIds[relativeOldTabletIndex] == (*tabletInfoIt)->TabletId &&
                oldTabletMountRevisions[relativeOldTabletIndex] == (*tabletInfoIt)->MountRevision)
            {
                ++relativeOldTabletIndex;
                if (relativeOldTabletIndex == 1) {
                    firstTabletInfoOffset = std::distance(owner->Tablets.begin(), tabletInfoIt);
                }
            } else if (relativeOldTabletIndex > 0) {
                break;
            }
        }

        if (relativeOldTabletIndex != std::ssize(oldTabletIds)) {
            continue;
        }

        auto clone = owner->Clone();
        auto firstTabletInfoIt = clone->Tablets.begin() + firstTabletInfoOffset;

        if (newTabletInfos.empty()) {
            newTabletInfos.reserve(std::ssize(newTabletIds));

            for (const auto& [tabletId, pivotKey] : Zip(newTabletIds, newTabletPivotKeys)) {
                auto newTabletInfo = New<TTabletInfo>();
                newTabletInfo->TabletId = tabletId;
                newTabletInfo->MountRevision = newTabletsMountRevision;
                // Typically, tablets have the same state.
                newTabletInfo->State = tabletInfo->State;
                newTabletInfo->InMemoryMode = tabletInfo->InMemoryMode;
                newTabletInfo->PivotKey = pivotKey;
                newTabletInfo->CellId = tabletInfo->CellId;
                newTabletInfo->TableId = tabletInfo->TableId;
                newTabletInfo->UpdateTime = Now();

                newTabletInfos.push_back(newTabletInfo);
            }
        }

        clone->Tablets.erase(
            firstTabletInfoIt,
            firstTabletInfoIt + std::ssize(oldTabletIds));

        clone->Tablets.insert(
            firstTabletInfoIt,
            newTabletInfos.begin(),
            newTabletInfos.end());

        auto endIt = std::remove_if(
            clone->MountedTablets.begin(),
            clone->MountedTablets.end(),
            [&] (const TTabletInfoPtr& tabletInfo) {
                return ReshardedTabletIds.contains(tabletInfo->TabletId);
            });
        bool allTabletsPresentInMountedTablets = clone->MountedTablets.end() - endIt == ssize(oldTabletIds);
        clone->MountedTablets.erase(endIt, clone->MountedTablets.end());

        if (allTabletsPresentInMountedTablets && tabletInfo->State == ETabletState::Mounted) {
            auto targetIt = std::find_if(
                clone->MountedTablets.begin(),
                clone->MountedTablets.end(),
                [&] (const TTabletInfoPtr& tabletInfo) {
                    return tabletInfo->PivotKey >= *newTabletPivotKeys.begin();
            });
            clone->MountedTablets.insert(
                targetIt,
                newTabletInfos.begin(),
                newTabletInfos.end());
        }

        clonedTableInfos.push_back(std::move(clone));
    }

    SetTableInfos(std::move(clonedTableInfos));

    return {{
        .Retryable = true,
        .ErrorCode = NTabletClient::EErrorCode::TabletResharded,
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
        NTabletClient::EErrorCode::TabletResharded,
        NYTree::EErrorCode::ResolveError
    };

    if (error.IsOK()) {
        return {};
    }

    if (auto result = TryHandleRedirectionError(error)) {
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
