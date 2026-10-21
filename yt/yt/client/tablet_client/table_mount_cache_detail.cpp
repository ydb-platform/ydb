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

const THashSet<TErrorCode> TableMountCacheRetryableCodes = {
    NTabletClient::EErrorCode::NoSuchTablet,
    NTabletClient::EErrorCode::TabletNotMounted,
    NTabletClient::EErrorCode::InvalidMountRevision,
    NTabletClient::EErrorCode::TabletServantIsNotActive,
    NTabletClient::EErrorCode::TabletResharded,
    NTabletClient::EErrorCode::TestingFailureBeforeWrite,
    NTabletClient::EErrorCode::ReadOnlySmoothMovementStage,
    NYTree::EErrorCode::ResolveError,
};

static constexpr auto TabletCacheSweepPeriod = TDuration::Seconds(60);

bool IsRetryableError(const TError& error)
{
    bool retryable = true;
    auto onError = [&] (const TError& error, auto&& self) -> void {
        if (TableMountCacheRetryableCodes.contains(error.GetCode())) {
            retryable &= error.Attributes().Get<bool>("retryable", true);
        }

        for (const auto& innerError : error.InnerErrors()) {
            self(innerError, self);
        }
    };

    onError(error, onError);
    return retryable;
}

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
        YT_TLOG_DEBUG("Start sweeping expired tablet info")
            .With("ExpiredTabletCount", expiredTabletIds.size());

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

        YT_TLOG_DEBUG("Finish sweeping expired tablet info");
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
        logger.WithTag("Cache", "TableMount"),
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
            .With(TError(NTabletClient::EErrorCode::TableMountInfoNotReady,
                "Table mount info is not ready, but has already been requested"))
            .With("path", path);
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

TTabletInfoPtr TTableMountCacheBase::FindTabletInfo(
    TTabletId tabletId,
    std::optional<TRevision> mountRevision)
{
    TTabletInfoPtr result;

    for (auto weakOwner : TabletInfoOwnerCache_.GetOwners(tabletId)) {
        auto owner = weakOwner.Lock();
        if (!owner) {
            continue;
        }

        for (const auto& tabletInfo : owner->Tablets) {
            if (tabletInfo->TabletId == tabletId &&
                (!mountRevision || tabletInfo->MountRevision == *mountRevision))
            {
                if (!result ||
                    tabletInfo->MountRevision > result->MountRevision ||
                    tabletInfo->MountRevision == result->MountRevision && tabletInfo->UpdateTime > result->UpdateTime)
                {
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
    static const THashSet<TErrorCode> handledCodes = {
        NTabletClient::EErrorCode::ReadOnlySmoothMovementStage,
        NTabletClient::EErrorCode::TabletServantIsNotActive,
        NTabletClient::EErrorCode::TabletResharded,
    };

    std::vector<std::pair<TSmoothMovementRedirectionHint, TTabletInfoPtr>> smoothMovementRedirectionHints;
    std::vector<std::pair<TReshardRedirectionHintPtr, TTabletInfoPtr>> reshardRedirectionHints;
    bool retryInplace = false;

    auto onError = [&] (const TError& error, auto&& self) {
        if (handledCodes.contains(error.GetCode())) {
            auto tabletId = error.Attributes().Find<TTabletId>("tablet_id");
            if (!tabletId) {
                return;
            }

            if (error.Attributes().Get<bool>("mount_cache_invalidation_exhausted", false)) {
                return;
            }

            auto tabletInfo = FindTabletInfo(*tabletId);
            if (!tabletInfo) {
                return;
            }

            if (error.GetCode() == NTabletClient::EErrorCode::ReadOnlySmoothMovementStage) {
                retryInplace = error.Attributes().Get<bool>("retry_inplace", retryInplace);
            }

            auto redirectionHint = error.Attributes().Find<TTabletRedirectionHint>("redirection_hint");
            if (!redirectionHint) {
                return;
            }

            if (error.GetCode() == NTabletClient::EErrorCode::TabletResharded) {
                reshardRedirectionHints.emplace_back(redirectionHint->ReshardRedirectionHint, tabletInfo);
            } else {
                smoothMovementRedirectionHints.emplace_back(redirectionHint->SmoothMovementRedirectionHint, tabletInfo);
            }
        } else {
            for (const auto& innerError : error.InnerErrors()) {
                self(innerError, self);
            }
        }
    };

    onError(error, onError);

    if (retryInplace) {
        YT_TLOG_ALERT_UNLESS(
            smoothMovementRedirectionHints.empty() && reshardRedirectionHints.empty(),
            "In-place retry is combined with tablet redirection hints within a single request; "
            "redirection hints are ignored in favor of in-place retry")
            .With("HasSmoothMovementRedirectionHints", !smoothMovementRedirectionHints.empty())
            .With("HasReshardRedirectionHints", !reshardRedirectionHints.empty())
            .With(error);

        return {{
            .Retryable = true,
            .ErrorCode = NTabletClient::EErrorCode::ReadOnlySmoothMovementStage,
            .TableInfoUpdatedFromError = true,
        }};
    }

    std::optional<TInvalidationResult> result;
    for (const auto& [reshardRedirectionHint, tabletInfo] : reshardRedirectionHints) {
        auto reshardResult = TryHandleTabletReshardedError(reshardRedirectionHint, tabletInfo);
        if (!reshardResult) {
            return {};
        }
        result = std::move(reshardResult);
    }

    if (!smoothMovementRedirectionHints.empty()) {
        auto smoothMovementResult = TryHandleServantNotActiveError(std::move(smoothMovementRedirectionHints));
        if (!smoothMovementResult) {
            return {};
        }
        result = std::move(smoothMovementResult);
    }

    return result;
}

auto TTableMountCacheBase::TryHandleServantNotActiveError(
    std::vector<std::pair<TSmoothMovementRedirectionHint, TTabletInfoPtr>> hints)
    -> std::optional<TInvalidationResult>
{
    YT_VERIFY(!hints.empty());

    // Validate all hints first. If any is invalid, bail out entirely.
    decltype(hints) filteredHints;
    for (auto&& [hint, tabletInfo] : hints) {
        if (!hint.NewMountRevision ||
            !hint.OldMountRevision ||
            !hint.CellId ||
            !hint.CellDescriptor)
        {
            return {};
        }

        if (tabletInfo->MountRevision == hint.OldMountRevision) {
            filteredHints.emplace_back(std::move(hint), std::move(tabletInfo));
        } else if (tabletInfo->MountRevision == hint.NewMountRevision) {
            // This tablet info is up-to-date, but other owners may still have stale tablet infos
            // that we want to update.
            auto oldTabletInfo = FindTabletInfo(tabletInfo->TabletId, hint.OldMountRevision);
            if (oldTabletInfo) {
                filteredHints.emplace_back(std::move(hint), std::move(oldTabletInfo));
            }
        } else {
            return {};
        }
    }

    if (filteredHints.empty()) {
        // Recent mount info already is up-to-date.
        return {{
            .Retryable = true,
            .ErrorCode = NTabletClient::EErrorCode::TabletServantIsNotActive,
            .TabletInfo = hints[0].second,
            .TableInfoUpdatedFromError = true,
        }};
    }

    hints = std::move(filteredHints);

    // Build a map from (TabletId, OldMountRevision) -> newTabletInfo for fast lookup.
    // Also register cells and log.
    struct TTabletReplacement {
        TTabletInfoPtr OldTabletInfo;
        TTabletInfoPtr NewTabletInfo;
    };
    THashMap<TTabletId, TTabletReplacement> replacements;
    replacements.reserve(hints.size());

    TTabletInfoPtr lastNewTabletInfo;

    for (auto& [smoothMovementHint, tabletInfo] : hints) {
        RegisterCell(smoothMovementHint.CellDescriptor);

        auto newTabletInfo = tabletInfo->Clone();
        newTabletInfo->CellId = smoothMovementHint.CellId;
        newTabletInfo->MountRevision = smoothMovementHint.NewMountRevision;
        // Logical mount revision is preserved during smooth movement.

        auto owners = TabletInfoOwnerCache_.GetOwners(tabletInfo->TabletId);

        YT_TLOG_DEBUG("Switching tablet servant in table mount cache")
            .With("TabletId", tabletInfo->TabletId)
            .With("PreviousCellId", tabletInfo->CellId)
            .WithFormat("PreviousMountRevision", "%x", tabletInfo->MountRevision)
            .WithFormat("LogicalMountRevision", "%x", tabletInfo->LogicalMountRevision)
            .With("NewCellId", smoothMovementHint.CellId)
            .WithFormat("NewMountRevision", "%x", smoothMovementHint.NewMountRevision)
            .With("Owners", MakeFormattableView(owners, [] (auto* builder, const auto& weakOwner) {
                if (auto owner = weakOwner.Lock()) {
                    builder->AppendString(owner->Path);
                } else {
                    builder->AppendString("<expired>");
                }
            }));

        lastNewTabletInfo = newTabletInfo;
        replacements.emplace(tabletInfo->TabletId, TTabletReplacement{
            .OldTabletInfo = tabletInfo,
            .NewTabletInfo = std::move(newTabletInfo),
        });
    }

    // Collect all unique owner tables that contain any of the affected tablets.
    THashSet<TYPath> ownerPaths;
    for (const auto& [tabletId, replacement] : replacements) {
        for (auto& weakOwner : TabletInfoOwnerCache_.GetOwners(tabletId)) {
            if (auto owner = weakOwner.Lock()) {
                ownerPaths.insert(owner->Path);
            }
        }
    }

    YT_TLOG_DEBUG("Switching tablet servants in table mount cache")
        .With("TabletIds", MakeFormattableView(replacements, [] (auto* builder, const auto& pair) {
            builder->AppendFormat("%v", pair.first);
        }))
        .With("Owners", ownerPaths);

    std::vector<TTableMountInfoPtr> clonedTableInfos;

    for (const auto& path : ownerPaths) {
        auto errorOrOwner = Find(path);
        if (!errorOrOwner || !errorOrOwner->IsOK()) {
            continue;
        }

        auto clone = errorOrOwner->Value()->Clone();

        bool replaced = false;

        for (auto& tableTabletInfo : Concatenate(clone->Tablets, clone->MountedTablets)) {
            if (auto it = replacements.find(tableTabletInfo->TabletId); it != replacements.end()) {
                const auto& replacement = it->second;
                if (tableTabletInfo->MountRevision == replacement.OldTabletInfo->MountRevision) {
                    tableTabletInfo = replacement.NewTabletInfo;
                    replaced = true;
                }
            }
        }

        if (replaced) {
            clonedTableInfos.push_back(std::move(clone));
        }
    }

    SetTableInfos(std::move(clonedTableInfos));

    return {{
        .Retryable = true,
        .ErrorCode = NTabletClient::EErrorCode::TabletServantIsNotActive,
        .TabletInfo = lastNewTabletInfo,
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

    THashSet<TYPath> ownerPaths;
    for (const auto& weakOwner : TabletInfoOwnerCache_.GetOwners(tabletInfo->TabletId)) {
        if (auto owner = weakOwner.Lock()) {
            ownerPaths.insert(owner->Path);
        }
    }

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

    YT_TLOG_DEBUG("Updating info of tablets in table mount cache after reshard")
        .With("OldTabletIds", oldTabletIds)
        .WithFormat("OldTabletMountRevisions", "%llx", oldTabletMountRevisions)
        .With("CellId", tabletInfo->CellId)
        .With("NewTabletIds", newTabletIds)
        .WithFormat("NewTabletsMountRevision", "%llx", newTabletsMountRevision)
        .With("Owners", ownerPaths);

    THashSet<TTabletId> reshardedTabletIds(oldTabletIds.begin(), oldTabletIds.end());

    std::vector<TTabletInfoPtr> newTabletInfos;
    std::vector<TTableMountInfoPtr> clonedTableInfos;
    for (const auto& path : ownerPaths) {
        auto errorOrOwner = Find(path);
        if (!errorOrOwner || !errorOrOwner->IsOK()) {
            continue;
        }
        auto owner = errorOrOwner->Value();

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
                newTabletInfo->LogicalMountRevision = newTabletsMountRevision;
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
                return reshardedTabletIds.contains(tabletInfo->TabletId);
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

auto TTableMountCacheBase::InvalidateOnError(const TError& error, bool forceRetry, TTabletId tabletIdHint)
    -> TInvalidationResult
{
    static const THashSet<TErrorCode> retryableCodesWithoutTabletId = {
        NRpc::EErrorCode::NoSuchRealm,
        NRpc::EErrorCode::NoSuchService,
    };

    if (error.IsOK()) {
        return {};
    }

    bool retryable = IsRetryableError(error);
    if (auto result = TryHandleRedirectionError(error)) {
        result->Retryable &= retryable;
        return *result;
    }

    auto errorFilter = [&] (const TError& error) {
        bool isRetryableCode = TableMountCacheRetryableCodes.contains(error.GetCode()) ||
            retryableCodesWithoutTabletId.contains(error.GetCode());
        return isRetryableCode && !error.Attributes().Get<bool>("mount_cache_invalidation_exhausted", false);
    };

    if (auto retryableError = error.FindMatching(errorFilter)) {
        auto code = retryableError->GetCode();

        auto tabletId = retryableError->Attributes().Find<TTabletId>("tablet_id");
        if (!tabletId && retryableCodesWithoutTabletId.contains(code)) {
            tabletId = tabletIdHint;
        }

        if (!tabletId) {
            return {};
        }

        auto isTabletUnmounted = retryableError->Attributes().Get<bool>("is_tablet_unmounted", false);
        auto tabletInfo = FindTabletInfo(*tabletId);
        if (tabletInfo) {
            YT_TLOG_DEBUG("Invalidating tablet in table mount cache")
                .With("TabletId", tabletInfo->TabletId)
                .With("CellId", tabletInfo->CellId)
                .WithFormat("MountRevision", "%x", tabletInfo->MountRevision)
                .WithFormat("LogicalMountRevision", "%x", tabletInfo->LogicalMountRevision)
                .With("IsTabletUnmounted", isTabletUnmounted)
                .With("Owners", MakeFormattableView(TabletInfoOwnerCache_.GetOwners(*tabletId), [] (auto* builder, const auto& weakOwner) {
                    if (auto owner = weakOwner.Lock()) {
                        FormatValue(builder, owner->Path, TStringBuf());
                    } else {
                        builder->AppendString(TStringBuf("<expired>"));
                    }
                }))
                .With(error);

            InvalidateTablet(*tabletId);
        }

        if (code == NTabletClient::EErrorCode::TabletNotMounted &&
            isTabletUnmounted &&
            !forceRetry)
        {
            return {};
        }

        return {
            .Retryable = retryable,
            .ErrorCode = code,
            .TabletInfo = tabletInfo,
            .TableInfoUpdatedFromError = false,
        };
    }

    return {};
}

void TTableMountCacheBase::RegisterCell(INodePtr /*cellDescriptor*/)
{ }

void TTableMountCacheBase::Clear()
{
    TAsyncExpiringCache::Clear();
    TabletInfoOwnerCache_.Clear();
    YT_TLOG_DEBUG("Table mount info cache cleared");
}

void TTableMountCacheBase::Reconfigure(TTableMountCacheConfigPtr config)
{
    TAsyncExpiringCache::Reconfigure(config);
    {
        auto guard = WriterGuard(SpinLock_);
        Config_ = config;
    }
    YT_TLOG_DEBUG("Table mount info cache reconfigured")
        .With("NewConfig", NYson::ConvertToYsonString(config, NYson::EYsonFormat::Text).AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
