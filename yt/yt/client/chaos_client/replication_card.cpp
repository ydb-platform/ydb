#include "replication_card.h"

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/public.h>

#include <util/digest/multi.h>

#include <algorithm>

namespace NYT::NChaosClient {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

namespace NDetail {

void FormatProgressWithProjection(
    TStringBuilderBase* builder,
    const TReplicationProgress& replicationProgress,
    TReplicationProgressProjection replicationProgressProjection)
{
    const auto& segments = replicationProgress.Segments;
    if (segments.empty()) {
        builder->AppendString("[]");
        return;
    }

    auto it = std::upper_bound(
    segments.begin(),
    segments.end(),
    replicationProgressProjection.From,
    [] (const auto& lhs, const auto& rhs) {
        return CompareRows(lhs, rhs.LowerKey) <= 0;
    });

    bool comma = false;
    builder->AppendChar('[');

    if (it != segments.begin()) {
        builder->AppendFormat("<%v, %x>", segments[0].LowerKey, segments[0].Timestamp);
        if (it != std::next(segments.begin())) {
            builder->AppendString(", ...");
        }
        comma = true;
    }

    for (; it != segments.end() && it->LowerKey <= replicationProgressProjection.To; ++it) {
        builder->AppendFormat("%v<%v, %x>", comma ? ", " : "", it->LowerKey,  it->Timestamp);
        comma = true;
    }

    if (it != segments.end()) {
        builder->AppendString(", ...");
    }

    builder->AppendChar(']');
}

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EReplicationCardOptionsBits, ui8,
    ((None)(0))
    ((IncludeCoordinators)(1 << 0))
    ((IncludeProgress)(1 << 1))
    ((IncludeHistory)(1 << 2))
    ((IncludeReplicatedTableOptions)(1 << 3))
);

EReplicationCardOptionsBits ToBitMask(const TReplicationCardFetchOptions& options)
{
    auto mask = EReplicationCardOptionsBits::None;
    if (options.IncludeCoordinators) {
        mask |= EReplicationCardOptionsBits::IncludeCoordinators;
    }

    if (options.IncludeProgress) {
        mask |= EReplicationCardOptionsBits::IncludeProgress;
    }

    if (options.IncludeHistory) {
        mask |= EReplicationCardOptionsBits::IncludeHistory;
    }

    if (options.IncludeReplicatedTableOptions) {
        mask |= EReplicationCardOptionsBits::IncludeReplicatedTableOptions;
    }

    return mask;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TReplicationCardFetchOptions::operator size_t() const
{
    return MultiHash(
        IncludeCoordinators,
        IncludeProgress,
        IncludeHistory);
}

void FormatValue(TStringBuilderBase* builder, const TReplicationCardFetchOptions& options, TStringBuf /*spec*/)
{
    builder->AppendFormat("{IncludeCoordinators: %v, IncludeProgress: %v, IncludeHistory: %v}",
        options.IncludeCoordinators,
        options.IncludeProgress,
        options.IncludeHistory);
}

bool TReplicationCardFetchOptions::Contains(const TReplicationCardFetchOptions& other) const
{
    auto selfMask = NDetail::ToBitMask(*this);
    return (selfMask | NDetail::ToBitMask(other)) == selfMask;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationProgress& replicationProgress,
    TStringBuf /*spec*/,
    std::optional<TReplicationProgressProjection> replicationProgressProjection)
{
    builder->AppendString("{Segments: ");
    const auto& segments = replicationProgress.Segments;
    if (!replicationProgressProjection) {
        builder->AppendFormat("%v", MakeFormattableView(segments, [] (auto* builder, const auto& segment) {
            builder->AppendFormat("<%v, %x>", segment.LowerKey, segment.Timestamp);
        }));
    } else  {
        NDetail::FormatProgressWithProjection(builder, replicationProgress, *replicationProgressProjection);
    }

    builder->AppendFormat(", UpperKey: %v}", replicationProgress.UpperKey);

}

void FormatValue(TStringBuilderBase* builder, const TReplicaHistoryItem& replicaHistoryItem, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Era: %v, Timestamp: %v, Mode: %v, State: %v}",
        replicaHistoryItem.Era,
        replicaHistoryItem.Timestamp,
        replicaHistoryItem.Mode,
        replicaHistoryItem.State);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicaInfo& replicaInfo,
    TStringBuf /*spec*/,
    std::optional<TReplicationProgressProjection> replicationProgressProjection)
{
    builder->AppendFormat("{ClusterName: %v, ReplicaPath: %v, ContentType: %v, Mode: %v, State: %v, Progress: ",
        replicaInfo.ClusterName,
        replicaInfo.ReplicaPath,
        replicaInfo.ContentType,
        replicaInfo.Mode,
        replicaInfo.State);

    FormatValue(builder, replicaInfo.ReplicationProgress, TStringBuf(), replicationProgressProjection);

    builder->AppendFormat(", History: %v}", replicaInfo.History);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationCard& replicationCard,
    TStringBuf /*spec*/,
    std::optional<TReplicationProgressProjection> replicationProgressProjection)
{
    builder->AppendFormat("{Era: %v, Replicas: %v, CoordinatorCellIds: %v, TableId: %v, TablePath: %v, TableClusterName: %v, CurrentTimestamp: %v, CollocationId: %v}",
        replicationCard.Era,
        MakeFormattableView(
            replicationCard.Replicas,
            [&] (TStringBuilderBase* builder, std::pair<const NYT::TGuid, NYT::NChaosClient::TReplicaInfo> replica) {
                FormatValue(builder, replica.first, TStringBuf());
                builder->AppendString(": ");
                FormatValue(builder, replica.second, TStringBuf(), replicationProgressProjection);
            }),
        replicationCard.CoordinatorCellIds,
        replicationCard.TableId,
        replicationCard.TablePath,
        replicationCard.TableClusterName,
        replicationCard.CurrentTimestamp,
        replicationCard.ReplicationCardCollocationId);
}

TString ToString(
    const TReplicationCard& replicationCard,
    std::optional<TReplicationProgressProjection> replicationProgressProjection)
{
    TStringBuilder builder;
    FormatValue(&builder, replicationCard, {}, replicationProgressProjection);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationProgress::TSegment::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, LowerKey);
    Persist(context, Timestamp);
}

void TReplicationProgress::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Segments);
    Persist(context, UpperKey);
}

void TReplicaHistoryItem::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Era);
    Persist(context, Timestamp);
    Persist(context, Mode);
    Persist(context, State);
}

bool TReplicaHistoryItem::IsSync() const
{
    return Mode == ETableReplicaMode::Sync && State == ETableReplicaState::Enabled;
}

////////////////////////////////////////////////////////////////////////////////

int TReplicaInfo::FindHistoryItemIndex(TTimestamp timestamp) const
{
    auto it = std::upper_bound(
        History.begin(),
        History.end(),
        timestamp,
        [] (TTimestamp lhs, const TReplicaHistoryItem& rhs) {
            return lhs < rhs.Timestamp;
        });
    return std::distance(History.begin(), it) - 1;
}

TReplicaInfo* TReplicationCard::FindReplica(TReplicaId replicaId)
{
    auto it = Replicas.find(replicaId);
    return it == Replicas.end() ? nullptr : &it->second;
}

TReplicaInfo* TReplicationCard::GetReplicaOrThrow(TReplicaId replicaId, TReplicationCardId replicationCardId)
{
    auto* replicaInfo = FindReplica(replicaId);
    if (!replicaInfo) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "Replication card %v does not contain replica %v",
            replicationCardId,
            replicaId);
    }
    return replicaInfo;
}

////////////////////////////////////////////////////////////////////////////////

bool IsReplicaSync(ETableReplicaMode mode, const std::vector<TReplicaHistoryItem>& replicaHistory)
{
    // Check actual replica state to avoid merging transition states (e.g. AsyncToSync -> SyncToAsync)
    if (mode == ETableReplicaMode::Sync) {
        return true;
    }

    if (mode != ETableReplicaMode::SyncToAsync) {
        return false;
    }

    // Replica in transient state MUST have previous non-transient state
    YT_VERIFY(!replicaHistory.empty());
    return replicaHistory.back().IsSync();
}

bool IsReplicaAsync(ETableReplicaMode mode)
{
    return mode == ETableReplicaMode::Async || mode == ETableReplicaMode::AsyncToSync;
}

bool IsReplicaEnabled(ETableReplicaState state)
{
    return state == ETableReplicaState::Enabled || state == ETableReplicaState::Disabling;
}

bool IsReplicaDisabled(ETableReplicaState state)
{
    return state == ETableReplicaState::Disabled || state == ETableReplicaState::Enabling;
}

bool IsReplicaReallySync(
    ETableReplicaMode mode,
    ETableReplicaState state,
    const std::vector<TReplicaHistoryItem>& replicaHistory)
{
    return IsReplicaSync(mode, replicaHistory) && IsReplicaEnabled(state);
}

ETableReplicaMode GetTargetReplicaMode(ETableReplicaMode mode)
{
    return (mode == ETableReplicaMode::Sync || mode == ETableReplicaMode::AsyncToSync)
        ? ETableReplicaMode::Sync
        : ETableReplicaMode::Async;
}

ETableReplicaState GetTargetReplicaState(ETableReplicaState state)
{
    return (state == ETableReplicaState::Enabled || state == ETableReplicaState::Enabling)
        ? ETableReplicaState::Enabled
        : ETableReplicaState::Disabled;
}

void UpdateReplicationProgress(TReplicationProgress* progress, const TReplicationProgress& update)
{
    std::vector<TReplicationProgress::TSegment> segments;
    auto progressIt = progress->Segments.begin();
    auto progressEnd = progress->Segments.end();
    auto updateIt = update.Segments.begin();
    auto updateEnd = update.Segments.end();
    auto progressTimestamp = NullTimestamp;
    auto updateTimestamp = NullTimestamp;

    auto append = [&] (TUnversionedOwningRow key) {
        auto timestamp = std::max(progressTimestamp, updateTimestamp);
        if (segments.empty() || segments.back().Timestamp != timestamp) {
            segments.push_back({std::move(key), timestamp});
        }
    };

    bool upper = false;
    auto processUpperKey = [&] (const TUnversionedOwningRow& key) {
        if (upper || updateIt != updateEnd) {
            return;
        }

        auto cmpResult = CompareRows(key, update.UpperKey);
        if (cmpResult >= 0) {
            updateTimestamp = NullTimestamp;
            upper = true;
        }
        if (cmpResult > 0) {
            append(update.UpperKey);
        }
    };

    while (progressIt < progressEnd || updateIt < updateEnd) {
        int cmpResult;
        if (updateIt == updateEnd) {
            cmpResult = -1;
        } else if (progressIt == progressEnd) {
            cmpResult = 1;
        } else {
            cmpResult = CompareRows(progressIt->LowerKey, updateIt->LowerKey);
        }

        if (cmpResult < 0) {
            if (updateIt == updateEnd) {
                processUpperKey(progressIt->LowerKey);
            }
            progressTimestamp = progressIt->Timestamp;
            append(std::move(progressIt->LowerKey));
            ++progressIt;
        } else if (cmpResult > 0) {
            updateTimestamp = updateIt->Timestamp;
            append(updateIt->LowerKey);
            ++updateIt;
        } else {
            updateTimestamp = updateIt->Timestamp;
            progressTimestamp = progressIt->Timestamp;
            append(std::move(progressIt->LowerKey));
            ++progressIt;
            ++updateIt;
        }
    }

    processUpperKey(progress->UpperKey);
    progress->Segments = std::move(segments);
}

bool IsReplicationProgressGreaterOrEqual(const TReplicationProgress& progress, const TReplicationProgress& other)
{
    auto progressIt = progress.Segments.begin();
    auto otherIt = std::upper_bound(
        other.Segments.begin(),
        other.Segments.end(),
        progressIt->LowerKey,
        [] (const auto& lhs, const auto& rhs) {
            return CompareRows(lhs, rhs.LowerKey) < 0;
        });

    auto progressEnd = progress.Segments.end();
    auto otherEnd = other.Segments.end();
    auto progressTimestamp = MaxTimestamp;
    auto otherTimestamp = otherIt == other.Segments.begin()
        ? NullTimestamp
        : (otherIt - 1)->Timestamp;

    while (progressIt < progressEnd || otherIt < otherEnd) {
        int cmpResult;
        if (otherIt == otherEnd) {
            if (CompareRows(progressIt->LowerKey, other.UpperKey) >= 0) {
                return true;
            }
            cmpResult = -1;
        } else if (progressIt == progressEnd) {
            if (CompareRows(progress.UpperKey, otherIt->LowerKey) <= 0) {
                return true;
            }
            cmpResult = 1;
        } else {
            cmpResult = CompareRows(progressIt->LowerKey, otherIt->LowerKey);
        }

        if (cmpResult < 0) {
            progressTimestamp = progressIt->Timestamp;
            ++progressIt;
        } else if (cmpResult > 0) {
            otherTimestamp = otherIt->Timestamp;
            ++otherIt;
        } else {
            progressTimestamp = progressIt->Timestamp;
            otherTimestamp = otherIt->Timestamp;
            ++progressIt;
            ++otherIt;
        }

        if (progressTimestamp < otherTimestamp) {
            return false;
        }
    }

    return true;
}

bool IsReplicationProgressEqual(const TReplicationProgress& progress, const TReplicationProgress& other)
{
    if (progress.Segments.size() != other.Segments.size() || progress.UpperKey != other.UpperKey) {
        return false;
    }
    for (int index = 0; index < std::ssize(progress.Segments); ++index) {
        const auto& segment = progress.Segments[index];
        const auto& otherSegment = other.Segments[index];
        if (segment.LowerKey != otherSegment.LowerKey || segment.Timestamp != otherSegment.Timestamp) {
            return false;
        }
    }
    return true;
}

bool IsReplicationProgressGreaterOrEqual(const TReplicationProgress& progress, TTimestamp timestamp)
{
    for (const auto& segment : progress.Segments) {
        if (segment.Timestamp < timestamp) {
            return false;
        }
    }
    return true;
}

TReplicationProgress ExtractReplicationProgress(
    const TReplicationProgress& progress,
    TLegacyKey lower,
    TLegacyKey upper)
{
    TReplicationProgress extracted;
    extracted.UpperKey = TUnversionedOwningRow(upper);

    auto it = std::upper_bound(
        progress.Segments.begin(),
        progress.Segments.end(),
        lower,
        [] (const auto& lhs, const auto& rhs) {
            return CompareRows(lhs, rhs.LowerKey) < 0;
        });

    YT_VERIFY(it != progress.Segments.begin());
    --it;

    extracted.Segments.push_back({TUnversionedOwningRow(lower), it->Timestamp});

    for (++it; it < progress.Segments.end() && it->LowerKey < upper; ++it) {
        extracted.Segments.push_back(*it);
    }

    return extracted;
}

TReplicationProgress AdvanceReplicationProgress(const TReplicationProgress& progress, TTimestamp timestamp)
{
    TReplicationProgress result;
    result.UpperKey = progress.UpperKey;

    for (const auto& segment : progress.Segments) {
        if (segment.Timestamp > timestamp) {
            result.Segments.push_back(segment);
        } else if (result.Segments.empty() || result.Segments.back().Timestamp > timestamp) {
            result.Segments.push_back({segment.LowerKey, timestamp});
        }
    }

    return result;
}

TReplicationProgress LimitReplicationProgressByTimestamp(const TReplicationProgress& progress, TTimestamp timestamp)
{
    TReplicationProgress result;
    result.UpperKey = progress.UpperKey;

    for (const auto& segment : progress.Segments) {
        if (segment.Timestamp < timestamp) {
            result.Segments.push_back(segment);
        } else if (result.Segments.empty() || result.Segments.back().Timestamp < timestamp) {
            result.Segments.push_back({segment.LowerKey, timestamp});
        }
    }

    return result;
}

void CanonizeReplicationProgress(TReplicationProgress* progress)
{
    int current = 0;
    for (int index = 1; index < std::ssize(progress->Segments); ++index) {
        if (progress->Segments[current].Timestamp != progress->Segments[index].Timestamp) {
            ++current;
            if (current != index) {
                progress->Segments[current] = std::move(progress->Segments[index]);
            }
        }
    }
    progress->Segments.resize(current + 1);
}

TTimestamp GetReplicationProgressMinTimestamp(const TReplicationProgress& progress)
{
    auto minTimestamp = MaxTimestamp;
    for (const auto& segment : progress.Segments) {
        minTimestamp = std::min(segment.Timestamp, minTimestamp);
    }
    return minTimestamp;
}

TTimestamp GetReplicationProgressMaxTimestamp(const TReplicationProgress& progress)
{
    auto maxTimestamp = MinTimestamp;
    for (const auto& segment : progress.Segments) {
        maxTimestamp = std::max(segment.Timestamp, maxTimestamp);
    }
    return maxTimestamp;
}

std::optional<TTimestamp> FindReplicationProgressTimestampForKey(
    const TReplicationProgress& progress,
    TUnversionedValueRange key)
{
    if (CompareValueRanges(progress.UpperKey.Elements(), key) <= 0 ||
        CompareValueRanges(progress.Segments[0].LowerKey.Elements(), key) > 0)
    {
        return {};
    }

    auto it = std::upper_bound(
        progress.Segments.begin(),
        progress.Segments.end(),
        key,
        [&] (const auto& /*key*/, const auto& segment) {
            return CompareValueRanges(key, segment.LowerKey.Elements()) < 0;
        });
    YT_VERIFY(it > progress.Segments.begin());

    return (it - 1)->Timestamp;
}

TTimestamp GetReplicationProgressTimestampForKeyOrThrow(
    const TReplicationProgress& progress,
    TUnversionedRow key)
{
    auto timestamp = FindReplicationProgressTimestampForKey(progress, key.Elements());
    if (!timestamp) {
        THROW_ERROR_EXCEPTION("Key %v is out or replication progress range", key);
    }
    return *timestamp;
}

TTimestamp GetReplicationProgressMinTimestamp(
    const TReplicationProgress& progress,
    TLegacyKey lower,
    TLegacyKey upper)
{
    auto it = std::upper_bound(
        progress.Segments.begin(),
        progress.Segments.end(),
        lower,
        [] (const auto& lhs, const auto& rhs) {
            return CompareRows(lhs, rhs.LowerKey) < 0;
        });

    YT_VERIFY(it != progress.Segments.begin());
    --it;

    auto minTimestamp = MaxTimestamp;
    for (; it < progress.Segments.end() && it->LowerKey < upper; ++it) {
        minTimestamp = std::min(it->Timestamp, minTimestamp);
    }
    return minTimestamp;
}

TReplicationProgress GatherReplicationProgress(
    std::vector<TReplicationProgress> progresses,
    const std::vector<TUnversionedRow>& pivotKeys,
    TUnversionedRow upperKey)
{
    YT_VERIFY(progresses.size() == pivotKeys.size());
    YT_VERIFY(!pivotKeys.empty() && pivotKeys.back() < upperKey);

    TReplicationProgress progress;
    for (int index = 0; index < std::ssize(progresses); ++index) {
        auto& segments = progresses[index].Segments;
        auto lowerKey = pivotKeys[index];
        if (segments.empty()) {
            progress.Segments.push_back({TUnversionedOwningRow(lowerKey), MinTimestamp});
        } else {
            YT_VERIFY(lowerKey == segments[0].LowerKey);
            progress.Segments.insert(
                progress.Segments.end(),
                std::make_move_iterator(segments.begin()),
                std::make_move_iterator(segments.end()));
        }
    }

    YT_VERIFY(upperKey > progress.Segments.back().LowerKey);
    progress.UpperKey = TUnversionedOwningRow(upperKey);
    CanonizeReplicationProgress(&progress);
    return progress;
}

std::vector<TReplicationProgress> ScatterReplicationProgress(
    TReplicationProgress progress,
    const std::vector<TUnversionedRow>& pivotKeys,
    TUnversionedRow upperKey)
{
    YT_VERIFY(!pivotKeys.empty() && !progress.Segments.empty());
    YT_VERIFY(pivotKeys[0] >= progress.Segments[0].LowerKey);
    YT_VERIFY(progress.UpperKey.Get() >= upperKey);
    YT_VERIFY(pivotKeys.back() < upperKey);

    std::vector<TReplicationProgress> result;
    auto& segments = progress.Segments;
    int segmentIndex = 0;
    int pivotIndex = 0;
    auto previousTimestamp = MaxTimestamp;

    while (pivotIndex < std::ssize(pivotKeys)) {
        auto& pivotKey = pivotKeys[pivotIndex];
        auto cmpResult = segmentIndex < std::ssize(segments)
            ? CompareRows(pivotKey, segments[segmentIndex].LowerKey.Get())
            : -1;

        if (cmpResult <= 0) {
            if (!result.empty()) {
                result.back().UpperKey = TUnversionedOwningRow(pivotKey);
            }

            result.emplace_back();
            YT_VERIFY(cmpResult == 0 || segmentIndex > 0);
            auto timestamp = cmpResult == 0
                ? segments[segmentIndex].Timestamp
                : previousTimestamp;
            result.back().Segments.push_back({TUnversionedOwningRow(pivotKey), timestamp});

            ++pivotIndex;
            if (cmpResult == 0) {
                previousTimestamp = segments[segmentIndex].Timestamp;
                ++segmentIndex;
            }
        } else {
            previousTimestamp = segments[segmentIndex].Timestamp;
            if (!result.empty()) {
                result.back().Segments.push_back(std::move(segments[segmentIndex]));
            }
            ++segmentIndex;
        }
    }

    for (; segmentIndex < std::ssize(segments) && segments[segmentIndex].LowerKey < upperKey; ++segmentIndex) {
        result.back().Segments.push_back(std::move(segments[segmentIndex]));
    }

    result.back().UpperKey = TUnversionedOwningRow(upperKey);
    return result;
}

bool IsReplicaLocationValid(
    const TReplicaInfo* replica,
    const NYPath::TYPath& tablePath,
    const TString& clusterName)
{
    return replica->ReplicaPath == tablePath && replica->ClusterName == clusterName;
}

TReplicationProgress BuildMaxProgress(
    const TReplicationProgress& progress,
    const TReplicationProgress& other)
{
    if (progress.Segments.empty()) {
        return other;
    }
    if (other.Segments.empty()) {
        return progress;
    }

    TReplicationProgress result;

    auto progressIt = progress.Segments.begin();
    auto otherIt = other.Segments.begin();
    auto progressEnd = progress.Segments.end();
    auto otherEnd = other.Segments.end();

    auto progressTimestamp = NullTimestamp;
    auto otherTimestamp = NullTimestamp;

    bool upperKeySelected = false;

    auto tryAppendSegment =[&result] (TUnversionedOwningRow row, TTimestamp timestamp) {
        if (result.Segments.empty() || result.Segments.back().Timestamp != timestamp) {
            result.Segments.push_back({std::move(row), timestamp});
        }
    };

    while (progressIt != progressEnd || otherIt != otherEnd) {
        int cmpResult;

        if (otherIt == otherEnd) {
            cmpResult = -1;
            if (!upperKeySelected && CompareRows(progressIt->LowerKey, other.UpperKey) >= 0) {
                upperKeySelected = true;
                otherTimestamp = NullTimestamp;
                tryAppendSegment(other.UpperKey, progressTimestamp);
                continue;
            }
        } else if (progressIt == progressEnd) {
            cmpResult = 1;
            if (!upperKeySelected && CompareRows(otherIt->LowerKey, progress.UpperKey) >= 0) {
                upperKeySelected = true;
                progressTimestamp = NullTimestamp;
                tryAppendSegment(progress.UpperKey, otherTimestamp);
                continue;
            }
        } else {
            cmpResult = CompareRows(progressIt->LowerKey, otherIt->LowerKey);
        }

        TUnversionedOwningRow lowerKey;
        if (cmpResult < 0) {
            progressTimestamp = progressIt->Timestamp;
            lowerKey = progressIt->LowerKey;
            ++progressIt;
        } else if (cmpResult > 0) {
            otherTimestamp = otherIt->Timestamp;
            lowerKey = otherIt->LowerKey;
            ++otherIt;
        } else {
            progressTimestamp = progressIt->Timestamp;
            otherTimestamp = otherIt->Timestamp;
            lowerKey = progressIt->LowerKey;
            ++progressIt;
            ++otherIt;
        }

        tryAppendSegment(std::move(lowerKey), std::max(progressTimestamp, otherTimestamp));
    }

    auto cmpResult = CompareRows(progress.UpperKey, other.UpperKey);
    result.UpperKey = cmpResult > 0 ? progress.UpperKey : other.UpperKey;

    if (!upperKeySelected) {
        if (cmpResult > 0) {
            tryAppendSegment(other.UpperKey, progressTimestamp);
        } else if (cmpResult < 0) {
            tryAppendSegment(progress.UpperKey, otherTimestamp);
        }
    }

    return result;
}

TDuration ComputeReplicationProgressLag(
    const TReplicationProgress& syncProgress,
    const TReplicationProgress& replicaProgress)
{
    if (syncProgress.Segments.empty()) {
        return TDuration::Zero();
    }
    auto timestampDiff = [] (TTimestamp loTimestamp, TTimestamp hiTimestamp) {
        if (loTimestamp >= hiTimestamp) {
            return TDuration::Zero();
        }
        return TimestampDiffToDuration(loTimestamp, hiTimestamp).first;
    };

    if (replicaProgress.Segments.empty()) {
        return timestampDiff(GetReplicationProgressMaxTimestamp(syncProgress), NullTimestamp);
    }

    auto syncIt = syncProgress.Segments.begin();
    auto replicaIt = replicaProgress.Segments.begin();
    auto syncEnd = syncProgress.Segments.end();
    auto replicaEnd = replicaProgress.Segments.end();

    auto syncSegmentTimestamp = NullTimestamp;
    auto replicaSegmentTimestamp = NullTimestamp;
    auto lag = TDuration::Zero();

    while (syncIt != syncEnd || replicaIt != replicaEnd) {
        int cmpResult;
        if (syncIt == syncEnd) {
            cmpResult = -1;
            if (CompareRows(replicaIt->LowerKey, syncProgress.UpperKey) >= 0) {
                syncSegmentTimestamp = NullTimestamp;
            }
        } else if (replicaIt == replicaEnd) {
            cmpResult = 1;
            if (CompareRows(syncIt->LowerKey, replicaProgress.UpperKey) >= 0) {
                replicaSegmentTimestamp = NullTimestamp;
            }
        } else {
            cmpResult = CompareRows(replicaIt->LowerKey, syncIt->LowerKey);
        }

        if (cmpResult > 0) {
            syncSegmentTimestamp = syncIt->Timestamp;
            ++syncIt;
        } else if (cmpResult < 0) {
            replicaSegmentTimestamp = replicaIt->Timestamp;
            ++replicaIt;
        } else {
            syncSegmentTimestamp = syncIt->Timestamp;
            replicaSegmentTimestamp = replicaIt->Timestamp;
            ++replicaIt;
            ++syncIt;
        }

        lag = std::max(lag, timestampDiff(replicaSegmentTimestamp, syncSegmentTimestamp));
    }

    if (CompareRows(syncProgress.UpperKey, replicaProgress.UpperKey) > 0) {
        lag = std::max(lag, timestampDiff(NullTimestamp, syncProgress.Segments.back().Timestamp));
    }

    return lag;
}

THashMap<TReplicaId, TDuration> ComputeReplicasLag(const THashMap<TReplicaId, TReplicaInfo>& replicas)
{
    TReplicationProgress syncProgress;
    for (const auto& [replicaId, replicaInfo] : replicas) {
        if (IsReplicaReallySync(replicaInfo.Mode, replicaInfo.State, replicaInfo.History)) {
            if (syncProgress.Segments.empty()) {
                syncProgress = replicaInfo.ReplicationProgress;
            } else {
                syncProgress = BuildMaxProgress(syncProgress, replicaInfo.ReplicationProgress);
            }
        }
    }

    THashMap<TReplicaId, TDuration> result;
    for (const auto& [replicaId, replicaInfo] : replicas) {
        if (IsReplicaReallySync(replicaInfo.Mode, replicaInfo.State, replicaInfo.History)) {
            result.emplace(replicaId, TDuration::Zero());
        } else {
            result.emplace(
                replicaId,
                ComputeReplicationProgressLag(syncProgress, replicaInfo.ReplicationProgress));
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

