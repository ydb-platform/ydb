#include "log_title.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/datetime/cputimer.h>
#include <util/generic/overloaded.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TOptional
{
    const T& Value;
};

template <typename T>
TOptional(const T&) -> TOptional<T>;

template <typename T>
TStringBuilder& operator<<(TStringBuilder& stream, TOptional<T> opt)
{
    if (!opt.Value) {
        if constexpr (std::is_same_v<TString, T>) {
            stream << "???";
        } else {
            stream << "?";
        }
    } else {
        stream << opt.Value;
    }

    return stream;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept HasDiskId = requires(T t) { t.DiskId = TString(); };

template <typename T>
concept HasGeneration = requires(T t) { t.Generation = ui32(); };

template <typename T>
concept HasTabletId = requires(T t) { t.TabletId = ui64(); };

////////////////////////////////////////////////////////////////////////////////

TString GetPartitionPrefix(
    const TString& title,
    ui64 tabletId,
    ui32 partitionIndex,
    ui32 partitionCount)
{
    auto builder = TStringBuilder();

    builder << title;
    if (partitionCount > 1) {
        builder << partitionIndex;
    }
    builder << ":";
    builder << tabletId;

    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TLogTitle::TVolume& data)
{
    TStringBuilder stream;

    stream << "[v:" << data.TabletId;
    stream << " g:" << TOptional{data.Generation};
    stream << " d:" << TOptional{data.DiskId};

    return stream;
}

TString ToString(const TLogTitle::TPartitionDirect& data)
{
    TStringBuilder stream;

    stream << "[pd:" << data.TabletId;
    stream << " g:" << TOptional{data.Generation};
    stream << " d:" << TOptional{data.DiskId};

    return stream;
}

TString ToString(const TLogTitle::TDirectBlockGroup& data)
{
    TStringBuilder stream;

    stream << "[dbg:" << data.DiskId;

    return stream;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// static
TString TLogTitle::GetPartitionPrefix(
    ui64 tabletId,
    ui32 partitionIndex,
    ui32 partitionCount)
{
    return NYdb::NBS::GetPartitionPrefix(
        "p",
        tabletId,
        partitionIndex,
        partitionCount);
}

TChildLogTitle TLogTitle::GetChild(const ui64 startTime) const
{
    TStringBuilder childPrefix;
    childPrefix << CachedPrefix;
    const auto duration = CyclesToDurationSafe(startTime - StartTime);
    childPrefix << " t:" << FormatDuration(duration);

    return {childPrefix, startTime};
}

TChildLogTitle TLogTitle::GetChildWithTags(
    const ui64 startTime,
    std::span<const std::pair<TString, TString>> additionalTags) const
{
    TStringBuilder childPrefix;
    childPrefix << CachedPrefix;

    for (const auto& [key, value]: additionalTags) {
        childPrefix << " " << key << ":" << value;
    }

    const auto duration = CyclesToDurationSafe(startTime - StartTime);
    childPrefix << " t:" << FormatDuration(duration);

    return {childPrefix, startTime};
}

TChildLogTitle TLogTitle::GetChildWithTags(
    const ui64 startTime,
    std::initializer_list<std::pair<TString, TString>> additionalTags) const
{
    return GetChildWithTags(startTime, std::span(additionalTags));
}

TString TLogTitle::Get(EDetails details) const
{
    TStringBuilder result;
    result << CachedPrefix;

    switch (details) {
        case EDetails::Brief: {
            break;
        }
        case EDetails::WithTime: {
            const auto duration =
                CyclesToDurationSafe(GetCycleCount() - StartTime);
            result << " t:" << FormatDuration(duration);
            break;
        }
    }

    result << "]";
    return result;
}

TString TLogTitle::GetWithTime() const
{
    return Get(EDetails::WithTime);
}

TString TLogTitle::GetBrief() const
{
    return Get(EDetails::Brief);
}

void TLogTitle::SetDiskId(TString diskId)
{
    std::visit(
        [this, &diskId](auto& data)
        {
            if constexpr (HasDiskId<decltype(data)>) {
                data.DiskId = diskId;
            }
            CachedPrefix = ToString(data);
        },
        Data);
}

void TLogTitle::SetGeneration(ui32 generation)
{
    std::visit(
        [this, generation]<typename T>(T& data)
        {
            if constexpr (HasGeneration<decltype(data)>) {
                data.Generation = generation;
            }
            CachedPrefix = ToString(data);
        },
        Data);
}

void TLogTitle::SetTabletId(ui64 tabletId)
{
    std::visit(
        [this, tabletId]<typename T>(T& data)
        {
            if constexpr (HasTabletId<decltype(data)>) {
                data.TabletId = tabletId;
            }
            CachedPrefix = ToString(data);
        },
        Data);
}

void TLogTitle::Rebuild()
{
    std::visit([this](auto& data) { CachedPrefix = ToString(data); }, Data);
}

////////////////////////////////////////////////////////////////////////////////

TChildLogTitle::TChildLogTitle(TString cachedPrefix, ui64 startTime)
    : CachedPrefix(std::move(cachedPrefix))
    , StartTime(startTime)
{}

TString TChildLogTitle::GetWithTime() const
{
    const auto duration = CyclesToDurationSafe(GetCycleCount() - StartTime);
    TStringBuilder builder;
    builder << CachedPrefix << " + " << FormatDuration(duration) << "]";
    return builder;
}

}   // namespace NYdb::NBS
