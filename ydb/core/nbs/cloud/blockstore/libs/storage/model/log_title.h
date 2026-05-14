#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <span>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TChildLogTitle;

class TLogTitle
{
public:
    enum class EDetails
    {
        Brief,
        WithTime,
    };

    struct TVolume
    {
        ui64 TabletId;
        TString DiskId;
        ui32 Generation = 0;
    };

    struct TPartitionDirect
    {
        ui64 TabletId = 0;
        TString DiskId;
        ui32 Generation = 0;
    };

    struct TDirectBlockGroup
    {
        TString DiskId;
    };

private:
    using TData = std::variant<TVolume, TPartitionDirect, TDirectBlockGroup>;

    ui64 StartTime = 0;
    TData Data;

    TString CachedPrefix;

public:
    template <typename T>
    TLogTitle(ui64 startTime, T&& data)
        : StartTime(startTime)
        , Data(std::forward<T>(data))
    {
        Rebuild();
    }

    static TString
    GetPartitionPrefix(ui64 tabletId, ui32 partitionIndex, ui32 partitionCount);

    [[nodiscard]] TChildLogTitle GetChild(const ui64 startTime) const;

    [[nodiscard]] TChildLogTitle GetChildWithTags(
        ui64 startTime,
        std::span<const std::pair<TString, TString>> additionalTags) const;

    [[nodiscard]] TChildLogTitle GetChildWithTags(
        ui64 startTime,
        std::initializer_list<std::pair<TString, TString>> additionalTags)
        const;

    [[nodiscard]] TString Get(EDetails details) const;

    [[nodiscard]] TString GetWithTime() const;
    [[nodiscard]] TString GetBrief() const;

    void SetDiskId(TString diskId);
    void SetGeneration(ui32 generation);
    void SetTabletId(ui64 tabletId);

private:
    void Rebuild();
};

class TChildLogTitle
{
private:
    friend class TLogTitle;

    const TString CachedPrefix;
    const ui64 StartTime;

    TChildLogTitle(TString cachedPrefix, ui64 startTime);

public:
    [[nodiscard]] TString GetWithTime() const;
};

}   // namespace NYdb::NBS
