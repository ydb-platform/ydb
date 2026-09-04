#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/printable_params.h>

#include <util/generic/string.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <array>
#include <span>
#include <variant>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

inline constexpr size_t MaxLogTagCount = 4;

using TLogParam = NBlockStore::TPrintableParam;

class TChildLogTitle;

////////////////////////////////////////////////////////////////////////////////

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
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;
    };

    struct TPartitionDirect
    {
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;
    };

    struct TFastPathService
    {
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;
    };

    struct TDirectBlockGroup
    {
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;

        size_t DBGIndex = 0;
    };

    struct TVChunk
    {
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;

        ui32 DBGIndex = 0;
        ui32 VChunkIndex = 0;
    };

    struct TDDiskDataCopier
    {
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;

        ui32 DBGIndex = 0;
        ui32 VChunkIndex = 0;
        int Destination = 0;
    };

    struct TInterconnectTransport
    {
        TString DiskId;
        ui64 TabletId = 0;
        ui32 Generation = 0;

        size_t DBGIndex = 0;
    };

private:
    using TData = std::variant<
        TVolume,
        TPartitionDirect,
        TFastPathService,
        TDirectBlockGroup,
        TVChunk,
        TDDiskDataCopier,
        TInterconnectTransport>;

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

    template <size_t N>
    [[nodiscard]] TChildLogTitle GetChildWithTags(
        ui64 startTime,
        const TLogParam (&tags)[N]) const;

    [[nodiscard]] TString Get(EDetails details) const;

    [[nodiscard]] TString GetWithTime() const;
    [[nodiscard]] TString GetBrief() const;

    void SetDiskId(TString diskId);
    void SetGeneration(ui32 generation);
    void SetTabletId(ui64 tabletId);

private:
    void Rebuild();

    [[nodiscard]] TChildLogTitle MakeChild(
        ui64 startTime,
        std::span<const TLogParam> tags) const;
};

class TChildLogTitle
{
private:
    friend class TLogTitle;

    TString ParentPrefix;
    ui64 ParentStartTime = 0;
    ui64 StartTime = 0;
    std::array<TLogParam, MaxLogTagCount> Tags;
    size_t TagCount = 0;

    TChildLogTitle(
        TString parentPrefix,
        ui64 parentStartTime,
        ui64 startTime,
        std::span<const TLogParam> tags);

public:
    [[nodiscard]] TString GetWithTime() const;
};

template <size_t N>
TChildLogTitle TLogTitle::GetChildWithTags(
    const ui64 startTime,
    const TLogParam (&tags)[N]) const
{
    static_assert(
        N <= MaxLogTagCount,
        "too many log tags, raise MaxLogTagCount");
    return MakeChild(startTime, std::span<const TLogParam>(tags, N));
}

}   // namespace NYdb::NBS
