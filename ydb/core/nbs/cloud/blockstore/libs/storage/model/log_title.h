#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <array>
#include <concepts>
#include <cstring>
#include <initializer_list>
#include <span>
#include <type_traits>
#include <variant>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TChildLogTitle;

////////////////////////////////////////////////////////////////////////////////

// A log tag that stores its value inline and formats only when Out() is called.
// The printer is instantiated at the call site, so log_title never needs to
// know about the concrete value type.
class TLogTag
{
    static constexpr size_t MaxValueSize = 16;

public:
    TLogTag() = default;

    TLogTag(TStringBuf key, TStringBuf value);

    // The value is copied, so it need not outlive the tag. A TStringBuf value
    // must point at storage that does (string literals do).
    template <typename T>
        requires std::is_trivially_copyable_v<T> &&
                     (sizeof(T) <= MaxValueSize) && (alignof(T) <= 8) &&
                     (!std::convertible_to<const T&, TStringBuf>)
    TLogTag(TStringBuf key, const T& value)
        : Key(key)
        , Printer(+[](IOutputStream& out, const void* storage)
                  { out << *static_cast<const T*>(storage); })
    {
        static_assert(sizeof(T) <= MaxValueSize);
        std::memcpy(Storage, &value, sizeof(T));
    }

    // Would store a dangling TStringBuf into a temporary TString.
    // Constrained to exact TString so string literals still pick the
    // TStringBuf overload rather than becoming ambiguous with TString's
    // converting constructor.
    template <typename T>
        requires std::same_as<std::remove_cvref_t<T>, TString>
    TLogTag(TStringBuf key, const T& value) = delete;

    void Out(IOutputStream& out) const;

private:
    TStringBuf Key;
    void (*Printer)(IOutputStream&, const void*) = nullptr;
    alignas(8) char Storage[MaxValueSize] = {};
};

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

    [[nodiscard]] TChildLogTitle GetChildWithTags(
        ui64 startTime,
        std::span<const TLogTag> additionalTags) const;

    [[nodiscard]] TChildLogTitle GetChildWithTags(
        ui64 startTime,
        std::initializer_list<TLogTag> additionalTags) const;

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

    static constexpr size_t MaxTagCount = 4;

    TString ParentPrefix;
    ui64 ParentStartTime = 0;
    ui64 StartTime = 0;
    std::array<TLogTag, MaxTagCount> Tags;
    size_t TagCount = 0;

    TChildLogTitle(
        TString parentPrefix,
        ui64 parentStartTime,
        ui64 startTime,
        std::span<const TLogTag> tags);

public:
    [[nodiscard]] TString GetWithTime() const;
};

}   // namespace NYdb::NBS
