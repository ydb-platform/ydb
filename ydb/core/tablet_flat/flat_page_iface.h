#pragma once

#include <util/system/types.h>
#include <util/generic/utility.h>
#include <util/stream/output.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    using TSize = ui32;
    using TPageId = ui32;

    class TPageOffset {
        ui64 Raw;

        static constexpr ui64 MaxRaw = ::Max<ui64>();
        static constexpr ui64 IndexTag = ui64(1) << 63;

        explicit constexpr TPageOffset(ui64 raw) noexcept
            : Raw(raw)
        {}

    public:
        /** Default constructor — Max sentinel (all bits set) */
        TPageOffset() noexcept
            : Raw(MaxRaw)
        {}

        /** Factory: create from a real byte offset (pages in a blob sequence).
            Clears the MSB to prevent accidental PageIndex tag. */
        static TPageOffset FromByteOffset(ui64 value) noexcept {
            Y_VERIFY_DEBUG(value < IndexTag,
                "Byte offset is too large to be represented as TPageOffset");
            return TPageOffset(value);
        }

        /** Factory: create from a page index (independently addressed pages).
            Sets the MSB to tag this as a page index. */
        static TPageOffset FromPageIndex(ui32 value) noexcept {
            return TPageOffset(static_cast<ui64>(value) | IndexTag);
        }

        /** Max sentinel — all bits set */
        static TPageOffset Max() noexcept { return TPageOffset(); }

        /** True when this is not the Max sentinel */
        explicit operator bool() const noexcept { return Raw != MaxRaw; }

        bool IsMax() const noexcept { return Raw == MaxRaw; }
        bool IsByteOffset() const noexcept { return !IsMax() && !(Raw & IndexTag); }
        bool IsPageIndex() const noexcept { return !IsMax() && (Raw & IndexTag); }

        /** Access as a real byte offset — asserts ByteOffset kind */
        ui64 AsByteOffset() const noexcept {
            Y_VERIFY_DEBUG(IsByteOffset(),
                "TPageOffset is not a byte offset (raw=%" PRIu64 ")", Raw);
            return Raw;
        }

        /** Access as a page index — asserts PageIndex kind */
        ui32 AsPageIndex() const noexcept {
            Y_VERIFY_DEBUG(IsPageIndex(),
                "TPageOffset is not a page index (raw=%" PRIu64 ")", Raw);
            return static_cast<ui32>(Raw);
        }

        bool operator==(const TPageOffset& rhs) const noexcept { return Raw == rhs.Raw; }
        bool operator!=(const TPageOffset& rhs) const noexcept { return Raw != rhs.Raw; }

        explicit operator size_t() const noexcept {
            return static_cast<size_t>(Raw);
        }

        void Describe(IOutputStream& out) const noexcept {
            if (IsMax()) {
                out << "Max";
            } else if (IsByteOffset()) {
                out << "bo:" << Raw;
            } else {
                out << "pi:" << static_cast<ui32>(Raw);
            }
        }

        friend IOutputStream& operator<<(IOutputStream& out, const TPageOffset& offset) {
            offset.Describe(out);
            return out;
        }
    };

    struct TPageLocation {
        TPageOffset Offset;
        ui64 Size = 0;
        ui32 Crc32 = 0;
        ui32 Pad0_ = 0;

        TPageLocation() = default;

        /// Construct from a real byte offset (pages in a blob sequence)
        static TPageLocation FromByteOffset(ui64 offset, ui64 size,
                                            ui32 crc32 = 0) noexcept {
            return {TPageOffset::FromByteOffset(offset), size, crc32};
        }

        /// Construct from a page index (independently addressed pages)
        static TPageLocation FromPageIndex(ui32 pageId, ui64 size) noexcept {
            return {TPageOffset::FromPageIndex(pageId), size, 0};
        }

    private:
        TPageLocation(TPageOffset offset, ui64 size, ui32 crc32) noexcept
            : Offset(offset), Size(size), Crc32(crc32)
        {}

    public:

        explicit operator bool() const noexcept { return bool(Offset); }

        /** Access the offset as a byte offset — asserts the location was built from FromByteOffset */
        ui64 GetByteOffset() const noexcept { return Offset.AsByteOffset(); }

        /** Access the offset as a page index — asserts the location was built from FromPageIndex */
        ui32 GetPageIndex() const noexcept { return Offset.AsPageIndex(); }

        bool operator==(const TPageLocation& rhs) const noexcept {
            // Skip Crc32 as it can be zero
            // 2 pages at the same offset is impossible
            Y_VERIFY_DEBUG(Offset != rhs.Offset || Size == rhs.Size);
            return Offset == rhs.Offset;
        }
        bool operator!=(const TPageLocation& rhs) const noexcept { return !(*this == rhs); }

        static TPageLocation Max() noexcept { return TPageLocation{}; }

        void Describe(IOutputStream& out) const {
            out << "{" << Offset << "," << Size << "," << Crc32 << "}";
        }
    };

    /**
     * A type-safe column group identifier
     */
    struct TGroupId {
        ui32 Index : 31;
        ui32 Historic : 1;

        TGroupId() noexcept
            : Index(0)
            , Historic(0)
        { }

        explicit TGroupId(ui32 index, bool historic = false) noexcept
            : Index(index)
            , Historic(historic)
        { }

        ui32 Raw() const noexcept {
            return (Historic << 31) | Index;
        }

        bool IsMain() const noexcept {
            return Index == 0 && Historic == 0;
        }

        bool IsHistoric() const noexcept {
            return Historic;
        }

        bool operator<(TGroupId rhs) const noexcept { return Raw() < rhs.Raw(); }
        bool operator>(TGroupId rhs) const noexcept { return Raw() > rhs.Raw(); }
        bool operator<=(TGroupId rhs) const noexcept { return Raw() <= rhs.Raw(); }
        bool operator>=(TGroupId rhs) const noexcept { return Raw() >= rhs.Raw(); }
        bool operator==(TGroupId rhs) const noexcept { return Raw() == rhs.Raw(); }
        bool operator!=(TGroupId rhs) const noexcept { return Raw() != rhs.Raw(); }

        /**
         * Used by the default THash implementation
         */
        explicit operator size_t() const noexcept {
            return Historic * 31 + Index;
        }

        friend inline IOutputStream& operator<<(IOutputStream& out, const TGroupId& groupId) {
            return out << "{" << groupId.Index << "," << groupId.IsHistoric() << "}";
        }
    };

    enum class EPage : ui16 {
        Undef = 0,
        Scheme = 2,
        FlatIndex = 3,
        DataPage = 4,
        Frames = 5, /* Tagged entities to TRowId relation index     */
        Globs = 6,  /* Just enumeration of NPageCollection::TGlobId refs    */
        Schem2 = 7, /* New version of EPage::Scheme with TLabel     */
        Opaque = 8, /* User defined content, cell value as blob     */
        Bloom = 9,  /* Bloom filter for some app. defined cells set */
        GarbageStats = 10, /* Stats on garbage in historic data */
        TxIdStats = 11, /* Stats for uncommitted TxIds at compaction time */
        TxStatus = 12, /* Status of committed/removed transactions */
        BTreeIndex = 13,
    };

    enum class ECodec : ui8 {
        Plain   = 0,    /* Keep data in page as-is  */
        LZ4     = 1,    /* Use lz4fast compressor   */
    };

    enum class ECache {
        None    = 0,
        Once    = 1,    /* Put to cache once at load   */
        Ever    = 2,    /* Keep in cache util the end  */
    };

    enum class ECacheMode : ui32 {
        Regular = 0,
        TryKeepInMemory = 1,
    };

}
}
}
