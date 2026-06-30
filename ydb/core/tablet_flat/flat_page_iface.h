#pragma once

#include <util/system/types.h>
#include <util/generic/hash.h>
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
        static constexpr ui64 ByteOffsetTag = ui64(1) << 63;

        explicit constexpr TPageOffset(ui64 raw) noexcept
            : Raw(raw)
        {}

    public:
        /** Default constructor — Max sentinel (all bits set) */
        TPageOffset() noexcept
            : Raw(MaxRaw)
        {}

        /** Factory: create from a real byte offset (pages in a blob sequence).
            Sets the MSB to tag this as a byte offset. */
        static TPageOffset FromByteOffset(ui64 value) {
            Y_ENSURE(value < ByteOffsetTag,
                "Byte offset is too large to be represented as TPageOffset");
            return TPageOffset(value | ByteOffsetTag);
        }

        /** Factory: create from a page index (independently addressed pages). */
        static TPageOffset FromPageIndex(ui32 value) noexcept {
            return TPageOffset(static_cast<ui64>(value));
        }

        /** Max sentinel — all bits set */
        static TPageOffset Max() noexcept { return TPageOffset(); }
        static TPageOffset Min() noexcept { return TPageOffset(ui64(0)); }

        /** True when this is not the Max sentinel */
        explicit operator bool() const noexcept { return Raw != MaxRaw; }

        bool IsMax() const noexcept { return Raw == MaxRaw; }
        bool IsByteOffset() const noexcept { return !IsMax() && (Raw & ByteOffsetTag); }
        bool IsPageIndex() const noexcept { return !IsMax() && !(Raw & ByteOffsetTag); }

        /** Access as a real byte offset — asserts ByteOffset kind */
        ui64 AsByteOffset() const {
            Y_ENSURE(IsByteOffset(), "TPageOffset is not a byte offset (raw=" << Raw << ")");
            return Raw & ~ByteOffsetTag;
        }

        /** Access as a page index — asserts PageIndex kind */
        ui32 AsPageIndex() const {
            Y_ENSURE(IsPageIndex(), "TPageOffset is not a page index (raw=" << Raw << ")");
            return static_cast<ui32>(Raw);
        }

        /** Natural ui64 ordering: page-index (no MSB) < byte-offset (MSB set) < Max */
        friend auto operator<=>(const TPageOffset& a, const TPageOffset& b) noexcept = default;

        explicit operator size_t() const noexcept {
            return static_cast<size_t>(Raw);
        }

        void Describe(IOutputStream& out) const noexcept {
            if (IsMax()) {
                out << "Max";
            } else if (IsByteOffset()) {
                out << "bo:" << AsByteOffset();
            } else {
                out << "pi:" << static_cast<ui32>(Raw);
            }
        }

        friend IOutputStream& operator<<(IOutputStream& out, const TPageOffset& offset) {
            offset.Describe(out);
            return out;
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

    struct TPageLocation {
        TPageOffset Offset;
        ui64 Size = 0;
        EPage Type = EPage::Undef;
        ui32 Crc32 = 0;

        TPageLocation() = default;

        /// Construct from a real byte offset (pages in a blob sequence)
        static TPageLocation FromByteOffset(ui64 offset, ui64 size, EPage type = EPage::Undef, ui32 crc32 = 0) noexcept {
            return {TPageOffset::FromByteOffset(offset), size, type, crc32};
        }

        /// Construct from a page index (independently addressed pages)
        static TPageLocation FromPageIndex(ui32 pageId, ui64 size, EPage type = EPage::Undef, ui32 crc32 = 0) noexcept {
            return {TPageOffset::FromPageIndex(pageId), size, type, crc32};
        }

        TPageLocation(TPageOffset offset, ui64 size, EPage type = EPage::Undef, ui32 crc32 = 0) noexcept
            : Offset(offset), Size(size), Type(type), Crc32(crc32)
        {}

        explicit operator bool() const noexcept { return bool(Offset); }

        /** Access the offset as a byte offset — asserts the location was built from FromByteOffset */
        ui64 GetByteOffset() const { return Offset.AsByteOffset(); }

        /** Access the offset as a page index — asserts the location was built from FromPageIndex */
        ui32 GetPageIndex() const { return Offset.AsPageIndex(); }

        /** Ordering by Offset only */
        auto operator<=>(const TPageLocation& rhs) const {
            // 2 different pages at the same offset is impossible
            Y_ENSURE(Offset != rhs.Offset || (Size == rhs.Size && Type == rhs.Type && Crc32 == rhs.Crc32),
                "TPageLocation at same offset but differs (Size: " << Size << " vs " << rhs.Size << ", Type: " << Type
                                                                   << " vs " << rhs.Type << ", Crc32: " << Crc32
                                                                   << " vs " << rhs.Crc32 << ")");
            return Offset <=> rhs.Offset;
        }

        bool operator==(const TPageLocation& rhs) const noexcept { return (operator<=>(rhs)) == 0; }
        bool operator<(const TPageLocation& rhs) const noexcept { return (operator<=>(rhs)) < 0; }
        bool operator<=(const TPageLocation& rhs) const noexcept { return (operator<=>(rhs)) <= 0; }
        bool operator>(const TPageLocation& rhs) const noexcept { return (operator<=>(rhs)) > 0; }
        bool operator>=(const TPageLocation& rhs) const noexcept { return (operator<=>(rhs)) >= 0; }

        static TPageLocation Max() noexcept { return TPageLocation{}; }

        void Describe(IOutputStream& out) const {
            out << "{" << Offset << "," << Size << " type=" << (ui16)Type << " crc32=" << Crc32 << "}";
        }
    };

    inline IOutputStream& operator<<(IOutputStream& out, const TPageLocation& loc) {
        loc.Describe(out);
        return out;
    }

}

}

}

/** Hash specialization for TPageOffset — uses the same bit mixing */
template<>
struct THash<NKikimr::NTable::NPage::TPageOffset> {
    size_t operator()(NKikimr::NTable::NPage::TPageOffset offset) const noexcept {
        ui64 raw = static_cast<ui64>(offset);
        raw ^= raw >> 33;
        raw *= 0xff51afd7ed558ccdULL;
        raw ^= raw >> 33;
        raw *= 0xc4ceb9fe1a85ec53ULL;
        raw ^= raw >> 33;
        return static_cast<size_t>(raw);
    }
};

namespace NKikimr {
namespace NTable {
namespace NPage {

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

    /** Hash functor for TPageLocation — uses only Offset for hashing */
    struct TPageLocationByOffsetHash {
        size_t operator()(const TPageLocation& loc) const noexcept {
            return operator()(loc.Offset);
        }
        size_t operator()(TPageOffset offset) const noexcept {
            return THash<TPageOffset>()(offset);
        }
    };

}
}
}
