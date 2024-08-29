#pragma once

#include <util/system/types.h>
#include <util/stream/output.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    using TSize = ui32;
    using TPageId = ui32;

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
        Plain = 0, /* Keep data in page as-is  */
        GZIP = 1,
        SNAPPY = 2,
        LZO = 3,
        BROTLI = 4,
        LZ4_RAW = 5,
        LZ4 = 6, /* Use lz4fast compressor   */
        LZ4_HADOOP = 7,
        ZSTD = 8,
        BZ2 = 9,
    };

    enum class ECache {
        None    = 0,
        Once    = 1,    /* Put to cache once at load   */
        Ever    = 2,    /* Keep in cache util the end  */
    };

}
}
}
