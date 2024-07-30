#pragma once

#include "defs.h"

#include "blobstorage_pdisk_defs.h"
#include "blobstorage_pdisk_data.h"

namespace NKikimr::NPDisk {

    class TRequestBase;

} // NKikimr::NPDisk

namespace NKikimr::NPDisk::NMeta {

    enum class ESlotState {
        READ_PENDING,
        READ_IN_PROGRESS,
        PROCESSED,
        FREE,
        OCCUPIED,
        BEING_WRITTEN,
    };

    struct TSlotKey {
        TChunkIdx ChunkIdx = 0;
        ui32 OffsetInSectors = 0;

        TSlotKey() = default;
        TSlotKey(TChunkIdx chunkIdx, ui32 offsetInSectors) : ChunkIdx(chunkIdx), OffsetInSectors(offsetInSectors) {}

        friend auto operator <=>(const TSlotKey&, const TSlotKey&) = default;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TStoredMetadata variant -- different options describing what we have about metadata

    struct TScanInProgress { // we are still reading the metadata
    };

    struct TNoMetadata { // no metadata stored yet
    };

    struct TError {
        TString Description; // textual description of error happened when we loaded the metadata
    };

    using TStoredMetadata = std::variant<TScanInProgress, TNoMetadata, TError, TRcBuf>;

    inline TString ToString(const TStoredMetadata *m) {
        return std::visit<TString>(TOverloaded{
            [](const TScanInProgress&) { return "ScanInProgress"; },
            [](const TNoMetadata&) { return "NoMetadata"; },
            [](const TError& e) { return TStringBuilder() << "Error{" << e.Description << '}'; },
            [](const TRcBuf& meta) { return TStringBuilder() << "Metadata{" << meta.size() << "b}"; },
        }, *m);
    }

    // TMetadataPart -- a fragment of metadata (half-chunk) that has been read into memory, but not processed yet. 
    struct TPart {
        TSlotKey Key;
        TMetadataHeader Header;
        TRcBuf Payload;

        TPart(TSlotKey key, const TMetadataHeader& header, TRcBuf&& payload)
            : Key(key)
            , Header(header)
            , Payload(std::move(payload))
        {}

        friend auto operator <=>(const TPart& x, const TPart& y) {
            if (const auto diff = x.Header.SequenceNumber <=> y.Header.SequenceNumber; diff != 0) {
                return diff;
            } else if (const auto diff = x.Header.RecordIndex <=> y.Header.RecordIndex; diff != 0) {
                return diff;
            } else {
                return std::strong_ordering::equal;
            }
        }
    };

    // TFormatted -- state record for formatted version of PDisk metadata storage
    struct TFormatted {
        static constexpr ui32 MaxReadsInFlight = 1;

        std::map<TSlotKey, ESlotState> Slots;
        std::deque<TSlotKey> ReadPending; // slots in READ_PENDING state
        ui32 NumReadsInFlight = 0;
        std::deque<TPart> Parts;
    };

    // TUnformatted -- state record for unformatted version of PDisk metadata storage
    struct TUnformatted {
        std::optional<TMetadataFormatSector> Format; // format value, if set
        TMetadataFormatSector FormatInFlight; // being written
    };

    struct TInfo {
        std::variant<std::monostate, TFormatted, TUnformatted> State; // metadata storage state
        TStoredMetadata StoredMetadata; // the one that is persistently stored
        std::deque<std::unique_ptr<TRequestBase>> Requests; // requests pending; the first one is in flight
        bool WriteInFlight = false;
        ui64 NextSequenceNumber = 1;
    };

} // NKikimr::NPDisk::NMeta
