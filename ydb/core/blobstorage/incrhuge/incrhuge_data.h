#pragma once

#include "defs.h"

namespace NKikimr {
    namespace NIncrHuge {

#pragma pack(push, 1)
        struct TBlobIndexRecord {
            ui64          Id;               // raw TIncrHugeBlobId
            ui64          Lsn;              // Owner's LSN
            TBlobMetadata Meta;             // non-transparent owner metadata
            ui32          PayloadSize : 24; // size of payload in bytes
            ui32          Owner : 8;        // blob owner

            friend bool operator ==(const TBlobIndexRecord& left, const TBlobIndexRecord& right) {
                return memcmp(&left, &right, sizeof(TBlobIndexRecord)) == 0;
            }

            TString ToString() const {
                return Sprintf("{Id# %016" PRIx64 " Lsn# %" PRIu64 " PayloadSize# %" PRIu32 " Owner# %" PRIu32 "}",
                        Id, Lsn, PayloadSize, Owner);
            }
        };

        struct TBlobIndexHeader {
            ui32             Checksum;    // checksum (CRC32C) of TBlobIndexHeader + data
            TChunkSerNum     ChunkSerNum; // unique serial number of chunk this index header was written to
            ui32             NumItems;    // number of records in Index[]

            const TBlobIndexRecord *InplaceIndexBegin() const {
                return reinterpret_cast<const TBlobIndexRecord *>(this + 1);
            }

            const TBlobIndexRecord *InplaceIndexEnd() const {
                return InplaceIndexBegin() + NumItems;
            }
        };

        struct TBlobHeader {
            ui32             Checksum;    // checksum (CRC32C) of TBlobHeader + data, not including checksum
            TBlobIndexRecord IndexRecord; // index record for this blob
            TChunkSerNum     ChunkSerNum; // unique serial number of chunk this blob was written to

            TString ExtractInplacePayload() const {
                const char *begin = reinterpret_cast<const char *>(this + 1);
                const char *end = begin + IndexRecord.PayloadSize;
                return TString(begin, end);
            }
        };
#pragma pack(pop)

    } // NIncrHuge
} // NKikimr
