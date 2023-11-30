#pragma once

#include <util/generic/vector.h>
#include <util/memory/blob.h>

#define BLOBMETASIG 0x3456789Au

enum E_Multiblob_Flags {
    // if EMF_INTERLAY is clear
    //     multiblob format
    //       HeaderSize()       bytes for TMultiBlobHeader
    //       Count*sizeof(ui64) bytes for blob sizes
    //       blob1
    //       (alignment)
    //       blob2
    //       (alignment)
    //       ...
    //       (alignment)
    //       blobn
    // if EMF_INTERLAY is set
    //     multiblob format
    //       HeaderSize()       bytes for TMultiBlobHeader
    //       size1              ui64, the size of 1st blob
    //       blob1
    //       (alignment)
    //       size2              ui64, the size of 2nd blob
    //       blob2
    //       (alignment)
    //       ...
    //       (alignment)
    //       sizen              ui64, the size of n'th blob
    //       blobn
    EMF_INTERLAY = 1,

    // Means that multiblob contains blocks in TChunkedDataReader format
    // Legacy, use it only for old files, created for TChunkedDataReader
    EMF_CHUNKED_DATA_READER = 2,

    // Flags that may be configured for blobbuilder in client code
    EMF_WRITEABLE = EMF_INTERLAY,
};

struct TMultiBlobHeader {
    // data
    ui32 BlobMetaSig;
    ui32 BlobRecordSig;
    ui64 Count; // count of sub blobs
    ui32 Align; // alignment for every subblob
    ui32 Flags;
    static const ui32 RecordSig = 0x23456789;
    static inline size_t HeaderSize() {
        return 4 * sizeof(ui64);
    }
    inline const ui64* Sizes(const void* Data) const {
        return (const ui64*)((const char*)Data + HeaderSize());
    }
};

class TSubBlobs: public TVector<TBlob> {
public:
    TSubBlobs() {
    }
    TSubBlobs(const TBlob& multi) {
        ReadMultiBlob(multi);
    }
    void ReadMultiBlob(const TBlob& multi);
    const TMultiBlobHeader* GetHeader() const {
        return (const TMultiBlobHeader*)&Header;
    }

protected:
    TMultiBlobHeader Header;
    TBlob Multi;

private:
    bool ReadChunkedData(const TBlob& multi) noexcept;
};
