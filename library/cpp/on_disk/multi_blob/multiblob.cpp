#include <util/generic/yexception.h>
#include <util/system/align.h>

#include <library/cpp/on_disk/chunks/reader.h>

#include "multiblob.h"

void TSubBlobs::ReadMultiBlob(const TBlob& multi) {
    if (multi.Size() < sizeof(TMultiBlobHeader)) {
        ythrow yexception() << "not a blob, too small";
    }

    Multi = multi;
    memcpy((void*)&Header, Multi.Data(), sizeof(TMultiBlobHeader));

    if (Header.BlobMetaSig != BLOBMETASIG) {
        if (Header.BlobRecordSig != TMultiBlobHeader::RecordSig) {
            if (ReadChunkedData(multi))
                return;
        }
        ythrow yexception() << "is not a blob, MetaSig was read: "
                            << Header.BlobMetaSig
                            << ", must be" << BLOBMETASIG;
    }

    if (Header.BlobRecordSig != TMultiBlobHeader::RecordSig)
        ythrow yexception() << "unknown multiblob RecordSig "
                            << Header.BlobRecordSig;

    reserve(size() + Header.Count);
    if (Header.Flags & EMF_INTERLAY) {
        size_t pos = Header.HeaderSize();
        for (size_t i = 0; i < Header.Count; ++i) {
            pos = AlignUp<ui64>(pos, sizeof(ui64));
            ui64 size = *((ui64*)((const char*)multi.Data() + pos));
            pos = AlignUp<ui64>(pos + sizeof(ui64), Header.Align);
            push_back(multi.SubBlob(pos, pos + size));
            pos += size;
        }
    } else {
        const ui64* sizes = Header.Sizes(multi.Data());
        size_t pos = Header.HeaderSize() + Header.Count * sizeof(ui64);
        for (size_t i = 0; i < Header.Count; ++i) {
            pos = AlignUp<ui64>(pos, Header.Align);
            push_back(multi.SubBlob(pos, pos + *sizes));
            pos += *sizes;
            sizes++;
        }
    }
}

bool TSubBlobs::ReadChunkedData(const TBlob& multi) noexcept {
    Multi = multi;
    memset((void*)&Header, 0, sizeof(Header));

    TChunkedDataReader reader(Multi);
    Header.Count = reader.GetBlocksCount();
    resize(GetHeader()->Count);
    for (size_t i = 0; i < size(); ++i)
        // We can use TBlob::NoCopy() because of reader.GetBlock(i) returns
        // address into memory of multi blob.
        // This knowledge was acquired from implementation of
        // TChunkedDataReader, so we need care about any changes that.
        (*this)[i] = TBlob::NoCopy(reader.GetBlock(i), reader.GetBlockLen(i));
    Header.Flags |= EMF_CHUNKED_DATA_READER;
    return true;
}
