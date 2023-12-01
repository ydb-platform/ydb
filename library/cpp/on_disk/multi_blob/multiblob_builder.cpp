#include <util/memory/tempbuf.h>
#include <util/system/align.h>

#include "multiblob_builder.h"

/*
 * TBlobSaverMemory
 */
TBlobSaverMemory::TBlobSaverMemory(const void* ptr, size_t size)
    : Blob(TBlob::NoCopy(ptr, size))
{
}

TBlobSaverMemory::TBlobSaverMemory(const TBlob& blob)
    : Blob(blob)
{
}

void TBlobSaverMemory::Save(IOutputStream& output, ui32 /*flags*/) {
    output.Write((void*)Blob.Data(), Blob.Length());
}

size_t TBlobSaverMemory::GetLength() {
    return Blob.Length();
}

/*
 * TBlobSaverFile
 */

TBlobSaverFile::TBlobSaverFile(TFile file)
    : File(file)
{
    Y_ASSERT(File.IsOpen());
}

TBlobSaverFile::TBlobSaverFile(const char* filename, EOpenMode oMode)
    : File(filename, oMode)
{
    Y_ASSERT(File.IsOpen());
}

void TBlobSaverFile::Save(IOutputStream& output, ui32 /*flags*/) {
    TTempBuf buffer(1 << 20);
    while (size_t size = File.Read((void*)buffer.Data(), buffer.Size()))
        output.Write((void*)buffer.Data(), size);
}

size_t TBlobSaverFile::GetLength() {
    return File.GetLength();
}

/*
 * TMultiBlobBuilder
 */

TMultiBlobBuilder::TMultiBlobBuilder(bool isOwn)
    : IsOwner(isOwn)
{
}

TMultiBlobBuilder::~TMultiBlobBuilder() {
    if (IsOwner)
        DeleteSubBlobs();
}

namespace {
    ui64 PadToAlign(IOutputStream& output, ui64 fromPos, ui32 align) {
        ui64 toPos = AlignUp<ui64>(fromPos, align);
        for (; fromPos < toPos; ++fromPos) {
            output << (char)0;
        }
        return toPos;
    }
}

void TMultiBlobBuilder::Save(IOutputStream& output, ui32 flags) {
    TMultiBlobHeader header;
    memset((void*)&header, 0, sizeof(header));
    header.BlobMetaSig = BLOBMETASIG;
    header.BlobRecordSig = TMultiBlobHeader::RecordSig;
    header.Count = Blobs.size();
    header.Align = ALIGN;
    header.Flags = flags & EMF_WRITEABLE;
    output.Write((void*)&header, sizeof(header));
    for (size_t i = sizeof(header); i < header.HeaderSize(); ++i)
        output << (char)0;
    ui64 pos = header.HeaderSize();
    if (header.Flags & EMF_INTERLAY) {
        for (size_t i = 0; i < Blobs.size(); ++i) {
            ui64 size = Blobs[i]->GetLength();
            pos = PadToAlign(output, pos, sizeof(ui64));                // Align size record
            output.Write((void*)&size, sizeof(ui64));
            pos = PadToAlign(output, pos + sizeof(ui64), header.Align); // Align blob
            Blobs[i]->Save(output, header.Flags);
            pos += size;
        }
    } else {
        for (size_t i = 0; i < Blobs.size(); ++i) {
            ui64 size = Blobs[i]->GetLength();
            output.Write((void*)&size, sizeof(ui64));
        }
        pos += Blobs.size() * sizeof(ui64);
        for (size_t i = 0; i < Blobs.size(); ++i) {
            pos = PadToAlign(output, pos, header.Align);
            Blobs[i]->Save(output, header.Flags);
            pos += Blobs[i]->GetLength();
        }
    }
    // Compensate for imprecise size
    for (ui64 len = GetLength(); pos < len; ++pos) {
        output << (char)0;
    }
}

size_t TMultiBlobBuilder::GetLength() {
    // Sizes may be diferent with and without EMF_INTERLAY, so choose greater of 2
    size_t resNonInter = TMultiBlobHeader::HeaderSize() + Blobs.size() * sizeof(ui64);
    size_t resInterlay = TMultiBlobHeader::HeaderSize();
    for (size_t i = 0; i < Blobs.size(); ++i) {
        resInterlay = AlignUp<ui64>(resInterlay, sizeof(ui64)) + sizeof(ui64);
        resInterlay = AlignUp<ui64>(resInterlay, ALIGN) + Blobs[i]->GetLength();
        resNonInter = AlignUp<ui64>(resNonInter, ALIGN) + Blobs[i]->GetLength();
    }
    resInterlay = AlignUp<ui64>(resInterlay, ALIGN);
    resNonInter = AlignUp<ui64>(resNonInter, ALIGN);
    return Max(resNonInter, resInterlay);
}

TMultiBlobBuilder::TSavers& TMultiBlobBuilder::GetBlobs() {
    return Blobs;
}

const TMultiBlobBuilder::TSavers& TMultiBlobBuilder::GetBlobs() const {
    return Blobs;
}

void TMultiBlobBuilder::AddBlob(IBlobSaverBase* blob) {
    Blobs.push_back(blob);
}

void TMultiBlobBuilder::DeleteSubBlobs() {
    for (size_t i = 0; i < Blobs.size(); ++i)
        delete Blobs[i];
    Blobs.clear();
}
