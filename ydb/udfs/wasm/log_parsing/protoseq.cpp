#include "protoseq.h"

#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NLogParsing {
namespace {

constexpr size_t PROTOSEQ_FRAME_LEN_BYTES = sizeof(ui32);

} // namespace

TProtoseqSplitter::TProtoseqSplitter(TStringBuf syncWord)
    : SyncWord_(syncWord)
{}

bool TProtoseqSplitter::TryUnpackFrame(TStringBuf& buf, TStringBuf& data) const noexcept {
    if (buf.size() < PROTOSEQ_FRAME_LEN_BYTES) {
        return false;
    }
    const ui32 dataLen = LittleToHost(ReadUnaligned<ui32>(buf.data()));
    const size_t remaining = buf.size() - PROTOSEQ_FRAME_LEN_BYTES;
    if (SyncWord_.size() > remaining || dataLen > remaining - SyncWord_.size()) {
        return false;
    }
    const size_t frameLen = PROTOSEQ_FRAME_LEN_BYTES + dataLen + SyncWord_.size();
    if (buf.SubStr(PROTOSEQ_FRAME_LEN_BYTES + dataLen, SyncWord_.size()) != SyncWord_) {
        return false;
    }
    data = buf.SubStr(PROTOSEQ_FRAME_LEN_BYTES, dataLen);
    buf.Skip(frameLen);
    return true;
}

bool TProtoseqSplitter::Split(TStringBuf chunk, TVector<TStringBuf>* frames) const {
    frames->clear();
    if (SyncWord_.empty()) {
        return false;
    }
    TStringBuf buf = chunk;
    if (buf.empty()) {
        return true;
    }

    while (!buf.empty()) {
        TStringBuf frame;
        if (!TryUnpackFrame(buf, frame)) {
            return false;
        }
        frames->push_back(frame);
    }
    return true;
}

} // namespace NLogParsing
