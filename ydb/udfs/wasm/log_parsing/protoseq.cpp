#include "protoseq.h"

#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NLogParsing {
namespace {

// Same syncword as library/cpp/framing/syncword.cpp (must not change).
const TStringBuf SyncWord(
    "\x1F\xF7\xF7~\xBE\xA6^\2367\xA6\xF6.\xFE\xAEG\xA7\xB7n\xBF\xAF\x16\x9E\2377"
    "\xF6W\367f\xA7\6\xAF\xF7",
    32);

constexpr size_t PROTOSEQ_FRAME_LEN_BYTES = sizeof(ui32);

bool TryUnpackProtoseqFrame(TStringBuf& buf, TStringBuf& data) noexcept {
    if (buf.size() < PROTOSEQ_FRAME_LEN_BYTES) {
        return false;
    }
    const ui32 dataLen = LittleToHost(ReadUnaligned<ui32>(buf.data()));
    const size_t frameLen =
        PROTOSEQ_FRAME_LEN_BYTES + static_cast<size_t>(dataLen) + SyncWord.size();
    if (buf.size() < frameLen) {
        return false;
    }
    if (buf.SubStr(PROTOSEQ_FRAME_LEN_BYTES + dataLen, SyncWord.size()) != SyncWord) {
        return false;
    }
    data = buf.SubStr(PROTOSEQ_FRAME_LEN_BYTES, dataLen);
    buf.Skip(frameLen);
    return true;
}

} // namespace

bool SplitProtoseq(TStringBuf chunk, TVector<TStringBuf>* frames) {
    frames->clear();
    TStringBuf buf = chunk;
    if (buf.empty()) {
        return true;
    }

    while (!buf.empty()) {
        TStringBuf frame;
        if (TryUnpackProtoseqFrame(buf, frame)) {
            frames->push_back(frame);
            continue;
        }

        // Recovery like NFraming::TUnpacker: jump past a syncword and retry.
        const size_t signStart = buf.find(SyncWord);
        if (signStart == TStringBuf::npos) {
            return false;
        }
        buf.Skip(signStart + SyncWord.size());
        if (!TryUnpackProtoseqFrame(buf, frame)) {
            return false;
        }
        frames->push_back(frame);
    }
    return true;
}

} // namespace NLogParsing
