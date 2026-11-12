#include "block_checksums.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Walks an sglist as a flat byte stream. Empty buffers are skipped. A null
// data pointer with a non-zero size aborts: that is a zero range, not a write
// payload.
class TSgListCursor final
{
public:
    explicit TSgListCursor(const TSgList& sglist)
        : SgList(sglist)
    {
        SkipFinished();
    }

    // Unread bytes of the current buffer. Zero when the sglist is exhausted.
    // Data() is valid only when this returns non-zero.
    [[nodiscard]] size_t Available() const
    {
        if (Index == SgList.size()) {
            return 0;
        }

        const TBlockDataRef& block = SgList[Index];
        Y_ABORT_UNLESS(
            block.Data() != nullptr,
            "write checksum buffers must hold real bytes; a null buffer is a "
            "zero range, not a write");
        return block.Size() - Offset;
    }

    // Unread bytes of the current buffer. Valid only when Available()
    // returned non-zero.
    [[nodiscard]] const char* Data() const
    {
        return SgList[Index].Data() + Offset;
    }

    // Drops byteCount unread bytes of the current buffer, then skips buffers
    // that are finished. byteCount must not run past the end of that buffer.
    void Advance(size_t byteCount)
    {
        Y_ABORT_UNLESS(Index < SgList.size());
        Y_ABORT_UNLESS(Offset + byteCount <= SgList[Index].Size());
        Offset += byteCount;
        SkipFinished();
    }

private:
    // Moves to the next buffer that still has unread bytes.
    void SkipFinished()
    {
        while (Index < SgList.size() && Offset == SgList[Index].Size()) {
            ++Index;
            Offset = 0;
        }
    }

    const TSgList& SgList;
    size_t Index = 0;
    size_t Offset = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlockChecksums CalculateBlockChecksums(const TSgList& sglist)
{
    const size_t totalSize = SgListGetSize(sglist);
    Y_ABORT_UNLESS(totalSize > 0);
    Y_ABORT_UNLESS(totalSize % ChecksumUnitSize == 0);

    TBlockChecksums checksums;
    checksums.reserve(totalSize / ChecksumUnitSize);

    TSgListCursor cursor(sglist);
    for (size_t done = 0; done < totalSize; done += ChecksumUnitSize) {
        const size_t available = cursor.Available();
        Y_ABORT_UNLESS(available > 0);
        // The whole unit lies in this buffer: one hash call, no stream state.
        if (available >= ChecksumUnitSize) {
            checksums.push_back(XXH3_64bits(cursor.Data(), ChecksumUnitSize));
            cursor.Advance(ChecksumUnitSize);
            continue;
        }

        XXH3_state_t state{};
        XXH3_64bits_reset(&state);
        size_t left = ChecksumUnitSize;
        while (left > 0) {
            const size_t chunk = cursor.Available();
            Y_ABORT_UNLESS(chunk > 0);
            const size_t n = Min(left, chunk);
            XXH3_64bits_update(&state, cursor.Data(), n);
            cursor.Advance(n);
            left -= n;
        }
        checksums.push_back(XXH3_64bits_digest(&state));
    }

    return checksums;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
